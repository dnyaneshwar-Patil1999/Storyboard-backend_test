import os
import base64
import json
import io
import re
import sys
import unicodedata
import shutil
import pymupdf as fitz # Correct alias for PyMuPDF
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

# --- New Imports for Video Processing ---
import ffmpeg
# Temporarily comment out pydub until pyaudioop issue is resolved
# from pydub import AudioSegment
try:
    from pydub import AudioSegment
    AUDIO_PROCESSING_AVAILABLE = True
except ImportError:
    print("⚠️  Warning: pydub not available. Audio processing will be disabled.")
    AudioSegment = None
    AUDIO_PROCESSING_AVAILABLE = False

# --- Azure SDK Imports ---
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import HttpResponseError
from openai import AzureOpenAI
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential # Added for Text Analytics client initialization

print("✅ Libraries imported successfully.")

# ==============================================================================
# --- CONFIGURATION (Load from Environment Variables for Production) ---
# ==============================================================================
class PreprocessorConfig:
    """Configuration settings for the document preprocessor."""
    def __init__(self):
        # Azure Storage
        self.STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.SOURCE_CONTAINER_NAME = os.getenv("SOURCE_CONTAINER_NAME", "aptara")
        self.DESTINATION_CONTAINER_NAME = os.getenv("DESTINATION_CONTAINER_NAME", "aptara-processed-chunks")
        self.IMAGE_CONTAINER_NAME = os.getenv("IMAGE_CONTAINER_NAME", "aptara-images") # Not used for storage in this script

        # Azure OpenAI
        self.AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
        # For chat/vision models used in PDF structuring and video semantic chunking
        self.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_NAME", "apt-story-gpt-4o")
        # For Whisper ASR
        self.AZURE_WHISPER_DEPLOYMENT_NAME = os.getenv("AZURE_WHISPER_DEPLOYMENT_NAME", "apt-story-whisper")
        # Azure OpenAI API versions
        self.OPENAI_CHAT_API_VERSION = "2024-02-15-preview"
        self.OPENAI_WHISPER_API_VERSION = "2024-02-15-preview" # Often same as chat for newer models

        # Azure Text Analytics for Key Phrase Extraction
        self.AZURE_TEXT_ANALYTICS_ENDPOINT = os.getenv("AZURE_TEXT_ANALYTICS_ENDPOINT")
        self.AZURE_TEXT_ANALYTICS_KEY = os.getenv("AZURE_TEXT_ANALYTICS_KEY")

        # Document Processing Parameters
        self.SUPPORTED_EXTENSIONS = ('.pdf', '.txt', '.doc', '.docx', '.ppt', '.pptx', '.mp4') # <--- MODIFIED: Added .mp4
        self.TOC_DETECTION_PAGES = int(os.getenv("TOC_DETECTION_PAGES", "10"))
        self.GRAPH_DETECTION_PAGES = int(os.getenv("GRAPH_DETECTION_PAGES", "50"))
        self.DOC_TYPE_DETECTION_PAGES = int(os.getenv("DOC_TYPE_DETECTION_PAGES", "5"))
        self.MAX_WORD_LENGTH = int(os.getenv("MAX_WORD_LENGTH", "16000"))

        # Video Processing Specific Parameters
        self.LOCAL_TEMP_DIR = "temp_processing_data" # Local directory for temp files
        self.VIDEO_AUDIO_CHUNK_SIZE_MB = int(os.getenv("VIDEO_AUDIO_CHUNK_SIZE_MB", "24")) # Max size for Whisper audio chunks
        self.PROCESSING_WINDOW_WORD_COUNT = int(os.getenv("PROCESSING_WINDOW_WORD_COUNT", "2000")) # For semantic chunking LLM
        self.MIN_CHUNK_WORD_COUNT = int(os.getenv("MIN_CHUNK_WORD_COUNT", "30")) # Min words for a valid semantic chunk

# ==============================================================================
# --- INITIALIZE AZURE CLIENTS ---
# ==============================================================================
def initialize_clients(config: PreprocessorConfig) -> tuple[BlobServiceClient, AzureOpenAI, AzureOpenAI, TextAnalyticsClient]:
    """
    Initializes the Azure Blob Storage, Azure OpenAI (Chat/Vision),
    Azure OpenAI (Whisper), and Azure Text Analytics clients.
    """
    print("--- Connecting to Azure Services ---")
    blob_service_client = None
    openai_chat_client = None
    openai_whisper_client = None
    text_analytics_client = None

    try:
        blob_service_client = BlobServiceClient.from_connection_string(config.STORAGE_CONNECTION_STRING)
        print("✅ Connected to Azure Blob Storage.")
    except Exception as e:
        print(f"❌ Failed to connect to Azure Blob Storage. Error: {e}")
        raise

    try:
        openai_chat_client = AzureOpenAI(
            api_key=config.AZURE_OPENAI_API_KEY,
            api_version=config.OPENAI_CHAT_API_VERSION,
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT
        )
        print("✅ Connected to Azure OpenAI (Chat/Vision).")
    except Exception as e:
        print(f"❌ Failed to connect to Azure OpenAI (Chat/Vision). Error: {e}")
        # Not raising, as some features might degrade gracefully

    try:
        # Whisper might use the same endpoint/key but a different API version
        openai_whisper_client = AzureOpenAI(
            api_key=config.AZURE_OPENAI_API_KEY,
            api_version=config.OPENAI_WHISPER_API_VERSION,
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT
        )
        print("✅ Connected to Azure OpenAI (Whisper).")
    except Exception as e:
        print(f"❌ Failed to connect to Azure OpenAI (Whisper). Error: {e}")
        # Not raising, as ASR would just fail

    try:
        text_analytics_client = TextAnalyticsClient(
            endpoint=config.AZURE_TEXT_ANALYTICS_ENDPOINT,
            credential=AzureKeyCredential(config.AZURE_TEXT_ANALYTICS_KEY)
        )
        print("✅ Connected to Azure Text Analytics.")
    except Exception as e:
        print(f"❌ Failed to connect to Azure Text Analytics. Error: {e}")
        # Not raising, as key phrase extraction would just fail

    if not blob_service_client: # Only fail fatally if Blob Storage isn't connected
        raise RuntimeError("Critical client (BlobServiceClient) not initialized.")

    return blob_service_client, openai_chat_client, openai_whisper_client, text_analytics_client


# ==============================================================================
# --- HELPER FUNCTIONS ---
# ==============================================================================
def find_images_near_caption(page: fitz.Page, caption_rect: fitz.Rect, processed_image_xrefs: set, all_images_on_page: list, vertical_threshold: int = 100, horizontal_threshold: int = 50) -> list:
    """Find images near a caption, considering both above and below positions."""
    candidate_images = []
    search_area = fitz.Rect(
        caption_rect.x0 - horizontal_threshold,
        caption_rect.y0 - vertical_threshold,
        caption_rect.x1 + horizontal_threshold,
        caption_rect.y1 + vertical_threshold
    )
    
    for img in all_images_on_page:
        if img[0] in processed_image_xrefs:
            continue
        img_bbox = page.get_image_bbox(img)
        if img_bbox.intersects(search_area):
            candidate_images.append(img)
    
    return candidate_images

def is_graph_caption(text: str) -> bool:
    """
    Check if text is a figure caption that might contain a graph.
    """
    graph_keywords = [
        'graph', 'chart', 'plot', 'diagram', 'curve', 'axis', 'vs.', 'versus',
        'pie chart', 'bar chart', 'line chart', 'scatterplot', 'flowchart', 'infographic'
    ]
    return any(keyword in text.lower() for keyword in graph_keywords)

def break_long_words(text: str, max_len: int) -> str:
    if not text: return ""
    words = text.split()
    processed = []
    for word in words:
        if len(word) > max_len:
            for i in range(0, len(word), max_len):
                processed.append(word[i:i+max_len])
        else:
            processed.append(word)
    return " ".join(processed)

def sanitize_text(text: Any) -> str:
    if not isinstance(text, str): return ""
    cleaned = "".join(ch for ch in text if unicodedata.category(ch)[0] != "C" or ch in ('\n', '\t'))
    cleaned = cleaned.replace('\n', ' ').replace('\t', ' ')
    return cleaned.encode('utf-8', 'ignore').decode('utf-8').strip()

def extract_links_from_text(text: str) -> List[str]:
    url_pattern = re.compile(r'https?://[^\s/$.?#].[^\s]*|www\.[^\s/$.?#].[^\s]*')
    email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    return url_pattern.findall(text) + email_pattern.findall(text)

def get_page_image_for_llm(page: fitz.Page, dpi: int = 150) -> str:
    """Generates a base64 image of a page for LLM visual input."""
    pix = page.get_pixmap(dpi=dpi)
    img_bytes = pix.tobytes("png")
    return base64.b64encode(img_bytes).decode('utf-8')

# ==============================================================================
# --- VIDEO PROCESSING HELPER FUNCTIONS (FROM ORIGINAL VIDEO SCRIPT) ---
# ==============================================================================
def enrich_text_with_key_phrases(text_content: str, text_analytics_client_instance: TextAnalyticsClient) -> List[str]:
    """Extracts key phrases from text using the Text Analytics client."""
    if not text_content.strip() or not text_analytics_client_instance:
        print("    - Skipping key phrase extraction: Empty content or client not available."); return []
    try:
        response = text_analytics_client_instance.extract_key_phrases(documents=[text_content])
        return [] if response[0].is_error else response[0].key_phrases or []
    except Exception as e:
        print(f"    - Key phrase extraction failed: {e}"); return []

def find_next_semantic_chunk(transcript_window: str, last_chapter: str, last_topic: str, oai_chat_client: AzureOpenAI, config: PreprocessorConfig) -> Optional[Dict[str, Any]]:
    """
    Analyzes a window of transcript to find the first complete semantic chunk and its word count.
    """
    if not transcript_window.strip() or not oai_chat_client:
        print("  - Skipping semantic chunk identification: Empty window or OpenAI chat client not available."); return None

    system_prompt = f"""
You are an expert content analyst. Your task is to analyze the beginning of the provided 'Transcript Window' and identify the **very first complete semantic chunk**. A complete chunk is a section that fully discusses a single idea or subtopic before moving on.

**Context from Previous Chunk:**
- **Last Chapter:** "{last_chapter}"
- **Last Topic:** "{last_topic}"

**Instructions:**
1.  Read the 'Transcript Window' from the beginning. Determine where the first logical section (subtopic, topic, etc.) **ends**.
2.  Generate new, descriptive titles for the "chapter", "topic", and "subtopic" for this chunk, using the provided context.
    - If the chapter or topic hasn't changed, reuse the title from the context.
    - If a new chapter or topic begins, generate a new title.
3.  **CRITICAL:** Your response must be a single JSON object with the following keys:
    - `"chapter"`: The chapter title for this chunk.
    - `"topic"`: The topic title for this chunk.
    - `"subtopic"`: The subtopic title for this chunk.
    - `"chunk_text"`: The full, verbatim text of this first complete chunk.
    - `"word_count"`: The integer number of words in your generated `"chunk_text"`. This is essential for the next step.

**Rules:**
- The `"chunk_text"` must be an exact substring from the beginning of the 'Transcript Window'.
- If the 'Transcript Window' seems to end mid-thought, only create a chunk for the part that is complete.
"""
    try:
        response = oai_chat_client.chat.completions.create(
            model=config.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"**Transcript Window:**\n{transcript_window}"}
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
        )
        
        raw_response_text = response.choices[0].message.content
        chunk_data = json.loads(raw_response_text)
        
        if all(k in chunk_data for k in ["chunk_text", "word_count", "chapter", "topic", "subtopic"]):
            return chunk_data
        else:
            print(f"  - ⚠️ LLM response for semantic chunking was missing required keys: {raw_response_text[:200]}..."); return None
    except Exception as e:
        print(f"    - OpenAI semantic chunk identification failed for this window: {e}"); return None

def generate_summary(full_content: str, oai_chat_client: AzureOpenAI, config: PreprocessorConfig) -> List[str]:
    """Generates a bulleted list summary for the given text content."""
    if not full_content or len(full_content.split()) < 20 or not oai_chat_client:
        print("  - Skipping summary generation: content too short or OpenAI chat client not available."); return []
        
    system_prompt = """You are an expert summarization AI. Your task is to provide a concise, bullet-point summary of the given content. Focus on the main ideas and key takeaways. Respond with ONLY a single JSON object with the key "bullet_point_summary" and a list of strings as its value."""
    try:
        response = oai_chat_client.chat.completions.create(
            model=config.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME,
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": f"**Content:**\n{full_content}"}],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        return json.loads(response.choices[0].message.content).get('bullet_point_summary', [])
    except Exception as e:
        print(f"  - LLM Error summarizing content: {e}"); return []

def extract_audio_to_wav(mp4_path: str, wav_path: str) -> bool:
    try:
        print(f"  - Extracting audio from {os.path.basename(mp4_path)}...")
        # Use subprocess directly for better control over stdout/stderr
        subprocess.run([
            'ffmpeg',
            '-i', mp4_path,
            '-f', 'wav',
            '-acodec', 'pcm_s16le',
            '-ac', '1',
            '-ar', '16000',
            wav_path
        ], check=True, capture_output=True) # check=True raises CalledProcessError on non-zero exit code
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ❌ FFmpeg Error: {e.stderr.decode()}"); return False
    except FileNotFoundError:
        print("  ❌ FFmpeg not found. Please ensure FFmpeg is installed and in your system's PATH."); return False
    except Exception as e:
        print(f"  ❌ Unexpected error during audio extraction: {e}"); return False

def split_wav_for_transcription(wav_path: str, chunk_size_mb: int) -> List[str]:
    print(f"  - Audio file is large, splitting into chunks <= {chunk_size_mb}MB...")
    try:
        if not AUDIO_PROCESSING_AVAILABLE:
            print("  - Warning: Audio processing not available, returning original file")
            return [wav_path]
            
        audio = AudioSegment.from_wav(wav_path)
        total_mb = os.path.getsize(wav_path) / (1024 * 1024)
        num_chunks = int(total_mb // chunk_size_mb) + 1
        if num_chunks <= 1: return [wav_path]
        chunk_duration_ms = len(audio) // num_chunks
        chunk_paths = []
        for i in range(num_chunks):
            start_ms, end_ms = i * chunk_duration_ms, (i + 1) * chunk_duration_ms if i < num_chunks - 1 else len(audio)
            chunk = audio[start_ms:end_ms]
            chunk_path = f"{wav_path[:-4]}_chunk{i}.wav"
            chunk.export(chunk_path, format="wav"); chunk_paths.append(chunk_path)
        print(f"  - Split into {len(chunk_paths)} audio chunks.")
        return chunk_paths
    except Exception as e:
        print(f"  ❌ Error splitting WAV file: {e}"); return []

# ==============================================================================
# --- GRAPH DETECTION AND EXTRACTION ---
# ==============================================================================
def detect_graph_pages(doc: fitz.Document, oai_client: AzureOpenAI, config: PreprocessorConfig) -> set:
    """Use AI to identify pages containing graphs"""
    print("  - Scanning document for graph-containing pages (first few pages)...")
    graph_pages = set()
    pages_to_scan = min(config.GRAPH_DETECTION_PAGES, len(doc))
    
    system_prompt_content = """You are an expert document analyst. Determine if the provided page content contains any:
            - Graphs
            - Charts (bar chart, pie chart, line chart)
            - Plots
            - Data visualizations
            - Scientific diagrams
            - Visual representations of data
            - Flowcharts, Infographics

            Return ONLY a JSON object with a single key 'has_graph' with a boolean value."""

    for page_num in range(pages_to_scan):
        page = doc.load_page(page_num)
        page_text = page.get_text("text")
        
        page_image_base64 = None
        try:
            page_image_base64 = get_page_image_for_llm(page, dpi=100)
        except Exception as e:
            print(f"      - Warning: Could not get image for graph detection on page {page_num+1}: {e}")

        user_content_list = []
        if page_image_base64:
            user_content_list.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{page_image_base64}"}
            })
        user_content_list.append({
            "type": "text",
            "text": f"Page {page_num+1} content:\n{page_text}"
        })

        if not user_content_list: continue
        
        conversation_messages = [
            {"role": "system", "content": system_prompt_content},
            {"role": "user", "content": user_content_list}
        ]

        try:
            response = oai_client.chat.completions.create(
                model=config.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME, # <--- Modified to use AZURE_OPENAI_CHAT_DEPLOYMENT_NAME
                messages=conversation_messages,
                max_tokens=100,
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            if result.get('has_graph', False):
                graph_pages.add(page_num)
        except Exception as e:
            print(f"    - ❌ Graph detection failed for page {page_num+1}: {e}")
    
    return graph_pages

def extract_graph_area(page: fitz.Page, caption_rect: fitz.Rect) -> fitz.Pixmap:
    """Extract the entire content area around a graph caption."""
    page_rect = page.rect
    y_start = max(0, caption_rect.y0 - page_rect.height * 0.1)
    graph_area = fitz.Rect(0, y_start, page_rect.width, page_rect.height)
    pix = page.get_pixmap(clip=graph_area, dpi=300, alpha=True)
    return pix

def extract_graphs_from_page(page: fitz.Page, page_num: int, element_count: int) -> tuple[List[Dict[str, Any]], int]:
    """Enhanced graph extraction for identified graph pages."""
    elements = []
    p_fig = re.compile(
        r"^(figure|fig\.?|graph|chart|plot|diagram)\s*"
        r"([IVXLCDM]+[-\.]?\d+|"
        r"[A-Z]+[-\.]?\d+|"
        r"\d+[A-Z]?|"
        r"\d+\.\d+|"
        r"\d+-\d+|"
        r"\d+[a-z]?)\)?",
        re.IGNORECASE
    )
    
    text_blocks = page.get_text("blocks", sort=True)
    caption_blocks = []
    
    for b in text_blocks:
        block_text = b[4].strip()
        if p_fig.search(block_text) and is_graph_caption(block_text):
            caption_blocks.append(b)
    
    for caption_block in caption_blocks:
        try:
            caption_rect = fitz.Rect(caption_block[:4])
            caption_text = caption_block[4].strip().replace('\n', ' ')
            
            pix = extract_graph_area(page, caption_rect)
            
            element_id = f"p{page_num+1}_fig_{element_count}"
            elements.append({
                "type": "image",
                "id": element_id,
                "base64": base64.b64encode(pix.tobytes("png")).decode('utf-8'), 
                "page_num": page_num + 1,
                "pos": caption_rect.y0,
                "caption": caption_text,
                "is_graph": True
            })
            print(f"    ✅ Extracted graph: {caption_text} as {element_id}")
            element_count += 1
            
        except Exception as e:
            print(f"      - Warning: Could not process graph (caption-based) on page {page_num+1}. Error: {e}")
    
    return elements, element_count

def extract_tables_as_images(page: fitz.Page) -> List[Dict[str, Any]]:
    """Extracts tables as images."""
    table_images = []
    try:
        table_finder = page.find_tables()
        for i, table in enumerate(table_finder):
            bbox = fitz.Rect(table.bbox)
            pix = page.get_pixmap(clip=bbox, dpi=200)
            img_bytes = pix.tobytes("png")
            base64_image = base64.b64encode(img_bytes).decode('utf-8')
            table_image_id = f"p{page.number + 1}_tbl_img_{i}"
            table_images.append({
                "type": "table_image", "id": table_image_id, "data": base64_image,
                "page_num": page.number + 1, "pos": bbox.y1
            })
    except Exception as e:
        print(f"  - Warning: Could not process tables on page {page.number + 1}. Error: {e}")
    return table_images

def generate_graph_description(image_base64: str, caption_text: str, oai_client: AzureOpenAI, config: PreprocessorConfig) -> str:
    """
    Generates a concise textual description for a graph image using LLM.
    """
    print("      - Generating AI description for graph...")
    messages_content = [
        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_base64}"}},
        {"type": "text", "text": f"""
        Analyze the provided image, which is a graph or chart.
        Task: Provide a concise, factual description of the graph's key information, main trends, and components.
        If a caption is provided, use it for context. Focus on what is visually represented.
        Keep the description brief, ideally within 50-100 words (approx. 2-4 sentences).

        Graph Caption (if available): "{caption_text}"
        """}
    ]
    
    try:
        response = oai_client.chat.completions.create(
            model=config.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME, # <--- Modified to use AZURE_OPENAI_CHAT_DEPLOYMENT_NAME
            messages=[{"role": "user", "content": messages_content}],
            max_tokens=200,
            temperature=0.0
        )
        description = response.choices[0].message.content.strip()
        print(f"      ✅ Graph description generated.")
        return description
    except Exception as e:
        print(f"      ❌ Failed to generate graph description: {e}")
        return f"[GRAPH DESCRIPTION UNAVAILABLE FOR: {caption_text}]"

# ==============================================================================
# --- AI-DRIVEN DOCUMENT TYPE AND STRUCTURE DETERMINATION ---
# ==============================================================================

def detect_document_type(doc: fitz.Document, oai_client: AzureOpenAI, config: PreprocessorConfig) -> str:
    """
    Determines if the PDF is a slide deck or a traditional document using LLM.
    Analyzes initial pages with both text and images.
    """
    print("  - Detecting document type (slide deck vs. traditional document)...")
    content_for_llm = []
    num_pages_to_sample = min(config.DOC_TYPE_DETECTION_PAGES, len(doc))

    for page_num in range(num_pages_to_sample):
        page = doc.load_page(page_num)
        page_text = page.get_text("text")
        try:
            page_image_base64 = get_page_image_for_llm(page, dpi=100)
            content_for_llm.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{page_image_base64}"}
            })
            content_for_llm.append({
                "type": "text",
                "text": f"--- PAGE {page_num+1} TEXT ---\n{page_text}"
            })
        except Exception as e:
            print(f"      - Warning: Could not get image for page {page_num+1} for type detection. Error: {e}")
            content_for_llm.append({
                "type": "text",
                "text": f"--- PAGE {page_num+1} TEXT (Image unavailable) ---\n{page_text}"
            })

    if not content_for_llm:
        print("    - No content for document type detection. Defaulting to traditional_document.")
        return "traditional_document"

    system_prompt = """You are an expert document analyst. Determine if the provided document content (text and images) is primarily a slide presentation (like a PowerPoint converted to PDF, with distinct slides, titles, and bullet points) or a traditional document (like a report, manual, or book, with continuous text flow, headings, and paragraphs).

    Return ONLY a JSON object with two keys:
    - 'document_type': (string, "slide_deck" or "traditional_document")
    - 'reasoning': (string, a brief explanation for your classification)
    """
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": content_for_llm}
    ]
    
    try:
        response = oai_client.chat.completions.create(
            model=config.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME, # <--- Modified to use AZURE_OPENAI_CHAT_DEPLOYMENT_NAME
            messages=messages,
            max_tokens=200,
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        result = json.loads(response.choices[0].message.content)
        doc_type = result.get('document_type', 'traditional_document')
        reasoning = result.get('reasoning', 'No specific reason provided.')
        print(f"    ✅ Document classified as: '{doc_type}' - {reasoning}")
        return doc_type
    except Exception as e:
        print(f"    ❌ Document type detection failed: {e}. Defaulting to traditional_document.")
        return "traditional_document"

def detect_toc_presence(doc: fitz.Document, oai_client: AzureOpenAI, config: PreprocessorConfig) -> tuple[bool, List[Dict[str, Any]]]:
    """Use LLM to detect if document has a Table of Contents/Index/Agenda and extract it."""
    print("  - Scanning for Table of Contents/Index/Agenda...")
    toc_text = ""
    num_pages_to_scan = min(config.TOC_DETECTION_PAGES, len(doc))
    
    for i in range(num_pages_to_scan):
        page = doc.load_page(i)
        toc_text += f"\n--- PAGE {i+1} ---\n" + page.get_text("text")
    
    if not toc_text.strip():
        print("    - No text found in first pages for TOC detection.")
        return False, []
    
    system_prompt = """You are an expert document analyst. Determine if the provided document excerpt contains a Table of Contents (TOC), Index, Agenda, or similar content listing. If present, extract the chapter/section titles and their corresponding page numbers in a structured JSON format. Focus on 'Chapter' level entries.

    Return ONLY a JSON object with two keys:
    - 'has_toc': boolean (true if TOC/index/agenda is found, false otherwise)
    - 'toc_entries': array of objects, each with 'title' (string), 'page' (integer), and 'level' (string, e.g., 'Chapter', 'Section', 'Entry'). Prioritize 'Chapter' or major section levels. This array should be empty if 'has_toc' is false.

    Example Output for has_toc: true:
    {
      "has_toc": true,
      "toc_entries": [
        {"title": "1. Introduction", "page": 1, "level": "Chapter"},
        {"title": "2. Main Concepts", "page": 10, "level": "Chapter"}
      ]
    }
    """
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": toc_text}
    ]
    
    try:
        response = oai_client.chat.completions.create(
            model=config.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME, # <--- Modified to use AZURE_OPENAI_CHAT_DEPLOYMENT_NAME
            messages=messages,
            max_tokens=1000,
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        result = json.loads(response.choices[0].message.content)
        has_toc = result.get('has_toc', False)
        toc_entries = result.get('toc_entries', [])
        print(f"    - {'✅ TOC/Index detected' if has_toc else '❌ No TOC/Index found'}")
        return has_toc, toc_entries
    except Exception as e:
        print(f"    - ❌ TOC detection failed: {e}")
        return False, []

def _analyze_slide_page_with_llm(page: fitz.Page, page_num: int, oai_client: AzureOpenAI, config: PreprocessorConfig, current_context: Dict[str, str]) -> Dict[str, Any]:
    """
    Analyzes a single page of a slide deck to extract slide-specific structure and content blocks.
    """
    page_text = page.get_text("text")
    page_image_base64 = None
    try:
        page_image_base64 = get_page_image_for_llm(page, dpi=200)
    except Exception as e:
        print(f"      - Warning: Could not get image for LLM slide analysis on page {page_num+1}: {e}")

    messages_content = []
    if page_image_base64:
        messages_content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{page_image_base64}"}
        })
    messages_content.append({
        "type": "text",
        "text": f"""
        --- Current Page: {page_num+1} (Slide) ---
        --- Text Content of Page {page_num+1} ---
        {page_text}

        --- Current Logical Context Before This Page ---
        Chapter: '{current_context.get('Chapter', 'N/A')}'
        Topic: '{current_context.get('Topic', 'N/A')}'
        Subtopic: '{current_context.get('Subtopic', 'N/A')}'

        --- Task ---
        You are analyzing a single slide from a presentation.
        1. Identify the bounding box of the main slide content area (excluding static headers, footers, page numbers, sidebars not integral to the slide content). Provide as [x0, y0, x1, y1] coordinates relative to the page. If no clear slide content area, use [0.0, 0.0, {page.rect.width}.0, {page.rect.height}.0].
        2. Extract the **semantic main title** of this slide (NOT "Slide#X" labels). If none, return "N/A".
        3. Provide a **single, very concise descriptive phrase (MAX 5 words)** for the slide's primary topic, derived from its title or content. Return "N/A" if none.
        4. Identify **all distinct logical content blocks, bullet points, or key text sections** within the main slide content area. For each identified block:
           - Extract its exact text.
           - Extract the **most concise subtopic or key phrase (MAX 5 words)** for that block. If the block's text is already 5 words or less, use its exact text as the subtopic. If the block's text is *longer than 5 words* and no natural, concise subtopic (max 5 words) is evident *within* it, return null for 'subtopic'.
           - Provide its approximate bounding box [x0, y0, x1, y1] for later reference.
           - If no distinct content blocks are found, return an empty array for this list.
        5. Based on the content and flow, determine if this slide introduces a new major section or 'Chapter' for the overall document. If yes, what is its title? If no, return null.
        6. Provide the updated logical context (Chapter, Topic, Subtopic) that applies *after* processing this slide. This should propagate previous context if no new headings were found on this slide, or update them based on your findings for this slide. Ensure all context values are strings (e.g., "N/A" if empty).

        Return ONLY a JSON object with the following keys:
        - 'slide_bbox': [x0, y0, x1, y1] (float array)
        - 'slide_title': (string, e.g., "Equipment Familiarization", or "N/A")
        - 'concise_slide_topic': (string, a very concise phrase (MAX 5 words) for the slide's main topic, derived from its title or content. "N/A" if none.)
        - 'slide_content_blocks': (array of objects, e.g., [{{'text': '...', 'subtopic': '...', 'bbox': [x0,y0,x1,y1]}}], or empty array [])
        - 'is_new_chapter': (boolean, true if this slide marks a new overall chapter/section)
        - 'chapter_text': (string, the title of the new chapter if 'is_new_chapter' is true, else null)
        - 'updated_context': {{"Chapter": "...", "Topic": "...", "Subtopic": "..."}} (reflects context AFTER this page)
        """
    })
    
    llm_raw_output = "{}"
    try:
        response = oai_client.chat.completions.create(
            model=config.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME, # <--- Modified to use AZURE_OPENAI_CHAT_DEPLOYMENT_NAME
            messages=[{"role": "user", "content": messages_content}],
            max_tokens=2500,
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        llm_output = json.loads(response.choices[0].message.content)
        return llm_output
    except Exception as e:
        print(f"    ❌ LLM slide analysis failed for page {page_num+1}: {e}")
        print(f"    LLM raw output for debugging: {llm_raw_output}")
        return {
            'slide_bbox': [0.0, 0.0, page.rect.width, page.rect.height],
            'slide_title': "N/A",
            'concise_slide_topic': "N/A",
            'slide_content_blocks': [{'text': page_text, 'subtopic': f"Page {page_num+1} Content", 'bbox': [0,0,page.rect.width, page.rect.height]}],
            'is_new_chapter': False,
            'chapter_text': None,
            'updated_context': current_context
        }

def get_llm_page_by_page_structure_traditional(doc: fitz.Document, oai_client: AzureOpenAI, config: PreprocessorConfig) -> List[Dict[str, Any]]:
    """
    Infers document structure (Chapter, Topic, Subtopic) page by page for traditional documents.
    """
    print("  - Inferring traditional document structure page by page using AI (sending EVERY page image)...")
    print("  ⚠️ WARNING: This method sends every page as an image and text to the LLM.")
    print("             It is highly token-intensive and will incur significant API costs and slower processing.")

    document_outline = []
    
    current_chapter_text = "N/A"
    current_topic_text = "N/A"
    current_subtopic_text = "N/A"

    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        page_text = page.get_text("text")
        
        page_blocks = page.get_text("blocks", sort=True)
        first_text_pos_on_page = page_blocks[0][1] if page_blocks else 0

        llm_raw_output = "{}"
        try:
            page_image_base64 = get_page_image_for_llm(page)
            
            messages_content = []
            messages_content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{page_image_base64}"}
            })
            messages_content.append({
                "type": "text",
                "text": f"""
                --- Current Page: {page_num+1} (Traditional Document) ---
                --- Text Content of Page {page_num+1} ---
                {page_text}

                --- Current Logical Context Before This Page ---
                Chapter: '{current_chapter_text}'
                Topic: '{current_topic_text}'
                Subtopic: '{current_subtopic_text}'

                --- Task ---
                Analyze PAGE {page_num+1} (text and image) from a traditional document (e.g., manual, report). Identify ALL new structural headings (Chapter, Topic, Subtopic) on THIS specific page. Look for numbered sections (e.g., I. ABC, A. DEF, 1. GHI, 1.1.1 JKL), bold text, larger fonts. If no new heading of a certain level is found, its context continues from the previous page.

                Return ONLY a JSON object with two keys:
                - 'new_headings_on_page': An array of objects. Each object represents a new heading (Chapter, Topic, or Subtopic) found on THIS PAGE.
                  For each new heading, include:
                    - 'level': (string, e.g., "Chapter", "Topic", "Subtopic")
                    - 'text': (string, the exact heading text)
                    - 'page_num': (integer, always {page_num+1} for this page)
                    - 'parent_text': (string, the exact text of its immediate parent heading based on hierarchy, or null if it's a Chapter).
                  If no new headings are found on this page, this array should be empty.

                - 'updated_context': An object reflecting the FINAL logical context (Chapter, Topic, Subtopic) after processing THIS PAGE. This should propagate contexts if no new headings were found on this page, or update them if new headings were identified. Example: {{"chapter": "...", "topic": "...", "subtopic": "..."}}
                """
            })
            
            response = oai_client.chat.completions.create(
                model=config.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME, # <--- Modified to use AZURE_OPENAI_CHAT_DEPLOYMENT_NAME
                messages=[{"role": "user", "content": messages_content}],
                max_tokens=2000,
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            
            llm_raw_output = response.choices[0].message.content
            llm_output = json.loads(llm_raw_output)
            
            for heading in llm_output.get('new_headings_on_page', []):
                heading['pos'] = first_text_pos_on_page
                document_outline.append(heading)
            
            updated_ctx = llm_output.get('updated_context', {})
            current_chapter_text = updated_ctx.get('Chapter', updated_ctx.get('chapter', current_chapter_text)) 
            current_topic_text = updated_ctx.get('Topic', updated_ctx.get('topic', current_topic_text))
            current_subtopic_text = updated_ctx.get('Subtopic', updated_ctx.get('subtopic', current_subtopic_text))
            
        except Exception as e:
            print(f"    ❌ LLM traditional structure inference failed for page {page_num+1}: {e}")
            print(f"    LLM raw output for debugging: {llm_raw_output}")
            
    print(f"    ✅ LLM inferred {len(document_outline)} new headings across the document.")
    return document_outline

def assign_context_to_elements(elements: List[Dict[str, Any]], document_outline: List[Dict[str, Any]], toc_chapter_map: Optional[Dict[int, str]] = None) -> List[Dict[str, Any]]:
    """
    Assigns chapter, topic, and subtopic context to each element based on the document outline
    and potentially a TOC chapter map.
    """
    
    outline_sorted = sorted(document_outline, key=lambda x: (x['page_num'], x.get('pos', 0)))

    current_context = {
        "Chapter": "N/A",
        "Topic": "N/A",
        "Subtopic": "N/A"
    }
    
    outline_idx = 0
    for element in elements:
        while outline_idx < len(outline_sorted) and \
              (outline_sorted[outline_idx]['page_num'] < element['page_num'] or \
               (outline_sorted[outline_idx]['page_num'] == element['page_num'] and outline_sorted[outline_idx].get('pos', 0) <= element['pos'])):
            
            heading = outline_sorted[outline_idx]
            level = heading['level']
            text = heading['text']

            if level == "Chapter":
                current_context["Chapter"] = text
                current_context["Topic"] = "N/A" 
                current_context["Subtopic"] = "N/A"
            elif level == "Topic":
                current_context["Topic"] = text
                current_context["Subtopic"] = "N/A" 
            elif level == "Subtopic":
                current_context["Subtopic"] = text
            
            outline_idx += 1
            
        if toc_chapter_map:
            active_toc_chapter = None
            for toc_page, toc_title in toc_chapter_map.items():
                if element['page_num'] >= toc_page: 
                    active_toc_chapter = toc_title 
                else: 
                    break 
            if active_toc_chapter:
                current_context["Chapter"] = active_toc_chapter

        element['chapter'] = sanitize_text(current_context["Chapter"])
        element['topic'] = sanitize_text(current_context["Topic"])
        element['subtopic'] = sanitize_text(current_context["Subtopic"])
        
    return elements

# ==============================================================================
# --- MAIN ELEMENT EXTRACTION (Generic for Traditional Documents) ---
# ==============================================================================
def extract_document_elements(doc: fitz.Document, oai_client: AzureOpenAI, config: PreprocessorConfig) -> List[Dict[str, Any]]:
    """
    Extracts all raw elements (text blocks, images, tables) from the document.
    """
    elements = []
    element_counter = 0 
    
    graph_pages_from_detector = detect_graph_pages(doc, oai_client, config) 
    
    p_fig = re.compile(r"^(figure|fig\.?)\s*([A-Z]+[-\.]?\d+|\d+[A-Z]?|\d+\.\d+|\d+-\d+|\d+[a-z]?)\)?", re.IGNORECASE)
    p_tbl = re.compile(r"^(table|tab\.?)\s*([A-Z]+[-\.]?\d+|\d+[A-Z]?|\d+\.\d+|\d+-\d+|\d+[a-z]?)\)?", re.IGNORECASE)

    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        text_blocks = page.get_text("blocks", sort=True)
        all_images_on_page = page.get_images(full=True)
        processed_image_xrefs = set()
        
        if page_num in graph_pages_from_detector:
            graph_elements_on_page, element_counter = extract_graphs_from_page(page, page_num, element_counter)
            for ge in graph_elements_on_page:
                elements.append(ge)

        caption_blocks = []
        
        for b in text_blocks:
            block_text = b[4].strip()
            if not block_text: continue
            
            is_caption = False
            if p_fig.search(block_text):
                is_caption = True
            elif p_tbl.search(block_text):
                is_caption = True

            if is_caption:
                caption_blocks.append(b)
            else:
                elements.append({
                    "type": "text", 
                    "content": block_text,
                    "page_num": page_num + 1, 
                    "pos": b[1] 
                })
                element_counter += 1

        for caption_block in caption_blocks:
            try:
                caption_rect = fitz.Rect(caption_block[:4])
                caption_text = caption_block[4].strip().replace('\n', ' ')
                
                candidate_images = find_images_near_caption(
                    page, caption_rect, processed_image_xrefs, all_images_on_page
                )
                
                if is_graph_caption(caption_text):
                    if not candidate_images: 
                        candidate_images = find_images_near_caption(
                            page, caption_rect, processed_image_xrefs, all_images_on_page,
                            vertical_threshold=150, 
                            horizontal_threshold=100
                        )
                
                if candidate_images:
                    closest_image = min(
                        candidate_images,
                        key=lambda img: abs(page.get_image_bbox(img).y1 - caption_rect.y0)
                    )
                    img_bbox = page.get_image_bbox(closest_image)
                    
                    group_bbox = fitz.Rect(img_bbox).include_rect(caption_rect)
                    
                    padding = 25 if is_graph_caption(caption_text) else 15
                    
                    padded_area = fitz.Rect(group_bbox)
                    padded_area.x0 -= padding
                    padded_area.y0 -= padding
                    padded_area.x1 += padding
                    padded_area.y1 += padding
                    padded_area.intersect(page.rect)
                    
                    if not padded_area.is_empty:
                        dpi = 300 if is_graph_caption(caption_text) else 200
                        pix = page.get_pixmap(clip=padded_area, dpi=dpi, alpha=True)
                        element_id = f"p{page_num+1}_fig_{element_counter}"
                        elements.append({
                            "type": "image",
                            "id": element_id,
                            "base64": base64.b64encode(pix.tobytes("png")).decode('utf-8'),
                            "page_num": page_num + 1,
                            "pos": min(img_bbox.y0, caption_rect.y0), 
                            "caption": caption_text,
                            "is_graph": is_graph_caption(caption_text) 
                        })
                        processed_image_xrefs.add(closest_image[0])
                        element_counter += 1
                else:
                    elements.append({
                        "type": "text", 
                        "content": caption_text,
                        "page_num": page_num + 1, 
                        "pos": caption_rect.y0 
                    })
                    element_counter += 1

            except Exception as e:
                print(f"      - Warning: Could not process figure caption on page {page_num+1}. Error: {e}")

        for img in all_images_on_page:
            try:
                if img[0] in processed_image_xrefs: continue 
                img_bbox = page.get_image_bbox(img)
                if not img_bbox.is_valid or img_bbox.is_empty or min(img_bbox.width, img_bbox.height) < 10: continue
                
                aspect_ratio = max(img_bbox.width, img_bbox.height) / min(img_bbox.width, img_bbox.height) if min(img_bbox.width, img_bbox.height) > 0 else 999
                if aspect_ratio > 15 and (img_bbox.width * img_bbox.height) < 30000: continue
                
                pix = page.get_pixmap(clip=img_bbox, dpi=150, alpha=True)
                if pix.width > 0 and pix.height > 0:
                    element_id = f"p{page_num+1}_img_{element_counter}"
                    elements.append({
                        "type": "image",
                        "id": element_id,
                        "base64": base64.b64encode(pix.tobytes("png")).decode('utf-8'),
                        "page_num": page_num + 1,
                        "pos": img_bbox.y0,
                        "caption": "Uncaptioned image",
                        "is_graph": False 
                    })
                    processed_image_xrefs.add(img[0])
                    element_counter += 1
            except Exception as e:
                print(f"      - Warning: Could not process uncaptioned image on page {page_num+1}. Error: {e}")

        page_table_images = extract_tables_as_images(page)
        for tbl_img in page_table_images:
            elements.append(tbl_img)
            element_counter += 1

    elements.sort(key=lambda x: (x['page_num'], x['pos']))
    return elements


def assemble_chunks_from_elements(elements: List[Dict[str, Any]], oai_client: AzureOpenAI, config: PreprocessorConfig) -> List[Dict[str, Any]]:
    """
    Assembles chunks from elements. A new chunk is created based on the most granular change
    in the hierarchical context (Subtopic > Topic > Chapter).
    Also generates descriptions for graph images if marked as such.
    """
    chunks = []
    current_chunk: Optional[Dict[str, Any]] = None
    
    last_known_context = {
        "chapter": "N/A",
        "topic": "N/A",
        "subtopic": "N/A"
    }

# OLD CODE TO 
    # def finalize_chunk(chunk: Optional[Dict[str, Any]]) -> None:
    #     if chunk and (chunk['content'] or chunk['images'] or chunk['table_images']):
    #         chunk['content'] = "\n\n".join(chunk['content']).strip()
    #         if chunk['content'] or chunk['images'] or chunk['table_images']:
    #             chunks.append(chunk)

    def finalize_chunk(chunk: Optional[Dict[str, Any]]) -> None:
        if chunk and (chunk['content'] or chunk['image_text'] or chunk['images'] or chunk['table_images']):
            main_text = "\n\n".join(chunk['content']).strip()
            image_text = "\n\n".join(chunk['image_text']).strip()
            if image_text:
                chunk['content'] = main_text + "\n\n-- IMAGE TEXT --\n\n" + image_text if main_text else "-- IMAGE TEXT --\n\n" + image_text
            else:
                chunk['content'] = main_text
            if chunk['content'] or chunk['images'] or chunk['table_images']:
                chunks.append(chunk)


    for el in elements:
        el_chapter = el.get('chapter', "N/A")
        el_topic = el.get('topic', "N/A")
        el_subtopic = el.get('subtopic', "N/A")

        new_chunk_needed = False
        if current_chunk is None:
            new_chunk_needed = True
        elif (el_chapter != last_known_context["chapter"] or
              el_topic != last_known_context["topic"] or
              el_subtopic != last_known_context["subtopic"]):
            new_chunk_needed = True

        if new_chunk_needed:
            finalize_chunk(current_chunk)
            current_chunk = {
                "chapter": el_chapter,
                "topic": el_topic,
                "subtopic": el_subtopic,
                "content": [],
                "image_text": [],
                "images": [],
                "table_images": [],
                "start_page": el['page_num'],
                "end_page": el['page_num']
            }
            last_known_context = {
                "chapter": el_chapter,
                "topic": el_topic,
                "subtopic": el_subtopic
            }

        if current_chunk:
            if el['type'] == 'text':
                sanitized_block = break_long_words(el['content'], config.MAX_WORD_LENGTH)
            elif el['type'] == 'image':
                if el.get('is_graph', False):
                    graph_desc = generate_graph_description(el['base64'], el.get('caption', ''), oai_client, config)
                    current_chunk['image_text'].append(f"[GRAPH DESCRIPTION]: {graph_desc}")
                else:
                    current_chunk['image_text'].append(f"[IMAGE: {el['id']}]")

                current_chunk['images'].append({
                    "id": el['id'], 
                    "data": el['base64'], 
                    "caption": el.get('caption', 'Uncaptioned Image'), 
                    "is_graph": el.get('is_graph', False),
                    "is_slide_image": el.get('is_slide_image', False)
                })
            elif el['type'] == 'table_image':
                placeholder = f"[TABLE_IMAGE: {el['id']}]"
                current_chunk['content'].append(placeholder)
                current_chunk['table_images'].append({"id": el['id'], "data": el['data']})

            current_chunk['end_page'] = el['page_num']
    
    finalize_chunk(current_chunk)
    return chunks

# ==============================================================================
# --- DOCX/DOC/PPT/PPTX to PDF Conversion Logic ---
# ==============================================================================
def convert_office_to_pdf(doc_bytes: bytes, extension: str) -> bytes:
    """Convert DOC/DOCX/PPT/PPTX bytes to PDF bytes using LibreOffice."""
    print(f"  - Attempting to convert {extension} to PDF using LibreOffice...")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        input_path = tmp_path / f"input{extension}"
        
        with open(input_path, 'wb') as f:
            f.write(doc_bytes)
        
        output_pdf_path = input_path.with_suffix('.pdf')

        try:
            subprocess.run([
                'soffice',
                '--headless',
                '--convert-to', 'pdf',
                '--outdir', str(tmp_path),
                str(input_path)
            ], check=True, capture_output=True, text=True) 
            
            if not output_pdf_path.exists():
                raise FileNotFoundError(f"PDF not created by LibreOffice at {output_pdf_path}. Check LibreOffice logs for errors.")
            
            with open(output_pdf_path, 'rb') as pdf_file:
                print(f"  ✅ Successfully converted {extension} to PDF.")
                return pdf_file.read()
        except FileNotFoundError:
            raise RuntimeError("LibreOffice command 'soffice' not found. Please ensure LibreOffice is installed and in your system's PATH.")
        except subprocess.CalledProcessError as e:
            print(f"LibreOffice stdout: {e.stdout}")
            print(f"LibreOffice stderr: {e.stderr}")
            raise RuntimeError(f"LibreOffice conversion failed: {e.stderr} (Return code: {e.returncode})")
        except Exception as e:
            raise RuntimeError(f"An unexpected error occurred during LibreOffice conversion: {e}")

# ==============================================================================
# --- LLM CHUNK REVIEW & CORRECTION ---
# ==============================================================================

def review_document_chunks_with_llm(chunks: List[Dict[str, Any]], doc: Optional[fitz.Document], oai_client: AzureOpenAI, config: PreprocessorConfig, toc_entries: Optional[List[Dict[str, Any]]] = None, llm_derived_outline: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """
    Reviews and potentially corrects the chapter, topic, and subtopic for each chunk
    using an LLM, considering previous chunks, document outline, and TOC.
    """
    print("  - Starting LLM review for chunk hierarchy and context...")
    
    reviewed_chunks = []
    
    global_context_str = ""
    if toc_entries:
        global_context_str += "\n--- Table of Contents/Index ---\n"
        for entry in toc_entries:
            global_context_str += f"- {entry.get('level', 'Entry')}: {entry.get('title')} (Page {entry.get('page')})\n"
    if llm_derived_outline:
        global_context_str += "\n--- AI-Derived Document Outline ---\n"
        for entry in llm_derived_outline:
            global_context_str += f"- {entry.get('level')}: {entry.get('text')} (Page {entry.get('page_num')})\n"
    if not global_context_str:
        global_context_str = "No specific overall document structure (TOC/Outline) found."

    last_confirmed_context = {
        "chapter": "N/A",
        "topic": "N/A",
        "subtopic": "N/A"
    }

    for i, chunk in enumerate(chunks):
        chunk_content_excerpt = chunk['content'][:1000] + "..." if len(chunk['content']) > 1000 else chunk['content']
        chunk_page_range = chunk.get('source_page_range', 'N/A')

        user_prompt_content = f"""
        --- Current Chunk Details ---
        Chunk Number: {chunk['chunk_number']}
        Source Page Range: {chunk_page_range}
        Currently Assigned Chapter: '{chunk['chapter']}'
        Currently Assigned Topic: '{chunk['topic']}'
        Currently Assigned Subtopic: '{chunk['subtopic']}'

        --- Current Chunk Content Excerpt ---
        {chunk_content_excerpt}

        --- Previous Chunk's Confirmed Context ---
        Chapter: '{last_confirmed_context['chapter']}'
        Topic: '{last_confirmed_context['topic']}'
        Subtopic: '{last_confirmed_context['subtopic']}'

        {global_context_str}

        --- Task ---
        You are an expert document structure validator. Based on the previous chunk's confirmed context, the overall document structure, and the content of *this* chunk, evaluate if the 'Currently Assigned Chapter', 'Currently Assigned Topic', and 'Currently Assigned Subtopic' are logically correct and consistent with the document's flow.

        Consider:
        - If the content introduces a new major section, chapter, or topic.
        - If the current assigned values should logically follow from the 'Previous Chunk's Confirmed Context'.
        - If the TOC/Outline provides a more accurate chapter/section mapping for this page range (if applicable).

        If any of the assigned values (Chapter, Topic, Subtopic) are incorrect or can be improved for better logical flow and accuracy:
        - Provide the corrected values.
        - If a level is not applicable or cannot be determined for this chunk, use "N/A".
        - Provide a concise 'reasoning' for your decision (correction or confirmation).

        Return ONLY a JSON object with the following keys:
        - 'validation_status': (string, "correct" if no changes, "corrected" if changes were made)
        - 'corrected_chapter': (string, the validated or corrected Chapter text, or "N/A")
        - 'corrected_topic': (string, the validated or corrected Topic text, or "N/A")
        - 'corrected_subtopic': (string, the validated or corrected Subtopic text, or "N/A")
        - 'reasoning': (string, brief explanation)
        """

        messages = [
            {"role": "system", "content": """You are an expert document structure validator. Your task is to review a chunk of a document and verify or correct its assigned hierarchical context (Chapter, Topic, Subtopic). You will be provided with the current chunk's content, the context from previous chunks, and an overall document outline or Table of Contents if available. Your goal is to ensure logical flow and accurate hierarchical tagging."""},
            {"role": "user", "content": user_prompt_content}
        ]
        
        try:
            response = oai_client.chat.completions.create(
                model=config.AZURE_OPENAI_CHAT_DEPLOYMENT_NAME, # <--- Modified to use AZURE_OPENAI_CHAT_DEPLOYMENT_NAME
                messages=messages,
                max_tokens=500,
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            review_result = json.loads(response.choices[0].message.content)

            chunk['validation_status'] = review_result.get('validation_status', 'error')
            chunk['validation_reasoning'] = review_result.get('reasoning', 'No reasoning provided.')

            if 'corrected_chapter' in review_result and review_result['corrected_chapter'] is not None:
                chunk['chapter'] = sanitize_text(review_result['corrected_chapter'])
            if 'corrected_topic' in review_result and review_result['corrected_topic'] is not None:
                chunk['topic'] = sanitize_text(review_result['corrected_topic'])
            if 'corrected_subtopic' in review_result and review_result['corrected_subtopic'] is not None:
                chunk['subtopic'] = sanitize_text(review_result['corrected_subtopic'])
            
            # Update last_confirmed_context for the next iteration
            last_confirmed_context['chapter'] = chunk['chapter']
            last_confirmed_context['topic'] = chunk['topic']
            last_confirmed_context['subtopic'] = chunk['subtopic']

            print(f"    - Chunk {chunk['chunk_number']} review: {chunk['validation_status']}. Reason: {chunk['validation_reasoning']}")

        except Exception as e:
            print(f"    ❌ LLM review failed for chunk {chunk['chunk_number']}: {e}")
            chunk['validation_status'] = 'failed_review'
            chunk['validation_reasoning'] = f"LLM review failed: {e}"
            # In case of failure, keep the original assigned context, and continue propagating it
            last_confirmed_context['chapter'] = chunk['chapter']
            last_confirmed_context['topic'] = chunk['topic']
            last_confirmed_context['subtopic'] = chunk['subtopic']

        reviewed_chunks.append(chunk)

    print("  ✅ LLM chunk review completed.")
    return reviewed_chunks


# ==============================================================================
# --- Specialized PDF Processing Functions ---
# ==============================================================================

def _process_slide_deck_pdf(doc: fitz.Document, blob_name: str, oai_client: AzureOpenAI, dest_container_client: ContainerClient, config: PreprocessorConfig, parent_id: str, text_analytics_client_instance: TextAnalyticsClient) -> None:
    """Processes a PDF identified as a slide deck."""
    print(f"  - Processing '{blob_name}' as a SLIDE DECK.")
    
    has_toc, toc_entries = detect_toc_presence(doc, oai_client, config)
    toc_chapter_map = {}
    if has_toc and toc_entries:
        for entry in toc_entries:
            if entry.get('level', '').lower() in ['chapter', 'section', 'entry']: 
                toc_chapter_map[entry['page']] = entry['title']
        toc_chapter_map = dict(sorted(toc_chapter_map.items()))

    current_chapter_text = "N/A" 

    parts = blob_name.split("/")
    path_metadata = { 
        "Client": parts[0] if parts else "N/A", 
        "Project": parts[1] if len(parts) > 1 else "N/A", 
        "Module": parts[2] if len(parts) > 2 else "N/A", 
        "Source": parts[3] if len(parts) > 3 else "N/A", 
        "File": "/".join(parts[4:]) if len(parts) > 4 else (parts[-1] if parts else "N/A") 
    }

    chunks_to_review: List[Dict[str, Any]] = []
    
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        
        llm_input_context = {
            "Chapter": current_chapter_text,
            "Topic": "N/A", 
            "Subtopic": "N/A"
        }
        slide_analysis_result = _analyze_slide_page_with_llm(page, page_num, oai_client, config, llm_input_context)

        if slide_analysis_result.get('is_new_chapter', False) and slide_analysis_result.get('chapter_text'):
            current_chapter_text = slide_analysis_result['chapter_text']
        
        assigned_chapter = current_chapter_text 
        if toc_chapter_map:
            active_toc_chapter_from_map = None
            for toc_page, toc_title in toc_chapter_map.items():
                if (page_num + 1) >= toc_page: 
                    active_toc_chapter_from_map = toc_title
                else:
                    break 
            if active_toc_chapter_from_map:
                assigned_chapter = active_toc_chapter_from_map 

        slide_topic_for_chunk = sanitize_text(slide_analysis_result.get('concise_slide_topic', "N/A"))
        slide_title_cleaned = sanitize_text(slide_analysis_result.get('slide_title', f"Page {page_num+1} Content"))

        chunk_content_parts = []
        
        full_slide_text = sanitize_text(page.get_text("text"))
        if full_slide_text:
            chunk_content_parts.append(full_slide_text)

        for block_info in slide_analysis_result.get('slide_content_blocks', []):
            block_text = sanitize_text(block_info.get('text', '')).strip()
            if block_text:
                 if block_text not in full_slide_text:
                     chunk_content_parts.append(block_text)
                 elif len(block_text) > 100 and full_slide_text.count(block_text) == 1:
                      chunk_content_parts.append(block_text)
        
        chunk_images_payload = []
        chunk_table_images_payload = []
        image_description_list = []

        slide_bbox_list = slide_analysis_result.get('slide_bbox', [0.0, 0.0, page.rect.width, page.rect.height])
        slide_bbox = fitz.Rect(slide_bbox_list)
        slide_image_base64 = None
        if not slide_bbox.is_empty and slide_bbox.is_valid and min(slide_bbox.width, slide_bbox.height) > 10:
            try:
                slide_pix = page.get_pixmap(clip=slide_bbox, dpi=200, alpha=True)
                slide_image_base64 = base64.b64encode(slide_pix.tobytes("png")).decode('utf-8')
            except Exception as e:
                print(f"      - Warning: Could not extract main slide image for page {page_num+1}: {e}")
        
        if slide_image_base64:
            main_slide_img_id = f"p{page_num+1}_slide_visual"
            chunk_images_payload.append({
                "id": main_slide_img_id,
                "data": slide_image_base64,
                "caption": slide_title_cleaned,
                "is_graph": False,
                "is_slide_image": True
            })
            image_description_list.append(f"Main slide visual for '{slide_title_cleaned}' (Page {page_num+1}).")

        graph_elements_on_page, _ = extract_graphs_from_page(page, page_num, 0)
        for graph_el in graph_elements_on_page:
            graph_desc = generate_graph_description(graph_el['base64'], graph_el['caption'], oai_client, config)
            chunk_content_parts.append(f"[GRAPH DESCRIPTION]: {graph_desc}") 
            chunk_images_payload.append({
                "id": graph_el['id'],
                "data": graph_el['base64'],
                "caption": graph_el['caption'],
                "is_graph": True,
                "is_slide_image": False
            })
            image_description_list.append(f"Graph '{graph_el['caption']}' (ID: {graph_el['id']}) on page {page_num+1}.")

        page_table_images = extract_tables_as_images(page)
        for tbl_img in page_table_images:
            chunk_content_parts.append(f"[TABLE IMAGE: {tbl_img['id']}]")
            chunk_table_images_payload.append(tbl_img)
            image_description_list.append(f"Table image with ID: {tbl_img['id']} on page {page_num+1}.")

        final_chunk_content = "\n\n".join(filter(None, chunk_content_parts)).strip()
        
        chunk_number = page_num + 1
        chunk_id = base64.urlsafe_b64encode(f"{blob_name}-chunk-{chunk_number:04}".encode()).decode()
        
        chunk_payload = {
            "id": chunk_id,
            "chunk_number": chunk_number,
            "chunk_name": slide_topic_for_chunk if slide_topic_for_chunk != "N/A" else slide_title_cleaned, 
            "parent_id": parent_id,
            **path_metadata,
            "content": final_chunk_content,
            "relevant_snippet": generate_summary(final_chunk_content, oai_client, config), # Add relevant snippet for slide chunks
            "key_phrases": enrich_text_with_key_phrases(final_chunk_content, text_analytics_client_instance), # Add key phrases for slide chunks
            "links": extract_links_from_text(final_chunk_content), 
            "images": chunk_images_payload,
            "table_images": chunk_table_images_payload,
            "tables": [], 
            "chapter": sanitize_text(assigned_chapter),
            "topic": slide_topic_for_chunk, 
            "subtopic": "N/A", 
            "source_page_range": str(page_num + 1),
            "imageDescription": image_description_list 
        }
        chunks_to_review.append(chunk_payload)

    # Pass doc=None as review_document_chunks_with_llm expects it, but it's not applicable for this context
    reviewed_chunks = review_document_chunks_with_llm(chunks_to_review, None, oai_client, config, toc_entries=toc_entries)

    processed_chunks_count = 0
    for chunk_payload in reviewed_chunks:
        json_blob_name = (f"{os.path.dirname(blob_name) or ''}/{os.path.basename(blob_name)}-chunk-{chunk_payload['chunk_number']:04}.json").lstrip('/')
        dest_container_client.upload_blob(name=json_blob_name, data=json.dumps(chunk_payload, indent=2), overwrite=True)
        processed_chunks_count += 1

    print(f"  ✅ Successfully processed and uploaded {processed_chunks_count} slide chunks for {blob_name}.")

def _process_traditional_pdf(doc: fitz.Document, blob_name: str, oai_client: AzureOpenAI, dest_container_client: ContainerClient, config: PreprocessorConfig, parent_id: str, text_analytics_client_instance: TextAnalyticsClient) -> None: # <--- Modified: Added text_analytics_client_instance
    """Processes a PDF identified as a traditional document."""
    print(f"  - Processing '{blob_name}' as a TRADITIONAL DOCUMENT.")

    has_toc, toc_entries = detect_toc_presence(doc, oai_client, config)
    
    llm_derived_outline = get_llm_page_by_page_structure_traditional(doc, oai_client, config)
            
    for p_num in range(len(doc)):
        page = doc.load_page(p_num)
        for block in page.get_text("blocks", sort=True):
            block_text = sanitize_text(block[4])
            if block_text:
                for heading_item in llm_derived_outline:
                    if heading_item['page_num'] == p_num + 1 and sanitize_text(heading_item['text']) in block_text:
                        heading_item['pos'] = block[1] 
                        break 
    
    llm_derived_outline.sort(key=lambda x: (x['page_num'], x.get('pos', 0)))

    toc_chapter_map = {} 
    if has_toc and toc_entries:
        for entry in toc_entries:
            if entry.get('level', '').lower() == 'chapter': 
                toc_chapter_map[entry['page']] = entry['title']
        toc_chapter_map = dict(sorted(toc_chapter_map.items()))
    
    all_elements = extract_document_elements(doc, oai_client, config)
    if not all_elements:
        print("    - No elements found in document.")
        return

    contextualized_elements = assign_context_to_elements(all_elements, llm_derived_outline, toc_chapter_map)
    logical_chunks = assemble_chunks_from_elements(contextualized_elements, oai_client, config)

    parts = blob_name.split("/")
    path_metadata = { 
        "Client": parts[0] if parts else "N/A", 
        "Project": parts[1] if len(parts) > 1 else "N/A", 
        "Module": parts[2] if len(parts) > 2 else "N/A", 
        "Source": parts[3] if len(parts) > 3 else "N/A", 
        "File": "/".join(parts[4:]) if len(parts) > 4 else (parts[-1] if parts else "N/A") 
    }

    chunks_to_review: List[Dict[str, Any]] = []

    for i, chunk_data in enumerate(logical_chunks):
        chunk_number = i + 1
        content_str = chunk_data.get('content', '') 
        chunk_id = base64.urlsafe_b64encode(f"{blob_name}-chunk-{chunk_number:04}".encode()).decode()
        images_payload = chunk_data.get('images', [])
        table_images_payload = chunk_data.get('table_images', [])

        chunk_payload = {
            "id": chunk_id, 
            "chunk_number": chunk_number,
            "chunk_name": " - ".join(filter(None, [chunk_data.get(k) for k in ['chapter', 'topic', 'subtopic']])) or "Document Introduction",
            "parent_id": parent_id, 
            **path_metadata, 
            "content": content_str,
            "relevant_snippet": generate_summary(content_str, oai_client, config), # Add relevant snippet
            "key_phrases": enrich_text_with_key_phrases(content_str, text_analytics_client_instance), # Add key phrases
            "links": extract_links_from_text(content_str), 
            "images": images_payload,
            "table_images": table_images_payload, 
            "tables": [], 
            "chapter": sanitize_text(chunk_data.get('chapter', "N/A")),
            "topic": sanitize_text(chunk_data.get('topic', "N/A")),
            "subtopic": sanitize_text(chunk_data.get('subtopic', "N/A")),
            "source_page_range": str(chunk_data.get('start_page', '')) + ('' if chunk_data.get('start_page') == chunk_data.get('end_page') else '-' + str(chunk_data.get('end_page', ''))),
            "imageDescription": [f"Embedded image with ID: {img['id']} and caption: '{img.get('caption', 'Uncaptioned')}'" for img in images_payload],
        }
        chunks_to_review.append(chunk_payload)

    reviewed_chunks = review_document_chunks_with_llm(chunks_to_review, doc, oai_client, config, toc_entries=toc_entries, llm_derived_outline=llm_derived_outline)

    processed_chunks_count = 0
    for chunk_payload in reviewed_chunks:
        json_blob_name = (f"{os.path.dirname(blob_name) or ''}/{os.path.basename(blob_name)}-chunk-{chunk_payload['chunk_number']:04}.json").lstrip('/')
        dest_container_client.upload_blob(name=json_blob_name, data=json.dumps(chunk_payload, indent=2), overwrite=True)
        processed_chunks_count += 1

    print(f"  ✅ Successfully processed and uploaded {processed_chunks_count} chunks for {blob_name}.")

# ==============================================================================
# --- NEW: VIDEO PROCESSING FUNCTION ---
# ==============================================================================
def _process_video_document(
    blob_stream: io.BytesIO, 
    blob_name: str, 
    openai_chat_client: AzureOpenAI,
    openai_whisper_client: AzureOpenAI,
    text_analytics_client_instance: TextAnalyticsClient,
    dest_container_client: ContainerClient, 
    config: PreprocessorConfig,
    parent_id: str
) -> None:
    """Orchestrates video processing using an efficient consume-and-proceed semantic chunking method."""
    print(f"  - Processing '{blob_name}' as a VIDEO (.mp4).")
    
    # Ensure local temp directory exists and is clean
    if os.path.exists(config.LOCAL_TEMP_DIR): shutil.rmtree(config.LOCAL_TEMP_DIR)
    os.makedirs(config.LOCAL_TEMP_DIR)

    try:
        # Step 1: Transcribe the entire video
        local_mp4_path = os.path.join(config.LOCAL_TEMP_DIR, os.path.basename(blob_name))
        with open(local_mp4_path, "wb") as f: f.write(blob_stream.read())
        wav_path = local_mp4_path.replace(".mp4", ".wav")
        if not extract_audio_to_wav(local_mp4_path, wav_path):
            print("  ❌ Failed to extract audio. Aborting video processing.")
            return

        audio_chunks_paths = split_wav_for_transcription(wav_path, config.VIDEO_AUDIO_CHUNK_SIZE_MB)
        full_transcript = ""
        if not openai_whisper_client:
            print("  ❌ Whisper client not available. Skipping transcription for video."); return
            
        for i, chunk_path in enumerate(audio_chunks_paths):
            print(f"  - Transcribing audio chunk {i+1}/{len(audio_chunks_paths)}...")
            with open(chunk_path, "rb") as audio_file:
                result = openai_whisper_client.audio.transcriptions.create(
                    model=config.AZURE_WHISPER_DEPLOYMENT_NAME,
                    file=audio_file
                )
                full_transcript += result.text.strip() + " "
        
        words = full_transcript.split()
        if len(words) < config.MIN_CHUNK_WORD_COUNT:
            print(f"  - ⚠️ Transcription too short ({len(words)} words) to process into semantic chunks. Skipping video chunking."); return

        print(f"  - Transcription complete ({len(words)} words). Starting iterative semantic chunking.")
        
        all_chunks_data: List[Dict[str, Any]] = []
        current_word_index = 0
        last_known_chapter, last_known_topic = "N/A", "N/A"
        
        # --- Iterative Consumption Logic ---
        while current_word_index < len(words):
            window_start_time = datetime.now()
            # Create a manageable window of text from the current position
            window_end_index = current_word_index + config.PROCESSING_WINDOW_WORD_COUNT
            transcript_window = " ".join(words[current_word_index:window_end_index])
            
            print(f"  - Analyzing window starting at word {current_word_index} (up to {len(transcript_window.split())} words in window)...")
            
            # Use oai_chat_client for semantic chunking
            chunk_data = find_next_semantic_chunk(
                transcript_window, last_known_chapter, last_known_topic,
                openai_chat_client, config
            )

            if not chunk_data or chunk_data.get("word_count", 0) < config.MIN_CHUNK_WORD_COUNT:
                remaining_words_count = len(words) - current_word_index
                print(f"  - No valid chunk found or chunk too small (remaining words: {remaining_words_count}). Attempting to finalize last part.")
                # If there's a significant amount of text left, append it as a final chunk
                if remaining_words_count >= config.MIN_CHUNK_WORD_COUNT:
                    final_text = " ".join(words[current_word_index:])
                    print(f"    - Appending remaining {remaining_words_count} words as final chunk.")
                    all_chunks_data.append({
                        "chapter": last_known_chapter,
                        "topic": last_known_topic,
                        "subtopic": "Conclusion" if last_known_subtopic == "N/A" else f"{last_known_subtopic} (Conclusion)", # Adjust subtopic for conclusion
                        "chunk_text": final_text,
                        "word_count": remaining_words_count
                    })
                break # Exit loop if no valid chunk is found

            # A valid chunk was found, store it and update state
            all_chunks_data.append(chunk_data)
            last_known_chapter = chunk_data.get('chapter', last_known_chapter)
            last_known_topic = chunk_data.get('topic', last_known_topic)
            last_known_subtopic = chunk_data.get('subtopic', "N/A") # Store last subtopic for more context
            consumed_words = chunk_data.get("word_count", 0)
            
            # Consume the processed words by advancing the index
            current_word_index += consumed_words
            print(f"    ✅ Chunk identified and consumed {consumed_words} words. New index: {current_word_index}. Took {datetime.now() - window_start_time}.")

        if not all_chunks_data:
            print("  - ⚠️ Semantic chunking did not identify any chunks for video. Aborting."); return

        # Step 3: Enrich and upload each identified chunk
        parts = blob_name.split("/")
        path_metadata = { 
            "Client": parts[0] if parts else "N/A", 
            "Project": parts[1] if len(parts) > 1 else "N/A", 
            "Module": parts[2] if len(parts) > 2 else "N/A", 
            "Source": parts[3] if len(parts) > 3 else "N/A", 
            "File": "/".join(parts[4:]) if len(parts) > 4 else (parts[-1] if parts else "N/A") 
        }
        
        chunks_to_review = []
        for i, chunk_data in enumerate(all_chunks_data):
            chunk_number = i + 1
            page_content = chunk_data.get('chunk_text', '')
            if not page_content: continue

            chunk_id = base64.urlsafe_b64encode(f"{blob_name}-chunk-{chunk_number:04}".encode()).decode()

            chunk_payload = {
                "id": chunk_id,
                "parent_id": parent_id,
                "chunk_number": chunk_number,
                "source_page_range": str(chunk_number), # For video, map chunk_number to "page" for consistency
                **path_metadata,
                "content": sanitize_text(page_content),
                "relevant_snippet": generate_summary(page_content, openai_chat_client, config),
                "key_phrases": enrich_text_with_key_phrases(page_content, text_analytics_client_instance),
                "links": extract_links_from_text(page_content),
                "chapter": sanitize_text(chunk_data.get('chapter', 'N/A')),
                "topic": sanitize_text(chunk_data.get('topic', 'N/A')),
                "subtopic": sanitize_text(chunk_data.get('subtopic', 'N/A')),
                "imageText": [], "imageTags": [], "imageDescription": [], "images": [], "table_images": [], "tables": [] # Video has no images/tables
            }
            chunks_to_review.append(chunk_payload)

        # Apply LLM review and correction to video chunks
        # Pass doc=None as review_document_chunks_with_llm expects it, but it's not applicable for video
        reviewed_chunks = review_document_chunks_with_llm(chunks_to_review, None, openai_chat_client, config, toc_entries=None, llm_derived_outline=None)

        processed_chunks_count = 0
        for chunk_payload in reviewed_chunks:
            json_blob_name = f"{os.path.dirname(blob_name) or ''}/{os.path.basename(blob_name)}-chunk-{chunk_payload['chunk_number']:04}.json".lstrip('/')
            dest_container_client.upload_blob(name=json_blob_name, data=json.dumps(chunk_payload, indent=2), overwrite=True)
            processed_chunks_count += 1

        print(f"  ✅ Successfully processed video and created {processed_chunks_count} semantic text chunks.")

    finally:
        if os.path.exists(config.LOCAL_TEMP_DIR): shutil.rmtree(config.LOCAL_TEMP_DIR)

# ==============================================================================
# --- MAIN PROCESSING FUNCTION ---
# ==============================================================================
def preprocess_document_for_rag(
    blob_name: str, 
    blob_data_stream: io.BytesIO, 
    file_extension: str, 
    source_container_client: ContainerClient, # Not used here, but kept in signature for consistency if needed
    dest_container_client: ContainerClient, 
    openai_chat_client: AzureOpenAI, # Changed from openai_client to openai_chat_client
    openai_whisper_client: AzureOpenAI, # Added whisper client
    text_analytics_client_instance: TextAnalyticsClient, # Added text analytics client
    config: PreprocessorConfig
) -> Dict[str, str]:
    """
    Main preprocessing function to convert a document (PDF, DOCX, PPTX, TXT, MP4)
    into structured JSON chunks for RAG.

    Args:
        blob_name (str): The full path/name of the blob in the source container.
        blob_data_stream (io.BytesIO): A stream containing the document's content.
        file_extension (str): The file extension (e.g., '.pdf', '.docx', '.mp4').
        source_container_client: The Azure Blob Storage client for the source container.
        dest_container_client: The Azure Blob Storage client for the destination container.
        openai_chat_client: The Azure OpenAI client for chat/vision tasks.
        openai_whisper_client: The Azure OpenAI client for Whisper ASR.
        text_analytics_client_instance: The Azure Text Analytics client.
        config (PreprocessorConfig): Configuration object.

    Returns:
        Dict[str, str]: A dictionary with 'status' ('success' or 'failure') and a 'message'.
    """
    print(f"\n--- Starting preprocessing for: {blob_name} ---")
    
    if file_extension not in config.SUPPORTED_EXTENSIONS:
        return {"status": "failure", "message": f"Unsupported file type: {blob_name}"}

    parent_id = base64.urlsafe_b64encode(blob_name.encode()).decode()

    try:
        if file_extension == '.pdf':
            doc = fitz.open(stream=blob_data_stream, filetype="pdf")
            if doc.is_encrypted:
                doc.close()
                return {"status": "failure", "message": f"Skipping encrypted document: {blob_name}"}
            
            document_type = detect_document_type(doc, openai_chat_client, config)

            if document_type == "slide_deck":
                _process_slide_deck_pdf(doc, blob_name, openai_chat_client, dest_container_client, config, parent_id, text_analytics_client_instance)
            else: # "traditional_document" or fallback
                _process_traditional_pdf(doc, blob_name, openai_chat_client, dest_container_client, config, parent_id, text_analytics_client_instance)
            
            doc.close()
            return {"status": "success", "message": f"Successfully processed PDF: {blob_name}"}

        elif file_extension in ('.doc', '.docx', '.ppt', '.pptx'): 
            try:
                pdf_bytes = convert_office_to_pdf(blob_data_stream.read(), file_extension)
                pdf_stream = io.BytesIO(pdf_bytes)
                
                doc = fitz.open(stream=pdf_stream, filetype="pdf")
                if doc.is_encrypted:
                    doc.close()
                    return {"status": "failure", "message": f"Skipping encrypted document (converted from {file_extension}): {blob_name}"}

                document_type = detect_document_type(doc, openai_chat_client, config)

                if document_type == "slide_deck":
                    _process_slide_deck_pdf(doc, blob_name, openai_chat_client, dest_container_client, config, parent_id, text_analytics_client_instance)
                else:
                    _process_traditional_pdf(doc, blob_name, openai_chat_client, dest_container_client, config, parent_id, text_analytics_client_instance)
                
                doc.close()
                return {"status": "success", "message": f"Successfully converted and processed {file_extension}: {blob_name}"}
            except Exception as e:
                import traceback
                traceback.print_exc()
                return {"status": "failure", "message": f"Conversion and processing of {blob_name} failed: {e}"}

        elif file_extension == '.txt':
            content = blob_data_stream.read().decode('utf-8', errors='ignore')
            if not content.strip():
                return {"status": "success", "message": f"Empty text file: {blob_name}"}
                
            chunk_number = 1
            sanitized_content = sanitize_text(content)
            chunk_id = base64.urlsafe_b64encode(f"{blob_name}-chunk-{chunk_number:04}".encode()).decode()
            
            parts = blob_name.split("/")
            path_metadata = { 
                "Client": parts[0] if parts else "N/A", 
                "Project": parts[1] if len(parts) > 1 else "N/A", 
                "Module": parts[2] if len(parts) > 2 else "N/A", 
                "Source": parts[3] if len(parts) > 3 else "N/A", 
                "File": "/".join(parts[4:]) if len(parts) > 4 else (parts[-1] if parts else "N/A") 
            }

            chunk_payload = {
                "id": chunk_id, 
                "chunk_number": chunk_number,
                "chunk_name": "Full Document Content", 
                "parent_id": parent_id, 
                **path_metadata, 
                "content": sanitized_content,
                "relevant_snippet": generate_summary(sanitized_content, openai_chat_client, config), # Add relevant snippet
                "key_phrases": enrich_text_with_key_phrases(sanitized_content, text_analytics_client_instance), # Add key phrases
                "links": extract_links_from_text(sanitized_content), 
                "images": [],
                "table_images": [], 
                "tables": [],
                "chapter": "N/A",
                "topic": "N/A",
                "subtopic": "N/A",
                "source_page_range": "1", 
                "imageDescription": [],
            }

            json_blob_name = (f"{os.path.dirname(blob_name) or ''}/{os.path.basename(blob_name)}-chunk-{chunk_number:04}.json").lstrip('/')
            dest_container_client.upload_blob(name=json_blob_name, data=json.dumps(chunk_payload, indent=2), overwrite=True)

            return {"status": "success", "message": f"Successfully processed TXT: {blob_name}"}

        elif file_extension == '.mp4': # <--- NEW: Video processing block
            _process_video_document(
                blob_data_stream,
                blob_name,
                openai_chat_client,
                openai_whisper_client,
                text_analytics_client_instance,
                dest_container_client,
                config,
                parent_id
            )
            return {"status": "success", "message": f"Successfully processed MP4: {blob_name}"}


    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "failure", "message": f"Processing of {blob_name} failed: {e}"}

# ==============================================================================
# --- MAIN EXECUTION (Simulates a backend worker) ---
# ==============================================================================
if __name__ == "__main__":
    print("\n--- Document Processing Started ---")
    config = PreprocessorConfig()
    
    blob_service_client: Optional[BlobServiceClient] = None
    openai_chat_client: Optional[AzureOpenAI] = None
    openai_whisper_client: Optional[AzureOpenAI] = None
    text_analytics_client_instance: Optional[TextAnalyticsClient] = None

    try:
        blob_service_client, openai_chat_client, openai_whisper_client, text_analytics_client_instance = initialize_clients(config)
        
        source_container_client = blob_service_client.get_container_client(config.SOURCE_CONTAINER_NAME) 
        dest_container_client = blob_service_client.get_container_client(config.DESTINATION_CONTAINER_NAME)
        image_container_client = blob_service_client.get_container_client(config.IMAGE_CONTAINER_NAME) 

        # Ensure destination container exists
        try:
            dest_container_client.create_container()
            print(f"  - Created container '{config.DESTINATION_CONTAINER_NAME}'.")
        except HttpResponseError as e:
            if "ContainerAlreadyExists" in str(e):
                print(f"  - Container '{config.DESTINATION_CONTAINER_NAME}' already exists.")
            else: 
                raise

    except Exception as e:
        print(f"❌ Initial setup failed: {e}")
        sys.exit(1)

    # State tracking: Cache of already processed files and their last modification times
    processed_map = {}
    try:
        print(f"Loading processed file metadata from '{config.DESTINATION_CONTAINER_NAME}'...")
        for blob_item in dest_container_client.list_blobs():
            match = re.search(r"^(.*)-chunk-\d{4,}\.json$", blob_item.name)
            if match:
                source_name_prefix = match.group(1)
                processed_map[source_name_prefix] = blob_item.last_modified.astimezone(timezone.utc) if blob_item.last_modified else None
        print(f"Loaded metadata for {len(processed_map)} previously processed source documents.")

    except Exception as e:
        print(f"⚠️ State tracking initialization error: {e}. Proceeding without full state tracking.")
        processed_map = {}
    
    files_processed_in_this_run = 0
    
    source_blobs = list(source_container_client.list_blobs())
    print(f"\nFound {len(source_blobs)} source files in '{config.SOURCE_CONTAINER_NAME}'.")
    
    for blob_item in source_blobs:
        blob_name = blob_item.name.replace("\\", "/") 
        file_extension = os.path.splitext(blob_name.lower())[1]

        # Check if needs processing
        last_processed_timestamp = processed_map.get(blob_name)
        
        source_last_modified_utc = blob_item.last_modified.astimezone(timezone.utc) if blob_item.last_modified else None

        if last_processed_timestamp and source_last_modified_utc and source_last_modified_utc <= last_processed_timestamp:
            print(f"  - Skipping {blob_name} (already processed and up-to-date at {last_processed_timestamp}).")
            continue
        
        print(f"\nQueueing {blob_name} for processing...")
        files_processed_in_this_run += 1
        
        try:
            blob_downloader = source_container_client.download_blob(blob_name)
            blob_data_stream = io.BytesIO(blob_downloader.readall())

            result = preprocess_document_for_rag(
                blob_name=blob_name,
                blob_data_stream=blob_data_stream,
                file_extension=file_extension,
                source_container_client=source_container_client,
                dest_container_client=dest_container_client,
                openai_chat_client=openai_chat_client, # Pass chat client
                openai_whisper_client=openai_whisper_client, # Pass whisper client
                text_analytics_client_instance=text_analytics_client_instance, # Pass text analytics client
                config=config
            )
            print(f"Result for {blob_name}: {result['status'].upper()} - {result['message']}")

            if result['status'] == 'success':
                processed_map[blob_name] = source_last_modified_utc

        except Exception as e:
            print(f"  ❌ Critical error processing {blob_name}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n--- Document Processing Completed. Processed {files_processed_in_this_run} new/updated documents. ---")