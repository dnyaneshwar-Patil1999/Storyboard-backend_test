import pandas as pd
import re
import time
import json
import logging
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from openai import AzureOpenAI, OpenAIError
 
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
 
# ==============================================================================
# === CONFIGURATION & LOGGING ==================================================
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
 
# Environment Variables
SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT", "https://storybot.search.windows.net")
SEARCH_API_KEY = os.getenv("AZURE_SEARCH_API_KEY", "")  # SECURITY: removed hardcoded fallback
SEARCH_INDEX_NAME = os.getenv("AZURE_SEARCH_INDEX_NAME", "pdf-self-contained-vector-index")
 
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "https://aptstorybopenai.openai.azure.com/")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")  # SECURITY: removed hardcoded fallback
AZURE_OPENAI_CHAT_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_NAME", "apt-story-gpt-4o")
OPENAI_CHAT_API_VERSION = "2024-02-15-preview"
 
# GLOBAL SEED FOR CONSISTENCY
GLOBAL_SEED = 42
 
# ==============================================================================
# === CLASS 1: BASE SEARCH AGENT (Utilities & Tools) ===========================
# ==============================================================================
class BaseSearchAgent:
    """
    Base Agent responsible for:
    1. Client Initialization (Search & OpenAI)
    2. Common Helper Functions (Regex, Image processing, Sorting)
    3. Core Search Primitives (Keyword, Hybrid, Filter Building)
    4. Robust Pandas-based Result Processing
    """
    def __init__(self):
        self.search_client = None
        self.chat_client = None
        self.deployment_name = AZURE_OPENAI_CHAT_DEPLOYMENT_NAME
       
        # Default mapping
        self.json_field_mapping = {
            "File": "File", "Source Page": "Source Page", "Chapter": "Chapter",
            "Topic": "Topic", "Subtopic": "Subtopic", "Full Page Content": "Full Page Content",
            "Content_without_images": "Content_without_images",
            "Durations (Mins)": "Durations (Mins)"
        }
        self._initialize_clients()
 
    def _initialize_clients(self):
        try:
            self.search_client = SearchClient(SEARCH_ENDPOINT, SEARCH_INDEX_NAME, AzureKeyCredential(SEARCH_API_KEY))
            self.chat_client = AzureOpenAI(
                api_key=AZURE_OPENAI_API_KEY,
                api_version=OPENAI_CHAT_API_VERSION,
                azure_endpoint=AZURE_OPENAI_ENDPOINT
            )
            logger.info("✅ [BaseAgent] Clients Connected Successfully.")
        except Exception as e:
            logger.error(f"❌ [BaseAgent] Connection Failed: {e}")
 
    # --- HELPERS ---
    def get_sort_key(self, hit: dict):
        file_name = str(hit.get('File', '')).lower()
        if hit.get('Chapter') == "System Message": return (file_name, 9999, 0)
        page_val = hit.get('Source Page') or hit.get('source_page_range') or '0'
        page_match = re.search(r'(\d+)', str(page_val))
        page_num = int(page_match.group(1)) if page_match else 0
        chunk_id = str(hit.get('id', ''))
        chunk_match = re.findall(r'\d+', chunk_id)
        chunk_num = int(chunk_match[-1]) if chunk_match else 0
        return (file_name, page_num, chunk_num)
 
    def _is_toc_or_index_row(self, row: pd.Series) -> bool:
        chapter = str(row.get('chapter', '')).lower()
        topic = str(row.get('topic', '')).lower()
        content = str(row.get('content', '')).lower()
        bad_keywords = ["table of contents", "index", "list of tables", "list of figures"]
        if any(k == chapter for k in bad_keywords) or any(k == topic for k in bad_keywords):
            return True
        if content.count('....') > 5: return True
        return False
 
    def _find_image_in_map(self, image_id: str, image_map: dict, chunk_id: str) -> Optional[str]:
        return image_map.get(image_id.strip().lower()) if image_id else None
 
    def _format_duration(self, text: str) -> str:
        if not text: return "0 min 0 sec"
        word_count = len(text.split())
        seconds = word_count / 2.5
        return f"{int(seconds // 60)} min {int(seconds % 60)} sec"
 
    # --- ROBUST DATA PROCESSOR ---
    def process_raw_results(self, raw_hits: list[dict]) -> list[dict]:
        if not raw_hits: return []
        df_raw = pd.DataFrame(raw_hits)
        mask = df_raw.apply(self._is_toc_or_index_row, axis=1)
        df_filtered = df_raw[~mask].copy()
        if df_filtered.empty: return []
 
        processed_rows = []
        for _, row in df_filtered.iterrows():
            row_dict = row.to_dict()
            content_text = row_dict.get('content', '')
            chunk_id = row_dict.get('id', 'unknown')
           
            # Duration Calculation
            word_count = len(content_text.split())
            row_dict['duration_seconds'] = (word_count / 2.5) if word_count > 0 else 0
           
            # Image Parsing
            image_data_map = {}
            images_json = row_dict.get('images')
            if images_json:
                try:
                    images_list = json.loads(images_json) if isinstance(images_json, str) else images_json
                    for img_obj in images_list:
                        iid = str(img_obj.get('id') or img_obj.get('image_id') or '').strip().lower()
                        b64 = (img_obj.get('data') or img_obj.get('base64') or '').strip()
                        if iid and b64: image_data_map[iid] = b64
                except: pass
 
            # Image Replacement Logic
            def replace_regular_image(match):
                image_id = match.group(1).strip().lower()
                base64_data = self._find_image_in_map(image_id, image_data_map, chunk_id)
                return f"![Image {image_id}](data:image/png;base64,{base64_data})" if base64_data else f"[MISSING_IMAGE:{image_id}]"
 
            content_text = re.sub(r'\[IMAGE:\s*([^\]]+?)\]', replace_regular_image, content_text, flags=re.IGNORECASE)
            row_dict['content'] = content_text
            row_dict['Full Page Content'] = content_text
            row_dict['Content_without_images'] = re.sub(r'!\[.*?\]\(data:image/.*?;base64,.*?\)', '[IMAGE_REMOVED]', content_text)
            row_dict['Durations (Mins)'] = self._format_duration(content_text)
            processed_rows.append(row_dict)
 
        # Sorting & Renaming
        df_processed = pd.DataFrame(processed_rows)
        df_renamed = df_processed.rename(columns={
            'chapter': 'Chapter', 'topic': 'Topic', 'subtopic': 'Subtopic',
            'source_page_range': 'Source Page', 'File': 'File'
        })
        df_renamed['start_page'] = pd.to_numeric(df_renamed['Source Page'].astype(str).str.extract(r'(\d+)')[0], errors='coerce').fillna(0)
        if 'chunk_number' not in df_renamed.columns: df_renamed['chunk_number'] = 0
       
        df_sorted = df_renamed.sort_values(by=["File", "start_page", "chunk_number"], ascending=[True, True, True])
       
        final_results = []
        for _, row in df_sorted.iterrows():
            row_dict = row.to_dict()
            processed_row = {'id': row_dict.get('id'), 'duration_seconds': row_dict.get('duration_seconds')}
            for json_key, df_column_name in self.json_field_mapping.items():
                processed_row[json_key] = row_dict.get(df_column_name)
            final_results.append(processed_row)
        return final_results
 
    # --- SEARCH PRIMITIVES ---
    def build_odata_filter(self, client=None, project=None, files=None):
        parts = []
        if client: parts.append(f"Client eq '{client}'")
        if project: parts.append(f"Project eq '{project}'")
        if files:
            f_list = [files] if isinstance(files, str) else files
            f_parts = [f"File eq '{f}'" for f in f_list]
            parts.append(f"({' or '.join(f_parts)})")
        return " and ".join(parts) if parts else None
 
    def execute_keyword_search(self, query_text: str, active_filter: str, top_n=1000) -> List[dict]:
        if not self.search_client: return []
        try:
            results = self.search_client.search(
                search_text=f'"{query_text}"',
                query_type="simple", search_mode="all",
                search_fields=["chapter", "topic", "subtopic"],
                select=["id", "content", "chapter", "topic", "subtopic", "source_page_range", "chunk_number", "File", "images", "table_images"],
                filter=active_filter, top=top_n
            )
            return self.process_raw_results(list(results))
        except Exception as e:
            logger.error(f"Search failed for '{query_text}': {e}")
            return []
 
    def execute_hybrid_search(self, query_text: str, active_filter: str, top_n=5000) -> List[dict]:
        if not self.search_client: return []
        try:
            results = self.search_client.search(
                search_text=query_text, filter=active_filter, top=top_n,
                select=["id", "content", "chapter", "topic", "subtopic", "source_page_range", "chunk_number", "File", "images", "table_images"]
            )
            return self.process_raw_results(list(results))
        except Exception as e: return []
 
    def execute_fetch_all(self, active_filter: str) -> List[dict]:
        if not self.search_client or not active_filter: return []
        try:
            results = self.search_client.search(
                search_text="*", select=["id", "content", "chapter", "topic", "subtopic", "source_page_range", "chunk_number", "File", "images", "table_images"],
                filter=active_filter, top=2000
            )
            return self.process_raw_results(list(results))
        except Exception as e: return []
 
 
# ==============================================================================
# === CLASS 2: PROCESSES AGENT (Business Logic) ================================
# ==============================================================================
class ProcessesAgent(BaseSearchAgent):
    """
    Handles specific processing tasks and LLM transformations.
    Inherits core search capabilities from BaseSearchAgent.
    """
    def __init__(self):
        super().__init__()

 
    def _generate_content_index(self, pool: List[dict]) -> List[dict]:
        """
        Helper to build a lightweight index (Hierarchy) from a pool of chunks.
        Used to give the LLM a 'Table of Contents' view before deep filtering.
        """
        index_map = {}
        for c in pool:
            # Create a unique key for hierarchy (Chapter -> Topic -> Subtopic)
            chap = c.get('Chapter', 'Unknown')
            top = c.get('Topic', 'Unknown')
            sub = c.get('Subtopic', '')
            
            key = (chap, top, sub)
            
            if key not in index_map:
                index_map[key] = {
                    "Chapter": chap,
                    "Topic": top,
                    "Subtopic": sub,
                    "chunk_ids": []
                }
            index_map[key]["chunk_ids"].append(c['id'])
            
        return list(index_map.values())
  
    # ==============================================================================
    # STEP 1: TOPIC SEARCH + DETAILED FILTERING
    # ==============================================================================

    # 1. MULTI-TOPIC (REVISED FOR QUOTES AND SEMANTIC EXTRACTION)
    def extract_smart_topics(self, prompt: str) -> List[str]:
        if not prompt: return []
        logger.info(f"   🧠 [MULTI-TOPIC AGENT] Analyzing prompt: '{prompt[:60]}...'")

        # --- PHASE 1: LITERAL QUOTE EXTRACTION ---
        quoted_topics = re.findall(r'"([^"]*)"', prompt)

        # --- PHASE 2: SEMANTIC EXTRACTION (LLM) ---
        # UPDATED PROMPT: Explicitly forbids page numbers/ranges as topics
        system_msg = """
        You are a Semantic Search Query Architect. 
        Identify the CORE SUBJECT MATTER.

        RULES:
        1. **IGNORE METADATA**: Do NOT extract page numbers (e.g., "Page 20", "Pages 10-15") as topics.
        2. **IGNORE GENERIC COMMANDS**: Do NOT extract "outline", "summary", "structure", "introduction".
        3. **DETECT SUBJECTS**: Only extract actual subject matter (e.g., "Engine", "History", "Network").
        4. IF the prompt is ONLY about page ranges (e.g., "Outline from page 20 to 30"), return an empty list [].
        
        OUTPUT FORMAT: JSON { "topics": ["topic1", "topic2"] }
        """
        
        try:
            res = self.chat_client.chat.completions.create(
                model=self.deployment_name,
                messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.0,
                seed=GLOBAL_SEED 
            )
            payload = json.loads(res.choices[0].message.content)
            llm_topics = payload.get("topics", [])
            
            # --- PHASE 3: CLEANING ---
            blacklist = ["outline", "theme", "concept", "page", "pages", "summary", "section"]
            clean_llm_topics = []
            for t in llm_topics:
                # Regex to ensure we don't accidentally keep "Page 20" if LLM fails
                if not re.search(r'page\s*\d+', t, re.IGNORECASE) and t.lower() not in blacklist:
                    clean_llm_topics.append(t)
            
            final_topics = list(set(quoted_topics + clean_llm_topics))
            return final_topics
            
        except Exception as e:
            logger.error(f"      ❌ Topic Extraction Error: {e}")
            return quoted_topics

    # ==============================================================================
    # NEW: PAGE RANGE HELPERS
    # ==============================================================================
    def extract_page_constraints(self, prompt: str) -> Optional[tuple]:
        """Returns (start_page, end_page) or None"""
        # Matches: "page 20 to 27", "pg 20-27", "pages 20 through 30"
        pattern = r'(?:page|pg|slide)[s]?\s*(\d+)\s*(?:to|-|through)\s*(\d+)'
        match = re.search(pattern, prompt, re.IGNORECASE)
        if match:
            start = int(match.group(1))
            end = int(match.group(2))
            return (start, end)
        return None

    def apply_page_range_filter(self, pool: List[dict], start_p: int, end_p: int) -> List[dict]:
        if not pool: return []
        logger.info(f"   📄 [PAGE FILTER] Keeping content between Page {start_p} and {end_p}...")
        
        filtered_pool = []
        for c in pool:
            # Extract number from "Source Page" (e.g., "Page 25" -> 25)
            raw_page = str(c.get('Source Page', '0'))
            p_match = re.search(r'(\d+)', raw_page)
            if p_match:
                page_num = int(p_match.group(1))
                if start_p <= page_num <= end_p:
                    filtered_pool.append(c)
        
        return filtered_pool

    def search_detected_topics(self, topics: List[str], active_filter: str = None) -> List[dict]:
        if not topics: return []
        
        logger.info(f"   🔍 [STEP 1: TOPIC SEARCH] Executing search for: {topics}")
        
        # ==============================================================================
        # PHASE 1: STRICT KEYWORD SEARCH
        # ==============================================================================
        keyword_results_map = {}
        
        for topic in topics:
            kw_hits = self.execute_keyword_search(topic, active_filter)
            for hit in kw_hits:
                if 'error' not in hit:
                    keyword_results_map[hit['id']] = hit

        # ==============================================================================
        # PHASE 2: CONDITIONAL BRANCHING
        # ==============================================================================
        
        # CONDITION A: Found Keyword Matches -> Return immediately
        if len(keyword_results_map) > 0:
            logger.info(f"   🎯 [STRATEGY] Found {len(keyword_results_map)} Keyword matches. Returning results early.")
            return list(keyword_results_map.values())
            
        # CONDITION B: 0 Matches -> Index-Aware Fallback (Metadata + Hierarchy + Semantic)
        logger.info("   ⚠️ [STRATEGY] 0 Keyword matches found. Initiating Index-Aware Fallback Strategy.")
        
        # 1. Fetch "All Chunks"
        raw_pool = self.execute_fetch_all(active_filter)
        if not raw_pool:
            logger.warning("      ❌ No content available in file(s) to filter.")
            return []
        
        # Create map for fast lookup
        pool_map = {c['id']: c for c in raw_pool}
        final_kept_ids = set()

        for topic in topics:
            # --- SUB-STEP A: EXACT METADATA FILTER (Local) ---
            metadata_kept = set()
            normalized_topic = topic.lower().strip()
            
            # We will use this set to exclude already kept items from the LLM check
            for chunk in raw_pool:
                ch = str(chunk.get('Chapter', '')).lower()
                tp = str(chunk.get('Topic', '')).lower()
                sub = str(chunk.get('Subtopic', '')).lower()
                
                if (normalized_topic in ch) or (normalized_topic in tp) or (normalized_topic in sub):
                    metadata_kept.add(chunk['id'])
            
            final_kept_ids.update(metadata_kept)
            logger.info(f"      🔹 [{topic}] Metadata Check: Auto-kept {len(metadata_kept)} chunks based on headers.")

            # --- SUB-STEP B: INDEX-AWARE SEMANTIC FILTER ---
            # 1. Build Index of REMAINING chunks
            remaining_pool = [c for c in raw_pool if c['id'] not in metadata_kept]
            if not remaining_pool: continue

            content_index = self._generate_content_index(remaining_pool)
            logger.info(f"      🗂️ [{topic}] Index Analysis: Checking {len(content_index)} hierarchy sections...")

            # 2. Ask LLM to pick relevant SECTIONS first (Hierarchy Level)
            index_system_msg = f"""
            You are a Content Hierarchy Analyst. Target Topic: "{topic}"
            
            Task:
            1. Review the provided Content Index (Chapter -> Topic -> Subtopic).
            2. Identify sections that *likely contain* information about "{topic}".
            3. Select sections based on conceptual relevance (synonyms, related fields).
            4. Return JSON {{ 'relevant_chunk_ids': [list of all chunk_ids from selected sections] }}
            """

            # Batch the Index to avoid token limits (Indexes are small, so batch size can be large)
            INDEX_BATCH_SIZE = 20
            relevant_section_ids = set()

            for i in range(0, len(content_index), INDEX_BATCH_SIZE):
                batch = content_index[i : i + INDEX_BATCH_SIZE]
                try:
                    res = self.chat_client.chat.completions.create(
                        model=self.deployment_name, 
                        messages=[{"role": "system", "content": index_system_msg}, {"role": "user", "content": json.dumps(batch)}],
                        response_format={"type": "json_object"}, temperature=0.0, seed=GLOBAL_SEED
                    )
                    payload = json.loads(res.choices[0].message.content)
                    ids = payload.get("relevant_chunk_ids", [])
                    relevant_section_ids.update(ids)
                except Exception as e:
                    logger.warning(f"         ⚠️ Index LLM Error: {e}.")

            # 3. Deep Semantic Scan on CANDIDATE chunks (Filtered by Index)
            # Only process chunks that belong to the "Relevant Sections" identified above.
            candidate_pool = [c for c in remaining_pool if c['id'] in relevant_section_ids]
            
            if candidate_pool:
                logger.info(f"      🧠 [{topic}] Semantic Deep Read: Scanning {len(candidate_pool)} chunks identified via Index...")
                
                semantic_msg = f"""
                You are a Semantic Content Analyzer. Target Concept: "{topic}"
                Task: Read the 'Content_without_images'. Return JSON {{ 'relevant_ids': [] }} if relevant.
                """
                
                SEMANTIC_BATCH_SIZE = 10
                for i in range(0, len(candidate_pool), SEMANTIC_BATCH_SIZE):
                    batch = candidate_pool[i : i + SEMANTIC_BATCH_SIZE]
                    llm_input = [{"id": c['id'], "Content_Snippet": c.get('Content_without_images', '')[:800]} for c in batch]
                    
                    try:
                        res = self.chat_client.chat.completions.create(
                            model=self.deployment_name, 
                            messages=[{"role": "system", "content": semantic_msg}, {"role": "user", "content": json.dumps(llm_input)}],
                            response_format={"type": "json_object"}, temperature=0.0, seed=GLOBAL_SEED
                        )
                        payload = json.loads(res.choices[0].message.content)
                        valid = [bid for bid in payload.get("relevant_ids", []) if bid in {c['id'] for c in batch}]
                        final_kept_ids.update(valid)
                    except: pass
            
            logger.info(f"      ✅ [{topic} Result] Total Kept: {len(final_kept_ids)}")

        # 3. FINAL AGGREGATION
        final_results = [pool_map[bid] for bid in final_kept_ids if bid in pool_map]
        logger.info(f"   🎉 [STEP 1 FINAL] Final Total (Index-Aware Fallback) = {len(final_results)}")
             
        return final_results




    def apply_inclusion_exclusion_logic(self, pool: List[dict], prompt: str) -> List[dict]:
        if not pool: return []
        
        logger.info(f"   2️⃣ [STEP 2: INC/EXC] Checking constraints against prompt: '{prompt}'")
        
        # 1. SEPARATION (Preserved)
        keyword_safe_ids = set()   # X
        hybrid_pool_to_filter = [] # Candidates for Y
        
        for c in pool:
            if c.get('is_keyword_hit', False) is True:
                keyword_safe_ids.add(c['id'])
            else:
                hybrid_pool_to_filter.append(c)

        x_start = len(keyword_safe_ids)
        y_start = len(hybrid_pool_to_filter)
        logger.info(f"      📊 Input Split: {x_start} Keyword (Auto-Keep) | {y_start} Hybrid (To Filter)")

        # Optimization: Return early if no hybrid items
        if not hybrid_pool_to_filter:
            logger.info(f"      ⏭️ No hybrid items. Returning {x_start} keyword items.")
            unique_results = []
            seen = set()
            for c in pool:
                if c['id'] in keyword_safe_ids and c['id'] not in seen:
                    unique_results.append(c)
                    seen.add(c['id'])
            return unique_results

        # 2. INDEX-AWARE FILTERING (HYBRID ONLY)
        filtered_hybrid_ids = set()
        
        # --- A. Build Index for Hybrid Pool ---
        content_index = self._generate_content_index(hybrid_pool_to_filter)
        
        # --- B. Analyze Constraints vs Index (High Level) ---
        index_system_msg = """
        You are a Semantic Content Gatekeeper. 
        Analyze the 'User Prompt' for constraints (Include X, Exclude Y).
        
        Task:
        1. Review the Index (Chapter -> Topic).
        2. Identify sections that VIOLATE negative constraints (e.g. User: "Exclude Code" -> Mark "Appendix: Code Samples" as bad).
        3. Identify sections that MATCH positive constraints.
        4. If a section is neutral/safe, KEEP IT.
        
        Return JSON { 'relevant_chunk_ids': [list of chunk_ids to KEEP] }
        """
        
        # Batch index processing
        INDEX_BATCH_SIZE = 20
        candidate_ids_from_index = set()
        
        for i in range(0, len(content_index), INDEX_BATCH_SIZE):
            batch = content_index[i : i + INDEX_BATCH_SIZE]
            try:
                res = self.chat_client.chat.completions.create(
                    model=self.deployment_name, 
                    messages=[
                        {"role": "system", "content": index_system_msg}, 
                        {"role": "user", "content": f"User Prompt: {prompt}\n\nIndex Batch: {json.dumps(batch)}"}
                    ],
                    response_format={"type": "json_object"}, temperature=0.0, seed=GLOBAL_SEED
                )
                payload = json.loads(res.choices[0].message.content)
                candidate_ids_from_index.update(payload.get("relevant_chunk_ids", []))
            except Exception as e:
                logger.warning(f"      ⚠️ Index Filter Error: {e}. Keeping batch safely.")
                for entry in batch: candidate_ids_from_index.update(entry['chunk_ids'])

        # --- C. Deep Semantic Check (Only on Survivors) ---
        # Only process items that passed the Index Check
        survivor_pool = [c for c in hybrid_pool_to_filter if c['id'] in candidate_ids_from_index]
        
        semantic_msg = """
        You are a Content Filter. Enforce User Constraints on the TEXT content.
        1. IF chunk violates a negative constraint -> DISCARD.
        2. IF chunk fits positive/neutral -> KEEP.
        Return JSON { 'relevant_ids': [] }
        """
        
        SEMANTIC_BATCH_SIZE = 10
        for i in range(0, len(survivor_pool), SEMANTIC_BATCH_SIZE):
            batch = survivor_pool[i : i + SEMANTIC_BATCH_SIZE]
            llm_input = [{"id": c['id'], "Topic": c.get('Topic'), "Content_Snippet": c.get('Content_without_images', '')[:800]} for c in batch]
            
            try:
                res = self.chat_client.chat.completions.create(
                    model=self.deployment_name, 
                    messages=[{"role": "system", "content": semantic_msg}, {"role": "user", "content": f"User Prompt: {prompt}\n\nContent Batch: {json.dumps(llm_input)}"}],
                    response_format={"type": "json_object"}, temperature=0.0, seed=GLOBAL_SEED
                )
                payload = json.loads(res.choices[0].message.content)
                valid = [bid for bid in payload.get("relevant_ids", []) if bid in {c['id'] for c in batch}]
                filtered_hybrid_ids.update(valid)
            except Exception as e:
                # Fail open (keep if error)
                for c in batch: filtered_hybrid_ids.add(c['id'])

        logger.info(f"      ✅ [Hybrid Filter Result] Kept {len(filtered_hybrid_ids)} / {len(hybrid_pool_to_filter)}")

        # 3. FINAL RECONSTRUCTION
        final_pool = []
        added_ids_tracker = set() 
        
        for c in pool:
            cid = c['id']
            # Logic: Keep if Keyword OR (Hybrid AND Filtered)
            is_valid = (cid in keyword_safe_ids) or (cid in filtered_hybrid_ids)
            
            if is_valid and cid not in added_ids_tracker:
                final_pool.append(c)
                added_ids_tracker.add(cid)

        # 4. STATS
        total_final = len(final_pool)
        removed_count = len(pool) - total_final
        logger.info(f"   ✅ [STEP 2 FINAL] Final Total = {total_final} (Removed: {removed_count})")
        
        return final_pool
 

 
    # ==============================================================================
    # STEP 3: AUDIENCE ADAPTATION
    # ==============================================================================
    def apply_audience_filter(self, pool: List[dict], prompt: str) -> List[dict]:
        if not pool: return []
        
        logger.info(f"   3️⃣ [STEP 3: AUDIENCE] Filtering {len(pool)} chunks (Permissive Mode)...")

        BATCH_SIZE = 10
        kept_ids = set()
        total_batches = (len(pool) + BATCH_SIZE - 1) // BATCH_SIZE
        
        # ------------------------------------------------------------------
        # CHANGE 1: The System Message now emphasizes "High Retention"
        # and "Keep by Default".
        # ------------------------------------------------------------------
        system_msg = """
        You are a Permissive Audience Filter. Your goal is to RETAIN as much context as possible.
        
        Instructions:
        1. Identify the target audience (e.g., Beginner, Expert, Executive).
        2. DEFAULT STRATEGY: KEEP the chunk.
        3. EXCLUDE ONLY IF there is a DRASTIC MISMATCH, specifically:
           - REJECT if the audience is "Non-Technical" and the chunk is raw, unexplained code or complex math equations without text.
           - REJECT if the audience is "Expert" and the chunk is purely a definition of a very common term (e.g., "What is a computer?").
        4. If the chunk provides background, history, or general context, ALWAYS KEEP IT, regardless of complexity.
        
        If you are unsure, KEEP the chunk.
        
        Return JSON { 'relevant_ids': [list of all IDs to keep] }
        """
        
        for i in range(0, len(pool), BATCH_SIZE):
            batch = pool[i : i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1

            # Prepare input (Truncated)
            llm_input = []
            for c in batch:
                content_snippet = c.get('Content_without_images', '')
                if len(content_snippet) > 800: content_snippet = content_snippet[:800] + "..."
                llm_input.append({
                    "id": c['id'], 
                    "Topic": c.get('Topic'),
                    "Subtopic": c.get('Subtopic'),
                    "Content_Snippet": content_snippet
                })

            try:
                # ------------------------------------------------------------------
                # CHANGE 2: Added a gentle reminder in the user prompt to be permissive
                # ------------------------------------------------------------------
                user_content = (
                    f"User Prompt: {prompt}\n\n"
                    f"Candidates: {json.dumps(llm_input)}\n\n"
                    f"Task: Return 'relevant_ids' for everything except strictly unsuitable content."
                )

                res = self.chat_client.chat.completions.create(
                    model=self.deployment_name, 
                    messages=[
                        {"role": "system", "content": system_msg}, 
                        {"role": "user", "content": user_content}
                    ],
                    response_format={"type": "json_object"}, 
                    temperature=0.0, # Keep temp 0 for consistency
                    seed=GLOBAL_SEED
                )
                
                payload = json.loads(res.choices[0].message.content)
                batch_kept_ids = payload.get("relevant_ids", [])
                
                # Validation check: If LLM returns nothing, likely an error in "permissive" mode.
                # In this specific strict-to-loose transition, if it drops >80% of a batch, 
                # we might optionally want to just keep the whole batch (failsafe).
                batch_ids = {c['id'] for c in batch}
                valid_kept = [bid for bid in batch_kept_ids if bid in batch_ids]
                
                # Failsafe: If the filter accidentally wiped the whole batch, restore it.
                if len(valid_kept) == 0 and len(batch) > 0:
                    logger.warning(f"      ⚠️ Batch {batch_num} returned 0 items. Failsafe triggered: Keeping all.")
                    valid_kept = list(batch_ids)

                kept_ids.update(valid_kept)
                logger.info(f"      👉 Batch {batch_num}/{total_batches}: Kept {len(valid_kept)}/{len(batch)} chunks.")

            except Exception as e:
                logger.warning(f"      ⚠️ Batch {batch_num} Error: {e}. Keeping batch.")
                for c in batch: kept_ids.add(c['id'])

        final_pool = [c for c in pool if c['id'] in kept_ids]
        logger.info(f"   ✅ [STEP 3 FINAL] Retained {len(final_pool)} / {len(pool)} chunks.")
        return final_pool

    # ==============================================================================
    # STEP 5: GENERIC OUTLINE (FALLBACK STRUCTURE)
    # ==============================================================================
    def apply_generic_outline_logic(self, pool: List[Dict], prompt: str) -> List[Dict]:
        """
        Used when specific topics aren't detected but user wants 'Structure' or 'Outline'.
        Groups by Chapter/Topic and sorts simply.
        """
        if not pool: return []
        logger.info("   5️⃣ [STEP 5: GENERIC STRUCTURE] Grouping content by Metadata...")

        # 1. Sort by Chapter, then Topic, then Subtopic
        # This is a metadata sort, no LLM needed usually, but effective.
        def safe_str(val): return str(val) if val else "z"
        
        sorted_pool = sorted(
            pool, 
            key=lambda x: (safe_str(x.get('Chapter')), safe_str(x.get('Topic')), safe_str(x.get('Subtopic')))
        )
        
        # 2. Optional: If pool is huge, pick representative chunks (First/Last of each topic)
        # For now, we return the sorted list.
        logger.info(f"      ✅ [STEP 5 FINAL] Sorted {len(sorted_pool)} chunks by Chapter/Topic hierarchy.")
        return sorted_pool


     # ==============================================================================
    # STEP 6: SYNTHETIC CONTENT GENERATION
    # ==============================================================================
    def generate_synthetic_content(self, pool: List[Dict], prompt: str) -> Dict | None:
        """
        Generates a Summary, Quiz, or Abstract if requested.
        Note: No batching here. We need context to write the new item.
        """
        logger.info("   6️⃣ [STEP 6: SYNTHETIC GEN] Analyzing request for generation...")
        
        gen_type = "Summary"
        if "quiz" in prompt.lower(): gen_type = "Quiz"
        elif "abstract" in prompt.lower(): gen_type = "Abstract"
        elif "intro" in prompt.lower(): gen_type = "Introduction"

        # Prepare context (Truncate if too large)
        context_text = ""
        for c in pool[:15]: # Limit to top 15 chunks to avoid context overflow
            context_text += f"- {c.get('Content_without_images', '')[:300]}\n"

        system_msg = f"You are a Content Author. Write a {gen_type} based on the provided snippets."
        
        try:
            res = self.chat_client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": context_text}
                ],
                temperature=0.7, # Higher temp for creativity
                seed=GLOBAL_SEED
            )
            
            content = res.choices[0].message.content
            
            new_chunk = {
                "id": f"syn_{int(time.time())}",
                "Topic": "Generated Content",
                "Subtopic": gen_type,
                "Content_without_images": content,
                "duration_seconds": 120 # Estimate
            }
            logger.info(f"      ✅ [STEP 6 FINAL] Generated 1 new '{gen_type}' chunk.")
            return new_chunk

        except Exception as e:
            logger.error(f"      ❌ Generation Error: {e}. Skipping.")
            return None

    # ==============================================================================
    # STEP 7: DURATION ENFORCEMENT
    # ==============================================================================

    def extract_duration_from_prompt(self, prompt: str) -> int | None:
        if not prompt: return None
        match = re.search(r'(\d+)\s*(minute|min|hour|hr)', prompt, re.IGNORECASE)
        if match:
            val = int(match.group(1))
            unit = match.group(2).lower()
            return val * 60 if 'hour' in unit or 'hr' in unit else val
        return None


    def select_chunks_for_duration(self, pool: List[Dict], target_mins: int) -> List[Dict]:
        if not pool: return []
 
        # 1. Define Limits & Calc Status
        buffer_ratio = 1.2 
        max_allowed_seconds = target_mins * 60 * buffer_ratio
        current_total_seconds = sum(c.get('duration_seconds', 0) for c in pool)
        
        current_mins = current_total_seconds / 60
        max_mins = max_allowed_seconds / 60

        logger.info(f"   7️⃣ [STEP 7: DURATION] Target: {target_mins}m | Current: {current_mins:.1f}m | Max: {max_mins:.1f}m")
 
        # A. Fast Pass
        if current_total_seconds <= max_allowed_seconds:
            logger.info(f"      ✅ Under limit (By {(max_mins - current_mins):.1f}m). Keeping all.")
            return sorted(pool, key=lambda x: x.get('id', 0)) # Stable sort
       
        # B. Optimization Needed (LLM Selection)
        logger.info(f"      ✂️ Over limit by {(current_mins - max_mins):.1f}m. Optimizing via LLM...")
        
        sorted_pool = sorted(pool, key=lambda x: x.get('id', 0))
        
        # We cannot batch here effectively because LLM needs to weigh options against each other.
        # We limit input size to ensure it fits context.
        # Prepare input (Truncated)
        llm_input = []
        
        # --- FIX IS HERE: Changed 'batch' to 'sorted_pool' ---
        # We limit to top 30 to prevent token overflow if the pool is massive
        for c in sorted_pool[:30]: 
            content_snippet = c.get('Content_without_images', '')
            if len(content_snippet) > 800: content_snippet = content_snippet[:800] + "..."
            llm_input.append({"id": c['id'], "Chapter": c.get('Chapter'), "Topic": c.get('Topic'),"Subtopic": c.get('Subtopic'),"Content_Snippet": content_snippet})
 
        system_msg = (
            f"Select chunks to fit approx {target_mins} minutes. "
            f"Strict Max: {max_mins:.1f} mins. "
            "Prioritize: Intro > Core Concepts > Summary. "
            "Return JSON { 'selected_ids': [] }."
        )
 
        selected_ids = set()
        try:
            res = self.chat_client.chat.completions.create(
                model=self.deployment_name,
                messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": json.dumps(llm_input)}],
                response_format={"type": "json_object"}, temperature=0.0, seed=GLOBAL_SEED
            )
            payload = json.loads(res.choices[0].message.content)
            selected_ids = set(payload.get("selected_ids", []))
            logger.info(f"      🤖 LLM selected {len(selected_ids)} priority chunks.")
        except Exception as e:
            logger.error(f"      ❌ Duration LLM Error: {e}. Fallback to sequential selection.")
            selected_ids = {c['id'] for c in sorted_pool} # Fallback: keep all, let math trim

        # C. Mathematical Enforcement
        final_list = []
        accumulated_seconds = 0
        
        # Filter down to what LLM wanted first, then trim strictly
        llm_approved_pool = [c for c in sorted_pool if c['id'] in selected_ids]
        if not llm_approved_pool: llm_approved_pool = sorted_pool # Fallback

        for chunk in llm_approved_pool:
            dur = chunk.get('duration_seconds', 0)
            if (accumulated_seconds + dur) <= max_allowed_seconds:
                final_list.append(chunk)
                accumulated_seconds += dur
            # else: Drop

        final_mins = accumulated_seconds / 60
        logger.info(f"      ✅ [STEP 7 FINAL] Result: {final_mins:.1f}m. (Kept: {len(final_list)}, Dropped: {len(pool) - len(final_list)})")
        return final_list



    # ==============================================================================
    # STEP 8: LOGICAL SEQUENCING (AUTO)
    # ==============================================================================
    def apply_llm_logical_sequencing(self, pool: List[Dict]) -> List[Dict]:
        if len(pool) < 2: return pool
        
        logger.info(f"   8️⃣ [STEP 8: AUTO SEQ] Sequencing {len(pool)} chunks for logical flow...")

        # Map Index -> Item
        item_map = {i: c for i, c in enumerate(pool)}
        
        # Light input: Index + Metadata only (No heavy content)
        llm_input = [{
            "index": i, 
            "Topic": c.get('Topic', ''), 
            "Subtopic": c.get('Subtopic', ''),
            "Chapter": c.get('Chapter', '')
        } for i, c in enumerate(pool)]

        system_msg = """
        You are an Instructional Designer. Reorder indices for best logical flow.
        Flow: Intro -> Concepts -> Advanced -> Summary.
        Return JSON { 'ordered_indices': [0, 4, 2...] } using ALL indices.
        """

        try:
            res = self.chat_client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": json.dumps(llm_input)}
                ],
                response_format={"type": "json_object"}, temperature=0.0, seed=GLOBAL_SEED
            )
            
            payload = json.loads(res.choices[0].message.content)
            new_order_indices = payload.get("ordered_indices", [])
            
            # Validation
            if set(item_map.keys()) != set(new_order_indices):
                logger.warning("      ⚠️ Sequencing indices mismatch. Returning original.")
                return pool
            
            reordered_pool = [item_map[idx] for idx in new_order_indices]
            
            if reordered_pool != pool:
                logger.info(f"      ✅ [STEP 8 FINAL] Re-ordered successfully. New Start: {[c.get('Topic') for c in reordered_pool[:2]]}...")
            else:
                logger.info("      ✅ [STEP 8 FINAL] Original order was optimal.")
                
            return reordered_pool
        except Exception as e:
            logger.error(f"      ❌ Sequencing Error: {e}. Returning original.")
            return pool


    # ==============================================================================
    # STEP 9: MANUAL SEQUENCING (CORRECTED)
    # ==============================================================================
    def apply_manual_sequencing(self, pool: List[Dict], prompt: str) -> List[Dict]:
        if not pool: return []
        
        logger.info(f"   9️⃣ [STEP 9: MANUAL SEQ] Analyzing prompt for custom ordering...")
        
        # Skip if any chunk has error
        if any("error" in c and c["error"] for c in pool):
            logger.info("      ⚠️ Skipping manual sequencing for error objects.")
            return pool

        # ==============================================================================
        # STRATEGY 2: LLM SEMANTIC SEQUENCING
        # Use this only for abstract requests (e.g., "Group by topic", "Put Introduction first")
        # ==============================================================================
        
        # 1. CREATE LIGHTWEIGHT INPUT WITH INDICES
        llm_input = []
        for i, c in enumerate(pool):
            # Optimization: If list is long (>15), DO NOT send content snippet. 
            # It confuses the model's ordering ability. Send only metadata.
            include_snippet = len(pool) < 15 
            
            item_data = {
                "index": i,
                "Chapter": c.get('Chapter', 'N/A'),
                "Topic": c.get('Topic', 'N/A'),
                "Subtopic": c.get('Subtopic', 'N/A'),
                "Source Page" : c.get('Source Page', 'N/A')
            }
            
            if include_snippet:
                content_snippet = c.get('Content_without_images', '')
                if len(content_snippet) > 200: content_snippet = content_snippet[:200] + "..." # Reduce snippet size
                item_data["Content_Snippet"] = content_snippet
            
            llm_input.append(item_data)
        
        system_msg = """
        You are an Editor. Re-arrange the content flow based on the User Prompt.
        
        INSTRUCTIONS:
        1. Analyze the provided items.
        2. Re-order the indices based on the user's request.
        3. You MUST return a JSON object containing a list called 'ordered_indices'.
        4. INCLUDE ALL INDICES from the input. Do not drop any.
        
        Output format: { "ordered_indices": [2, 0, 1, 4, 3] }
        """
        
        try:
            res = self.chat_client.chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": f"User Prompt: {prompt}\n\nContent Items: {json.dumps(llm_input)}"}
                ],
                response_format={"type": "json_object"}, temperature=0.0, seed=GLOBAL_SEED
            )
            
            payload = json.loads(res.choices[0].message.content)
            ordered_indices = payload.get("ordered_indices", [])
            
            # 2. ROBUST RECONSTRUCTION
            final_pool = []
            seen_indices = set()
            
            for idx in ordered_indices:
                if isinstance(idx, int) and 0 <= idx < len(pool):
                    if idx not in seen_indices:
                        final_pool.append(pool[idx])
                        seen_indices.add(idx)
            
            # 3. FAIL-SAFE: Append missing items
            missing_count = 0
            for i in range(len(pool)):
                if i not in seen_indices:
                    final_pool.append(pool[i])
                    missing_count += 1
            
            if missing_count > 0:
                logger.warning(f"      ⚠️ LLM missed {missing_count} items. They were appended to the end.")

            logger.info(f"      ✅ [STEP 9 FINAL] Re-ordered {len(final_pool)} chunks based on user request.")
            return final_pool

        except Exception as e:
            logger.error(f"      ❌ Manual Sequencing Error: {e}. Returning original.")
            return pool

    # ==============================================================================
    # STEP: PAGE NUMBER SORTING (SINGLE FILE FORCE)
    # ==============================================================================
    def apply_page_number_sorting(self, pool: List[Dict]) -> List[Dict]:
        """
        Deterministic sort based on page numbers (Ascending).
        Used strictly for single-file scenarios.
        """
        logger.info("   🔢 [SINGLE FILE MODE] Enforcing Page Number Sorting (Ascending).")
        
        def get_page_number(item):
            # Extract the first number found in 'Source Page' (e.g., "Page 12" -> 12, "10-11" -> 10)
            raw_page = str(item.get('Source Page', '999999'))
            match = re.search(r'(\d+)', raw_page)
            if match:
                return int(match.group(1))
            return 999999 # Push items with no page number to the end

        try:
            # Sort ascending based on the helper function
            sorted_pool = sorted(pool, key=get_page_number)
            logger.info(f"      ✅ [SORT FINAL] Deterministically sorted {len(sorted_pool)} chunks by page number.")
            return sorted_pool
        except Exception as e:
            logger.warning(f"      ⚠️ Python page sort failed ({e}). Returning original.")
            return pool


    def _execute_sequencing_llm(self, pool, llm_input, system_msg, label):
        try:
            res = self.chat_client.chat.completions.create(
                model=self.deployment_name,
                messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": json.dumps(llm_input)}],
                response_format={"type": "json_object"},
                temperature=0.0,
                seed=GLOBAL_SEED
            )
            ordered_ids = json.loads(res.choices[0].message.content).get("ordered_ids", [])
            chunk_map = {c['id']: c for c in pool}
            final_seq = [chunk_map[cid] for cid in ordered_ids if cid in chunk_map]
           
            # Fail-safe: Append missing
            missing = [c for c in pool if c['id'] not in ordered_ids]
            if missing: final_seq.extend(sorted(missing, key=self.get_sort_key))
            return final_seq
        except: return sorted(pool, key=self.get_sort_key)


 

 
 
 
# ==============================================================================
# === CLASS 3: ORCHESTRATOR AGENT (Master Pipeline) ============================
# ==============================================================================
class OrchestratorAgent(ProcessesAgent):
    """
    Master Agent that orchestrates the search and transformation pipeline
    based on a strict 1-to-9 execution sequence.
    """
 
    def process_search_request(self, **kwargs) -> List[Dict]:
        start_time = time.time()
       
        # --- INPUTS ---
        prompt = kwargs.get("context_prompt", "").strip()
        files = kwargs.get("file_to_search", [])
        manual_topics = kwargs.get("topic", [])
        output_filename = kwargs.get("output_filename", "Orchestrator_Output")
       
        # Normalize manual_topics to list if string
        if isinstance(manual_topics, str):
            manual_topics = [t.strip() for t in manual_topics.split(',') if t.strip()]
       
        # Filter Construction
        active_filter = self.build_odata_filter(
            kwargs.get("client_to_search"), kwargs.get("project_to_search"), files
        )
 
        logger.info("="*80)
        logger.info(f"🚀 ORCHESTRATOR STARTED | Prompt: '{prompt}' | Files: {files}")
 
        # ======================================================================
        # STEP 1: TOPIC DETECTION & SEARCH
        # ======================================================================
        current_pool = []
        
        # 1. Detect Semantic Topics (Now ignores "Page 20")
        detected_topics = self.extract_smart_topics(prompt)
        all_topics = list(set(manual_topics + detected_topics))

        # 2. Check for Page Constraints (NEW)
        page_range = self.extract_page_constraints(prompt)

        # 3. Decision Matrix
        if all_topics:
            logger.info(f"   1️⃣ [TOPIC DETECTED] Topics: {all_topics}")
            current_pool = self.search_detected_topics(all_topics, active_filter)
        elif files:
            # THIS IS WHERE "Page 20 to 27" requests will now fall
            logger.info("   1️⃣ [NO SEMANTIC TOPIC] Fetching all content (Generic/Structural Request).")
            current_pool = self.execute_fetch_all(active_filter)
        else:
            logger.info("   1️⃣ [FALLBACK] Hybrid search on raw prompt.")
            current_pool = self.execute_hybrid_search(prompt, active_filter)

        # ======================================================================
        # INTERMEDIATE STEP: STRICT PAGE FILTERING
        # ======================================================================
        if page_range:
            pre_count = len(current_pool)
            start_p, end_p = page_range
            current_pool = self.apply_page_range_filter(current_pool, start_p, end_p)
            logger.info(f"      📊 [PAGE RANGE STATS] {pre_count} -> {len(current_pool)} chunks")
 
        if not current_pool:
            logger.warning("   ⚠️ No content found. Returning empty.")
            return []
 
        # ======================================================================
        # PIPELINE EXECUTION (Steps 2 - 9)
        # ======================================================================
       
        # --- 2. INCLUSION / EXCLUSION (UPDATED) ---
        # Logic: Run ONLY if explicit constraint keywords exist in the prompt.
        should_run_step_2 = any(w in prompt.lower() for w in ["include", "exclude", "don't", "only", "remove", "skip", "ensure"])
        
        if should_run_step_2:
            pre_count = len(current_pool)
            logger.info("   2️⃣ [STEP 2] Applying Inclusion/Exclusion Logic (Topic Detected or Keywords Found)...")
            current_pool = self.apply_inclusion_exclusion_logic(current_pool, prompt)
            logger.info(f"      📊 [STEP 2 STATS] {pre_count} -> {len(current_pool)} chunks (Removed: {pre_count - len(current_pool)})")
 
        # --- 3. AUDIENCE TYPE ---
        if any(w in prompt.lower() for w in [ "audience"]):
            pre_count = len(current_pool)
            logger.info("   3️⃣ [STEP 3] Applying Audience Filter...")
            current_pool = self.apply_audience_filter(current_pool, prompt)
            logger.info(f"      📊 [STEP 3 STATS] {pre_count} -> {len(current_pool)} chunks (Removed: {pre_count - len(current_pool)})")
 
        # --- 5. GENERIC (Structure/Outline) ---
        # Ensure Step 5 (Generic Outline) runs if no detected topics exist (e.g. Page Range request)
        if not all_topics and any(w in prompt.lower() for w in ["outline", "structure", "analyze"]):
            pre_count = len(current_pool)
            logger.info("   5️⃣ [STEP 5] Applying Generic Outline Structure...")
            current_pool = self.apply_generic_outline_logic(current_pool, prompt)
            logger.info(f"      📊 [STEP 5 STATS] Re-sorted {len(current_pool)} chunks (Count unchanged: {len(current_pool) - pre_count})")
 
        # --- 6. GENERATION (Synthetic Content) ---
        if any(w in prompt.lower() for w in ["write", "quiz", "abstract"]):
            pre_count = len(current_pool)
            logger.info("   6️⃣ [STEP 6] Generating Synthetic Content...")
            new_row = self.generate_synthetic_content(current_pool, prompt)
            if new_row:
                current_pool.append(new_row)
            logger.info(f"      📊 [STEP 6 STATS] {pre_count} -> {len(current_pool)} chunks (Added: {len(current_pool) - pre_count})")
 
        # --- 7. DURATION ---
        target_mins = self.extract_duration_from_prompt(prompt)
        if target_mins:
            pre_count = len(current_pool)
            logger.info(f"   7️⃣ [STEP 7] Applying Duration Limit ({target_mins} mins)...")
            current_pool = self.select_chunks_for_duration(current_pool, target_mins)
            logger.info(f"      📊 [STEP 7 STATS] {pre_count} -> {len(current_pool)} chunks (Removed: {pre_count - len(current_pool)})")
 
        # --- SEQUENCING LOGIC (UPDATED) ---
        
        # Determine File Count
        files_list = files if isinstance(files, list) else ([files] if files else [])
        is_single_file = (len(files_list) == 1)
        
        if is_single_file:
            # CASE A: SINGLE FILE -> Force Page Number Sorting
            pre_count = len(current_pool)
            logger.info("   🔢 [SEQUENCING] Single File detected. Forcing Page Number Sorting.")
            current_pool = self.apply_page_number_sorting(current_pool)
            logger.info(f"      📊 [SEQUENCING STATS] Sorted {len(current_pool)} chunks.")
            
        else:
            # CASE B: MULTI FILE -> Logical Sequencing + Optional Manual
            
            # 8. Logical Sequencing (ALWAYS Attempt First for Multi-File)
            if len(current_pool) > 1:
                pre_count = len(current_pool)
                logger.info("   8️⃣ [STEP 8] Applying Logical Sequencing (Auto)...")
                current_pool = self.apply_llm_logical_sequencing(current_pool)
                logger.info(f"      📊 [STEP 8 STATS] Re-ordered {len(current_pool)} chunks (Count change: {len(current_pool) - pre_count})")
            else:
                logger.info("   8️⃣ [STEP 8] Logical Sequencing skipped (Insufficient chunks < 2).")
    
            # 9. Manual Sequencing (RUN SECOND if keywords present)
            manual_keywords = ["organise", "first", "last", "then", "start", "end", "order", "sequence", "arrange", "organize", "restructure", "=>", "oraganizing", "organising"]
            has_manual_seq = any(w in prompt.lower() for w in manual_keywords)
        
            if has_manual_seq:
                pre_count = len(current_pool)
                logger.info("   9️⃣ [STEP 9] Applying Manual Sequencing...")
                current_pool = self.apply_manual_sequencing(current_pool, prompt)
                logger.info(f"      📊 [STEP 9 STATS] Re-ordered {len(current_pool)} chunks (Count change: {len(current_pool) - pre_count})")
 
        # ======================================================================
        # FINAL SAVING
        # ======================================================================
       
        # Construct Metadata Dictionary for the save function
        base_metadata = {
            "Client": kwargs.get("client_to_search", "N/A"),
            "Project": kwargs.get("project_to_search", "N/A"),
            "Context Prompt": prompt,
            "Files": ", ".join(files) if isinstance(files, list) else str(files),
            "Topic": ", ".join(manual_topics) if manual_topics else "Auto-Detected"
        }
 
        # Call save_results with current_pool (the processed data) and the constructed metadata
        self.save_results(current_pool, base_metadata, output_filename)
       
        logger.info(f"✅ ORCHESTRATION COMPLETE. Final Count: {len(current_pool)}")
        return current_pool
 
 
 
    def save_results(self, results: list, metadata_inputs: dict, filename_prefix: str, output_filename: str = None, inject_template_rows: bool = False):
        """
        Save orchestration results to a JSON file.

        Args:
            results: List of processed content rows
            metadata_inputs: Metadata dict to embed in output
            filename_prefix: Base filename
            output_filename: Optional override filename
            inject_template_rows: If True (legacy behavior), prepends Welcome/Nav/Course Overview
                                  rows and appends Knowledge Check rows after each chapter.
                                  DEFAULT IS FALSE — reviewers consistently flagged these phantom
                                  rows ("Welcome screen not in source", "KC even before content",
                                  "Section not present in source slides"). Set True only when the
                                  downstream process requires the template structure.
        """
        if not results: return
        total_seconds = sum(result.get('duration_seconds', 0) for result in results)
        total_duration = f"{int(total_seconds // 60)} min {int(total_seconds % 60)} sec"
        template_row = {key: "" for key in self.json_field_mapping.keys()}

        if inject_template_rows:
            # LEGACY behavior: inject Welcome/Nav/Course Overview + KC rows
            enhanced_results = [
                {**template_row, "Chapter": "Introductions", "Topic": "Welcome"},
                {**template_row, "Chapter": "Introductions", "Topic": "Navigation Tour"},
                {**template_row, "Chapter": "Introductions", "Topic": "Course Overview and Objectives"}
            ]

            error = results[0].get("error") if results and results[0].get("error") else None
            last_chapter = None
            for row in results:
                if last_chapter and row.get("Chapter") != last_chapter:
                    if "introduction" not in last_chapter.lower():
                        enhanced_results.append({**template_row, "Chapter": last_chapter, "Topic": "Knowledge Check", "error": error})
                enhanced_results.append(row)
                last_chapter = row.get("Chapter")

            if last_chapter:
                enhanced_results.append({**template_row, "Chapter": last_chapter, "Topic": "Knowledge Check", "error": error})

            enhanced_results.extend([
                {**template_row, "Chapter": "Course Summary", "Topic": "", "error": error},
                {**template_row, "Chapter": "Assessment", "Topic": "", "error": error}
            ])
        else:
            # DEFAULT: pass through source content without injecting phantom rows.
            # This matches what reviewers expect — the CO should reflect the source
            # document, not a predetermined template.
            error = results[0].get("error") if results and results[0].get("error") else None
            enhanced_results = list(results)

        output_data = {
            "metadata": {**metadata_inputs, "Duration": total_duration, "error": error},
            "results": [{k: v for k, v in row.items() if k in self.json_field_mapping} for row in enhanced_results]
        }
        print("---The output_filename starts here---")
        print(output_filename)
        filename = output_filename if output_filename else f"{filename_prefix}"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ Created: {filename}")

 
# ==============================================================================
# === EXECUTION ================================================================
# ==============================================================================
if __name__ == "__main__":
    agent = OrchestratorAgent()
    agent.process_search_request(
        context_prompt="Create an outline on Network Configuration",
        client_to_search="Roshan_Jan",
        project_to_search="MyCC",
        file_to_search=["Day 4 - Networking Connections.pptx"]
    )

    