import os
import json
import logging
import io
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
import uuid
import re


# --- Azure SDK Imports ---
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceNotFoundError
from azure.ai.vision.imageanalysis import ImageAnalysisClient
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from openai import AzureOpenAI
import docx
from core.nasba_checker import check_and_remediate_storyboards
# --- Import Pipeline Components from Your Other Files ---
# Correctly import the main functions/classes from your modules
from .preprocessing import preprocess_document_for_rag, initialize_clients as initialize_preprocessing_clients, PreprocessorConfig
from .Indexer import initialize_clients as initialize_indexer_clients, IndexerOperations, IndexerConfig
from .search_service import OrchestratorAgent
# --- NEW IMPORT FOR STORYBOARD GENERATION ---
from .storyboard_generator import create_base_slide_dict, generate_all_storyboards, read_rows_from_word_table
import tempfile
from . import storyboard_generator as sb_gen
# --- Configuration ---
logger = logging.getLogger(__name__)
load_dotenv()

# --- NEW INGESTION PIPELINE ---
def ingest_files_pipeline(client: str, project: str, module: str, files_to_process: List[str]) -> bool:
    """
    Orchestrates the ingestion pipeline for new files:
    1. Pre-processes specified source files into JSON chunks and uploads them.
    2. Runs the Azure AI Search indexer to ingest the new data.
    Returns True on success, False on failure.
    """
    logging.info(f"--- Starting Ingestion Pipeline for {len(files_to_process)} files in {project}/{module} ---")
    
    try:
        # --- Stage 1: Pre-processing ---
        logging.info("--- Stage 1: Document Pre-processing ---")
        
        preprocessor_config = PreprocessorConfig()
        blob_service_client, openai_chat_client, openai_whisper_client, text_analytics_client = initialize_preprocessing_clients(preprocessor_config)
        
        source_fs_client = blob_service_client.get_container_client(os.getenv("AZURE_STORAGE_FILESYSTEM_NAME"))
        dest_fs_client = blob_service_client.get_container_client(os.getenv("STORAGE_CONTAINER_NAME"))

        if not dest_fs_client.exists():
            logging.warning(f"Destination container '{os.getenv('STORAGE_CONTAINER_NAME')}' not found. Creating it now.")
            dest_fs_client.create_container()
        
        processed_files_count = 0
        for filename in files_to_process:
            blob_path = f"{client}/{project}/{module}/Source/{filename}"
            file_extension = os.path.splitext(filename.lower())[1]

            try:
                logging.info(f"Processing file: {blob_path}")
                blob_client = source_fs_client.get_blob_client(blob_path)
                if not blob_client.exists():
                    logging.warning(f"File not found at '{blob_path}', skipping.")
                    continue
                
                blob_downloader = blob_client.download_blob()
                blob_data_stream = io.BytesIO(blob_downloader.readall())
                
                result = preprocess_document_for_rag(
                    blob_name=blob_path,
                    blob_data_stream=blob_data_stream,
                    file_extension=file_extension,
                    source_container_client=source_fs_client,
                    dest_container_client=dest_fs_client,
                    openai_chat_client=openai_chat_client,
                    openai_whisper_client=openai_whisper_client,
                    text_analytics_client_instance=text_analytics_client,
                    config=preprocessor_config
                )
                
                if result['status'] == 'success':
                    logging.info(f"✅ Successfully pre-processed: {filename}")
                    processed_files_count += 1
                else:
                    logging.error(f"❌ Failed to pre-process {filename}: {result['message']}")

            except Exception as e:
                logging.error(f"A critical error occurred while processing file '{filename}': {e}", exc_info=True)

        if processed_files_count == 0:
            logging.error("No files were successfully processed. Halting ingestion pipeline.")
            return False
        
        # --- Stage 2: Run Indexer ---
        logging.info("--- Stage 2: Running Azure AI Search Indexer ---")
        try:
            indexer_config = IndexerConfig()
            indexer_config.FULL_REBUILD = False 
            
            index_client, indexer_client = initialize_indexer_clients(indexer_config)
            operations = IndexerOperations(index_client, indexer_client, indexer_config)
            
            operations.initialize_and_run_indexer()
            logging.info("✅ Indexer run completed.")
        except Exception as e:
            logging.error(f"❌ Indexer stage failed: {e}", exc_info=True)
            return False

        logging.info(f"✅ Ingestion pipeline completed successfully for {processed_files_count} files.")
        return True

    except Exception as e:
        logging.critical(f"An unrecoverable error occurred in the ingestion pipeline: {e}", exc_info=True)
        return False


# --- REFACTORED SEARCH PIPELINE ---
def search_outline_pipeline(selection_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Orchestrates the search part of the pipeline for outline generation.
    Also saves a local JSON copy of the generated outline.
    """
    try:
        logging.info("--- Starting Search Outline Pipeline ---")
        logging.info(f"Request payload: {json.dumps(selection_json, indent=2, ensure_ascii=False)}")
        search_service = OrchestratorAgent()

        # Generate a unique filename for this request
        request_id = str(uuid.uuid4())
        timestamp = int(time.time())
        output_filename = f"outline_{request_id}_{timestamp}.json"

        client = selection_json.get("client")
        project = selection_json.get("project") 
        files = selection_json.get("files")
        topics = selection_json.get("topics")
        context_prompt = selection_json.get("context_prompt")

        # Pass output_filename to the search service
        search_service.process_search_request(
            client_to_search=client,
            project_to_search=project,
            file_to_search=files,
            # topic=topics,
            context_prompt=context_prompt,
            top_percent=100,
            llm_pre_filter_limit=100,
            llm_batch_size=15,
            output_filename=output_filename  # <-- Pass unique filename
        )

        # Only load the file for this request
        if not os.path.exists(output_filename):
            logging.warning(f"No result file generated for this request: {output_filename}")
            return []

        try:
            with open(output_filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            metadata = data.get("metadata")
            error = metadata.get("error", "")
            
            if 'results' in data:
                final_outline = data['results']
                mapped_outline = []
                for item in final_outline:
                    mapped_item = {
                        "File": item.get("File", ""),
                        "Source Page": item.get("Source Page", ""),
                        "Chapter": item.get("Chapter", ""),
                        "Topic": item.get("Topic", ""),
                        "Subtopic": item.get("Subtopic", ""),
                        "Full Page Content": item.get("Full Page Content", ""),
                        "Durations (Mins)": item.get("Durations (Mins)", ""),
                        "error": error
                    }
                    mapped_outline.append(mapped_item)
                logging.info(f"Search outline pipeline completed successfully. Generated {len(mapped_outline)} outline items.")
                return mapped_outline
            else:
                logging.warning(f"The results file {output_filename} doesn't have the expected 'results' key")
                return []
        finally:
            # ✅ CLEANUP: Delete temporary JSON file after processing
            try:
                if os.path.exists(output_filename):
                    os.remove(output_filename)
                    logging.info(f"Cleaned up temporary file: {output_filename}")
            except Exception as cleanup_error:
                logging.warning(f"Failed to cleanup temporary file {output_filename}: {cleanup_error}")
    except Exception as e:
        logging.critical(f"An unrecoverable error occurred in the search outline pipeline: {e}", exc_info=True)
        print(f"An unrecoverable error occurred in the search outline pipeline: {e}")
        return [{"error": str(e)}]

# --- NEW STORYBOARD GENERATION PIPELINE ---

def _get_outline_and_save_temp(client_name: str, project: str, module: str, outline_name: str) -> str | None:
    """
    Retrieves the outline .docx file from ADLS, saves it to a temporary local file,
    and returns the path to that file. This is the bridge to using the full storyboard_generator.
    """
    try:
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            logger.error("AZURE_STORAGE_CONNECTION_STRING not found.")
            return None
        
        service_client = DataLakeServiceClient.from_connection_string(connection_string)
        filesystem_name = os.getenv("AZURE_STORAGE_FILESYSTEM_NAME", "aptara")
        file_system_client = service_client.get_file_system_client(filesystem_name)
        
        outline_path = f"{client_name}/{project}/{module}/Outline/{outline_name}"
        file_client = file_system_client.get_file_client(outline_path)
        
        download = file_client.download_file()
        file_content_bytes = download.readall()
        
        # Create a temporary file and write the content, ensuring it stays open
        # until we are done with it. The 'delete=False' is crucial.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".docx") as tmp:
            tmp.write(file_content_bytes)
            logger.info(f"Outline from ADLS saved to temporary file: {tmp.name}")
            return tmp.name # Return the path of the temporary file
        
    except ResourceNotFoundError:
        logger.error(f"Outline file not found in ADLS: {outline_path}")
        return None
    except Exception as e:
        logger.error(f"Failed to get outline from ADLS: {e}", exc_info=True)
        return None

def _run_full_generation_from_file(
    temp_file_path: str,
    openai_client: AzureOpenAI,
    deployment_name: str,
    bloom_level_int: int,
    course_title: str,
    context_prompt: str = None,  # Add parameter for frontend context prompt
    compliance_settings: dict = None  # Add parameter for compliance settings
) -> List[Dict[str, Any]]:
    """
    This function orchestrates the entire storyboard generation process by calling the
    functions from the storyboard_generator script in the correct order.
    """

    print(f"from _run_full_generation_from_file {bloom_level_int}")

    # 1. Read outline data from the temporary DOCX file
    input_rows_from_file = sb_gen.read_rows_from_word_table(temp_file_path)

    if not input_rows_from_file:
        logger.warning("No data rows could be read from the outline file.")
        return []

    # 2. Generate the main content slides using frontend context prompt
    # Instead of reading context from file, use the parameter
    main_course_storyboards, manual_objectives_content = sb_gen.generate_all_storyboards(
        all_rows=input_rows_from_file,
        client=openai_client,
        deployment_name=deployment_name,
        bloom_level_int=bloom_level_int,
        course_title=course_title,
        user_context_prompt=context_prompt,  # Use frontend context prompt
        compliance_settings=compliance_settings  # Pass compliance settings
    )

    # 3. Accumulate narration and metadata for intro/outro slides
    full_course_narration_list, unique_module_titles = [], set()
    for slide in main_course_storyboards:
        if slide.get("Narration"):
            narration_cleaned = re.sub(r'-\s*\[Narration for .*?\]\.\.\.\s*', '', slide["Narration"], flags=re.IGNORECASE).strip()
            if narration_cleaned: full_course_narration_list.append(narration_cleaned)
        if slide.get("Module Title"): unique_module_titles.add(slide["Module Title"])
    full_course_narration = "\n\n".join(filter(None, full_course_narration_list))
    if len(full_course_narration) > 100000:
        full_course_narration = full_course_narration[:100000]

    # 4. Generate introductory and concluding slides using the generator's functions
    welcome_slide = sb_gen.generate_welcome_slide(course_title, full_course_narration)
    navigation_slide = sb_gen.generate_navigation_slide(course_title, full_course_narration)
    course_objectives_slide = sb_gen.generate_course_objectives_slide(
        course_title, manual_objectives_content, full_course_narration, list(unique_module_titles),bloom_level_int=bloom_level_int
    )
    assessment_master_content = sb_gen.generate_final_course_assessment_slide(course_title, full_course_narration)
    summary_slide = sb_gen.generate_summary_slide(course_title, full_course_narration)

    # 5. Process and split the final assessment into individual slides (logic from generator's __main__)
    final_assessment_slides = []
    if assessment_master_content and "Questions" in assessment_master_content:
        assessment_intro_slide = assessment_master_content.copy()
        assessment_intro_slide.pop("Questions", None)
        final_assessment_slides.append(assessment_intro_slide)
        course_title_from_filename="Not needed so replaced"
        questions_list = assessment_master_content["Questions"]
        print(f"  - Splitting assessment into {len(questions_list)} individual question slides...")
        for i, q_data in enumerate(questions_list):
            q_type = q_data.get("question_type", "MCSS")  # Default to MCSS if type is missing
            question_slide = create_base_slide_dict(
                course_title_from_filename, "Final Assessment", f"Question {i + 1} ({q_type})", "Interactive Quiz",
                "00:45"
            )
    
            # Logic to format the On-screen text
            on_screen_parts = [
                f"{q_data.get('question', 'Missing question text.')}\n\nInstruction: {q_data.get('instruction', 'Please answer the question.')}"]
    
            if q_type in ["MCSS", "MCMS", "True/False"]:
                options = q_data.get('options', {})
                for key, value in options.items():
                    on_screen_parts.append(f"{key}. {value}")
                correct = q_data.get('correct_answer', 'N/A')
                correct_str = ", ".join(correct) if isinstance(correct, list) else correct
                on_screen_parts.append(f"\nCorrect Answer(s): {correct_str}")
                on_screen_parts.append("\n--- Feedback for All Options ---")
                feedback = q_data.get('feedback', {})
                for key in sorted(options.keys()):
                    on_screen_parts.append(f"Rationale for {key}: {feedback.get(key, 'No rationale provided.')}")
    
            elif q_type == "Drag and Drop":
                on_screen_parts.append("\n--- Items to Match ---")
                on_screen_parts.append("Drag Items: " + ", ".join(q_data.get("drag_items", [])))
                on_screen_parts.append("Drop Targets: " + ", ".join(q_data.get("drop_targets", [])))
                on_screen_parts.append("\n--- Correct Pairing ---")
                answers = q_data.get('correct_answer', {})
                for key, value in answers.items():
                    on_screen_parts.append(f"{key} -> {value}")
                on_screen_parts.append(
                    f"\nRationale: {q_data.get('correct_answer_feedback', 'No rationale provided.')}")
    
            elif q_type == "Fill in the Blank":
                on_screen_parts.append(f"\nCorrect Answer: {q_data.get('correct_answer', 'N/A')}")
                on_screen_parts.append(
                    f"Rationale: {q_data.get('correct_answer_feedback', 'No rationale provided.')}")
    
            formatted_on_screen_text = "\n".join(on_screen_parts)
    
            # Update the slide with values from the AI, defaulting to "N/A" if they are missing.
            question_slide.update({
                "Learning Objectives": "",
                "On-screen text": formatted_on_screen_text,
                "Narration": q_data.get("Narration", ""),
                "On-screen Recommendations": q_data.get("On-screen Recommendations", ""),
                "Developer Notes": q_data.get("Developer Notes", "")
            })
            final_assessment_slides.append(question_slide)
    else:
        print("  - WARNING: Final assessment generation failed or returned no questions.")
 

    # 6. Combine all generated slides into the final course sequence
    combined_final_storyboards = [
        welcome_slide, navigation_slide, course_objectives_slide,
        *main_course_storyboards, *final_assessment_slides, summary_slide
    ]

    # 7. Transform headers for final JSON output using the generator's own function
    storyboards_for_json = sb_gen.transform_storyboard_headers(combined_final_storyboards, sb_gen.HEADER_MAPPING)

    return storyboards_for_json


def generate_storyboard_pipeline(client: str, project: str, module: str, 
                               outline_file: str, blooms_level: str, compliance: dict ,
                               context_prompt: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Generate storyboard content based on client, project, and outline information.
    
    Args:
        client: Client name
        project: Project name
        module: Module name
        outline_file: Outline file name
        blooms_level: Bloom's taxonomy level
        context_prompt: Optional context for generation
        
    Returns:
        List of storyboard items
    """
    request_id = str(uuid.uuid4())
    start_time = time.time()
    logger.info(f"Request ID {request_id}: Starting Storyboard Generation Pipeline.")

    temp_file_path = None
    try:
        # --- Stage 1: Get Outline from ADLS and save to a temporary file ---
        print("\n[STAGE 1/4] Retrieving Outline from ADLS...")
        temp_file_path = _get_outline_and_save_temp(
            client,
            project,
            module,
            outline_file
        )
        if not temp_file_path:
            raise FileNotFoundError("Failed to retrieve or save outline from ADLS.")
        print(f"✅ Success: Outline available at temporary path: {temp_file_path}")
        
        # --- Stage 2: Initialize Azure OpenAI Client ---
        print("\n[STAGE 2/4] Initializing Azure OpenAI Client...")
        openai_client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION")
        )
        deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        print("✅ Success: OpenAI Client initialized.")

        # --- Stage 3: Prepare Parameters and Run Full Generation Process ---
        print("\n[STAGE 3/4] Running Full Storyboard Generation Process...")

        # print(f"from generate storyboard pipe line 1 {blooms_level}")
        # Frontend sends "Level 1 - Remember", etc.
        bloom_level_map = {'Level 1 - Remember': 1, 'Level 2 - Understand': 2, 'Level 3 - Apply': 3}
        blooms_level_str = blooms_level
        bloom_level_int = bloom_level_map.get(blooms_level_str, 2)

        # print(f"from generate storyboard pipe line 2 {bloom_level_int}")

        course_title = f"{module}"

        storyboards = _run_full_generation_from_file(
            temp_file_path=temp_file_path,
            openai_client=openai_client,
            deployment_name=deployment_name,
            bloom_level_int=bloom_level_int,
            course_title=course_title,
            context_prompt=context_prompt,
            compliance_settings=compliance  # Pass the context prompt from parameters
        )





        
        if not storyboards:
            logger.warning(f"Request ID {request_id}: AI generation returned no storyboards.")
            return []
        
        nasba_value = compliance.nasba
        print("Nasba:", nasba_value)
        if nasba_value:
            storyboards = check_and_remediate_storyboards(storyboards, client, deployment_name)

        # if nasba_value:
            
        duration = time.time() - start_time
        logger.info(f"Request ID {request_id}: Generated {len(storyboards)} raw storyboards in {duration:.2f}s.")
        print(f"✅ Success: Generated {len(storyboards)} raw storyboards from AI.")

        # --- Stage 4: Return raw data for service layer to format ---
        return storyboards
        
    except Exception as e:
        logger.error(f"Request ID {request_id}: An error occurred in the storyboard generation pipeline: {e}", exc_info=True)
        return []
    finally:
        # --- CRUCIAL CLEANUP STEP ---
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            logger.info(f"Cleaned up temporary file: {temp_file_path}")

# Delete the old get_outline_from_adls and get_outline_data_from_adls functions
# that were part of the storyboard pipeline, as they are now replaced by _get_outline_and_save_temp.



