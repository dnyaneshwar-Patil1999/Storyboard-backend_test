# /app/services/storyboard_service.py

import os
import tempfile
import json
import uuid
import logging
from typing import List, Dict, Any, Tuple

from openai import AzureOpenAI



from app.models.storyboard_models import ApplyChangesRequest, StoryboardRequest, FullStoryboard, DownloadRequest
from app.models.filestructure import get_latest_storyboard_stream
from core.orchestrator import generate_storyboard_pipeline
from core.storyboard_generator import save_storyboards_to_word_document
from core.save_storyboards_to_adls import save_storyboard_to_adls 
from core.storyboard_chatbot import process_storyboard_edits
from app.routes.style_guide_routes import apply_style_guide_to_storyboard
from app.services.job_store import JobStore

logger = logging.getLogger(__name__)


api_key = os.getenv("AZURE_OPENAI_API_KEY")
azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
api_version = os.getenv("AZURE_OPENAI_API_VERSION")
deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")

client = AzureOpenAI(api_key=api_key, azure_endpoint=azure_endpoint, api_version=api_version)

class StoryboardService:
    _last_generated_storyboard = []

    @staticmethod
    def _format_for_frontend(storyboards: List[Dict[str, Any]], module: str, blooms_level: str, compliance: Dict) -> List[Dict[str, Any]]:
        """
        Transforms the raw storyboard JSON from the generator into the exact structure
        required by the Angular frontend. The input keys are now the transformed keys
        from the generator (e.g., "Course_Title").
        """
        formatted_list = []
        for sb in storyboards:
            formatted_sb = {
                "Course_Title": sb.get("Course_Title", ""),
                "Module_Title": sb.get("Module_Title", ""),
                "Topic": sb.get("Topic", "N/A"),
                # Frontend expects a list of strings, which the generator's transform function provides
                "Learning_Objectives": sb.get("Learning_Objectives", []), 
                "Screen_type": sb.get("Screen_type", "Static"),
                # FIXED: Use the blooms_level parameter passed from the original request
                "Blooms_Level": blooms_level,  # Use the original request value
                "Duration_(min)": sb.get("Duration_(min)", "01:00"),
                # Frontend expects a list of base64 strings
                "Source_Images_(base64)": sb.get("Source_Images_(base64)", []), 
                # Frontend expects a list of markdown table strings
                "Source_Tables": sb.get("Source_Tables", []), 
                "On_screen_text": sb.get("On_screen_text", "No content specified"),
                "Narration": sb.get("Narration", "No narration provided"),
                # Frontend expects a list of strings
                "On_screen_Recommendations": sb.get("On_screen_Recommendations", []),
                # Frontend expects a list of strings
                "Developer_Notes": sb.get("Developer_Notes", []), 
                # Add the extra fields from the request
                "Module": module,
                "Bloom_Level": blooms_level, # Redundant but matches Angular model
                "Compliance": compliance
            }
            formatted_list.append(formatted_sb)
        return formatted_list

    @staticmethod
    def generate_storyboard(storyboard_request: StoryboardRequest) -> List[Dict[str, Any]]:
        """
        Generates storyboards from an outline and applies any client style guides.
        """
        try:
            print("Received storyboard generation request:", storyboard_request.dict())
            # Create a job ID for tracking progress
            job_id = JobStore.create_job()
            
            # Extract request parameters
            client = storyboard_request.client
            project = storyboard_request.project
            module = storyboard_request.module
            outline_file = storyboard_request.outline
            blooms_level = storyboard_request.blooms_level
            context_prompt = storyboard_request.context_prompt
            compliance = storyboard_request.compliance

            print(f"compliance {compliance} type {type(compliance)}")

            nasba_value = compliance.nasba
            print("Nasba:", nasba_value)

            print(f"from generate storyboard {blooms_level}")
            
            logger.info(f"Generating storyboard for {client}/{project}/{module}, outline: {outline_file}")
            
            # Update job status
            JobStore.update_job(job_id, {
                "status": "processing",
                "message": f"Generating storyboard content from outline",
                "progress": 20
            })
            
            # Call the orchestrator pipeline with all required parameters
            storyboards = generate_storyboard_pipeline(
                client, project, module, outline_file, blooms_level, compliance, context_prompt
            )




            # print(f"main story board {storyboards}")
            
            if not storyboards:
                logger.warning("No storyboards generated from pipeline")
                JobStore.update_job(job_id, {
                    "status": "error",
                    "message": "Failed to generate storyboard content",
                    "progress": 100
                })
                return []
            # fix for blooms level missing
            
            first_sb = storyboards[0]
            # print(first_sb)
            first_sb["Module_Title"] = storyboard_request.module
            first_sb["Blooms_Level"] = storyboard_request.blooms_level
                
            # Store the storyboards for later access
            StoryboardService._last_generated_storyboard = storyboards
            
            # Update job status
            JobStore.update_job(job_id, {
                "status": "processing",
                "message": f"Applying style guide rules to storyboard",
                "progress": 60
            })
            
            # Apply client style guide rules if available, passing the job_id for progress tracking
            styled_storyboards = apply_style_guide_to_storyboard(
                storyboard_list=storyboards,
                client=client,
                job_id=job_id
            )
            
            # Format for frontend display
            formatted_storyboards = StoryboardService._format_for_frontend(
                styled_storyboards, 
                module, 
                blooms_level, 
                compliance
            )
            
            # Final job status update
            JobStore.update_job(job_id, {
                "status": "completed",
                "message": f"Storyboard generation complete",
                "progress": 100
            })
            
            logger.info(f"Successfully generated {len(formatted_storyboards)} storyboard items")
            return formatted_storyboards
            
        except Exception as e:
            logger.exception(f"Error generating storyboard: {e}")
            if 'job_id' in locals():
                JobStore.update_job(job_id, {
                    "status": "error",
                    "message": f"Error: {str(e)}",
                    "progress": 100
                })
            return []

    @staticmethod
    def get_last_storyboard() -> List[Dict[str, Any]]:
        return StoryboardService._last_generated_storyboard or []
    
    @staticmethod
    def save_storyboard(full_storyboard: FullStoryboard) -> Tuple[bool, str, str]:
        """
        Generates a storyboard DOCX in memory with proper formatting and saves it to ADLS with versioning.
        """
        try:
            # Extract client, project, module
            client = full_storyboard.client
            project = full_storyboard.project
            module = full_storyboard.module
            
            if not all([client, project, module]):
                return False, "Missing required client, project, or module information.", ""
            
            # Use the get_client_short_name function from save_adls instead of inline logic
            from core.save_adls import get_client_short_name
            client_short_name = get_client_short_name(client)
            module_name = module.replace(" ", "")
            filename = f"{client_short_name}_{module_name}_SB.docx"
            
            # Convert storyboard to dictionary
            storyboard_dict = full_storyboard.dict(by_alias=True)
            
            # Log the path before saving for verification
            logger.info(f"Saving storyboard to {client}/{project}/{module}/Storyboard/{filename}")
            deployments_name=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
            # Save to ADLS
            saved_path = save_storyboard_to_adls(storyboard_dict, filename,AzureOpenAI,deployments_name)
            
            if not saved_path:
                return False, "Failed to save storyboard to ADLS.", ""
                
            return True, f"Storyboard saved successfully to {module}/Storyboard folder!", saved_path
        except Exception as e:
            logger.error(f"Failed to save storyboard: {e}", exc_info=True)
            return False, f"An unexpected error occurred while saving: {e}", ""

    @staticmethod
    def generate_storyboard_document(storyboard_data: dict) -> bytes:
        """
        Generate a DOCX file for download.
        Note: This is a simplified version for download. For the full-featured document,
        use the save functionality. This can be enhanced later if needed.
        """
        # This function remains largely unchanged but could be improved.
        storyboards = storyboard_data.get("storyboards", [])
        if not storyboards:
            raise ValueError("No storyboard data provided.")

        # This mapping is simplified. For full fidelity (images, complex tables),
        # this would need to call a document generation function similar to save_storyboard_to_adls.
        storyboards_for_doc = []
        for sb in storyboards:
            storyboards_for_doc.append({
                "Course Title": sb.get("Course_Title", ""),
                "Module Title": sb.get("Module_Title", ""),
                "Topic": sb.get("Topic", ""),
                "Learning Objectives": "\n".join(sb.get("Learning_Objectives", [])),
                "Screen-type": sb.get("Screen_type", ""),
                "Bloom's Level": sb.get("Blooms_Level", ""),
                "Duration (min)": sb.get("Duration_(min)", ""),
                "Source Images (base64)": [], # Images are complex; skipping for basic download
                "Source Tables": sb.get("Source_Tables", []),
                "On-screen text": sb.get("On_screen_text", ""),
                "Narration": sb.get("Narration", ""),
                "On-screen Recommendations": "\n".join(sb.get("On_screen_Recommendations", [])),
                "Developer Notes": "\n".join(sb.get("Developer_Notes", [])),
            })

        # Using the original generator function for download
        # Note: A temporary file is used here. For better performance, this could
        # also be done entirely in memory.
        with tempfile.NamedTemporaryFile(delete=True, suffix=".docx") as tmp:
            save_storyboards_to_word_document(storyboards_for_doc, tmp.name)
            tmp.seek(0)
            return tmp.read()
        
    @staticmethod
    def apply_chatbot_edits(request: ApplyChangesRequest) -> List[Dict[str, Any]]:
        """
        Applies user-prompted edits by writing the current storyboard to a temporary
        file and passing it to the external storyboard_chatbot script. Includes detailed
        tracing for debugging.
        """
        logger.info(f"--- TRACE: Starting chatbot edit process. Prompt: '{request.prompt}'")
        
        # --- 1. Check for existing storyboard data ---
        current_storyboards = StoryboardService.get_last_storyboard()
        if not current_storyboards:
            logger.error("--- TRACE: FAILED. No storyboard found in memory to edit.")
            raise ValueError("No storyboard has been generated yet to apply changes to.")
        logger.info(f"--- TRACE: Found {len(current_storyboards)} storyboards in memory to process.")

        # --- 2. Check and initialize Azure OpenAI Client ---
        try:
            logger.info("--- TRACE: Checking for Azure OpenAI environment variables...")
            
            api_key = os.getenv("AZURE_OPENAI_API_KEY")
            endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
            api_version = os.getenv("AZURE_OPENAI_API_VERSION")
            deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")

            # Explicit check for each variable to identify the exact missing one
            if not api_key:
                logger.error("--- TRACE: FAILED. Environment variable 'AZURE_OPENAI_API_KEY' not found.")
                raise ValueError("Credential 'AZURE_OPENAI_API_KEY' is not configured.")
            if not endpoint:
                logger.error("--- TRACE: FAILED. Environment variable 'AZURE_OPENAI_ENDPOINT' not found.")
                raise ValueError("Credential 'AZURE_OPENAI_ENDPOINT' is not configured.")
            if not api_version:
                logger.error("--- TRACE: FAILED. Environment variable 'AZURE_OPENAI_API_VERSION' not found.")
                raise ValueError("Credential 'AZURE_OPENAI_API_VERSION' is not configured.")
            if not deployment_name:
                logger.error("--- TRACE: FAILED. Environment variable 'AZURE_OPENAI_DEPLOYMENT_NAME' not found.")
                raise ValueError("Credential 'AZURE_OPENAI_DEPLOYMENT_NAME' is not configured.")

            logger.info("--- TRACE: All Azure credentials found. Initializing client.")
            client = AzureOpenAI(
                api_key=api_key,
                azure_endpoint=endpoint,
                api_version=api_version
            )
            logger.info("--- TRACE: Azure OpenAI client initialized successfully.")

        except ValueError as ve:
            # Re-raise value errors with a more user-friendly message for the HTTP response
            raise ConnectionError(f"Could not connect to the AI service. Configuration error: {ve}")
        except Exception as e:
            logger.error(f"--- TRACE: An unexpected error occurred during client initialization: {e}", exc_info=True)
            raise ConnectionError("Could not connect to the AI service. Please check backend configuration.")

        updated_storyboards = None
        
        # --- 3. Temporary File I/O and Chatbot Script Execution ---
        try:
            with tempfile.NamedTemporaryFile(mode='w+', delete=True, suffix='.json', encoding='utf-8') as tmp_file:
                logger.info(f"--- TRACE: Created temporary file at '{tmp_file.name}'.")
                
                # Write current in-memory data to the temporary file
                json.dump(current_storyboards, tmp_file, indent=4)
                tmp_file.flush() # Ensure all data is written to disk before reading from it
                logger.info(f"--- TRACE: Successfully wrote {len(current_storyboards)} storyboards to the temporary file.")
                
                # Call the unmodified chatbot function with the file path
                mode = "all" if request.applyToAll else "one"
                logger.info(f"--- TRACE: Calling 'process_storyboard_edits' in mode '{mode}' for slide number '{request.pageNumber}'.")
                
                updated_storyboards = process_storyboard_edits(
                    file_path=tmp_file.name,
                    user_request=request.prompt,
                    mode=mode,
                    slide_number=request.pageNumber,
                    client=client,
                    deployment_name=deployment_name
                )
                logger.info(f"--- TRACE: 'process_storyboard_edits' completed.")
            
            logger.info(f"--- TRACE: Temporary file '{tmp_file.name}' has been closed and deleted.")

        except Exception as e:
            logger.error(f"--- TRACE: An error occurred during file operations or the call to 'process_storyboard_edits': {e}", exc_info=True)
            raise Exception("An internal error occurred while processing the storyboard edits.")

        # --- 4. Final validation and state update ---
        if not updated_storyboards:
            logger.error("--- TRACE: FAILED. The 'process_storyboard_edits' function returned None or an empty list.")
            raise Exception("The AI editing process failed to return a valid result.")
            
        logger.info(f"--- TRACE: Received {len(updated_storyboards)} updated storyboards from the process.")
        
        # Update the in-memory state with the new version
        StoryboardService._last_generated_storyboard = updated_storyboards
        
        logger.info("--- TRACE: Successfully applied edits and updated the in-memory storyboard. Process finished.")
        return updated_storyboards

    @staticmethod
    def download_storyboard(request: DownloadRequest) -> Tuple[Any, str]:
        """
        Get storyboard file stream for download.
        Returns: (file_stream, filename)
        """
        logger.info(f"Processing storyboard download request: {request.dict()}")
        return get_latest_storyboard_stream(request.client, request.project, request.module)