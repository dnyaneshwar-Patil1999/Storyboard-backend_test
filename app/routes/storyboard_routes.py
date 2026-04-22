# /app/routes/storyboard_routes.py
import os
from fastapi import File, Form, Path, UploadFile, status
import io
import logging
import urllib.parse
from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from app.models.filestructure import parse_outline_metadata,download_file_from_adls,extract_section_improved,find_pattern_content,is_likely_header
from app.models.storyboard_models import ComplianceModel, OutlineMetadataResponse, StoryboardRequest, StoryboardItem, FullStoryboard, ApplyChangesRequest, DownloadRequest
from app.services.storyboard_service import StoryboardService
from app.services.outline_service import OutlineService
from app.models.project_models import get_style_guide_info, upload_style_guide
from app.routes.style_guide_routes import get_rules_from_adls,process_pdf_and_extract_rules,save_rules_to_adls
from core.save_storyboards_to_adls import parse_duration,format_hhmmss,keep_table_on_one_page
# Configure logger for this module
logger = logging.getLogger(__name__)

router = APIRouter()
from fastapi import BackgroundTasks
from app.services.job_store import JobStore


import re
from langdetect import detect, DetectorFactory

# Ensure consistent results from langdetect
DetectorFactory.seed = 0

from openai import AzureOpenAI

# --- Azure OpenAI Client Setup ---
api_key = os.getenv("AZURE_OPENAI_API_KEY")
azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
api_version = os.getenv("AZURE_OPENAI_API_VERSION")
deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")

client = AzureOpenAI(api_key=api_key, azure_endpoint=azure_endpoint, api_version=api_version)

def is_gibberish(text: str) -> bool:
    """Use LLM to check if text is gibberish."""
    prompt = f"""
You are a strict text quality validator.
Determine whether the following text is *mostly gibberish* — meaning it contains 
random characters, nonsensical words, numbers mixed with symbols, or meaningless repetition.
If it looks like a valid name, phrase, or sentence in any natural language, 
say "no". Otherwise, say "yes".

Text:
{text}

Respond with ONLY one word: "yes" or "no".
"""
    try:
        response = client.chat.completions.create(
            model=deployment_name,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2,
            temperature=0
        )
        answer = response.choices[0].message.content.strip().lower()
        return answer.startswith("y")
    except Exception as e:
        print("LLM check failed:", e)
        return False

@router.post("/submitStoryboard")
def submit_storyboard(
    input_data: StoryboardRequest = Body(...),
    background_tasks: BackgroundTasks = None
):
    # ✅ Check if context_prompt contains gibberish
    if input_data.context_prompt and is_gibberish(input_data.context_prompt):
        return JSONResponse(
            content={
                "status": "error",
                "message": "The context prompt appears to contain gibberish or meaningless text. Please provide a clearer context."
            },
            status_code=400
        )
        

    # ✅ Create background job
    job_id = JobStore.create_job()
    background_tasks.add_task(run_storyboard_job, job_id, input_data)

    return {"job_id": job_id}
 
def run_storyboard_job(job_id, input_data):
    from app.services.storyboard_service import StoryboardService
    try:
        storyboard = StoryboardService.generate_storyboard(input_data)
        JobStore.set_result(job_id, storyboard)
    except Exception as e:
        JobStore.set_error(job_id, str(e))
 
@router.get("/storyboardStatus/{job_id}")
def get_storyboard_status(job_id: str):
    from app.services.job_store import JobStore
    job = JobStore.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] == "processing":
        return {"status": "processing"}
    elif job["status"] == "done":
        return {"status": "done", "storyboard": job["result"]}
    else:
        return {"status": "error", "error": job["Failed to generate storyboard"]}
 
@router.get("/getStoryboard")
def get_storyboard_route():
    try:
        storyboard = StoryboardService.get_last_storyboard()
        if not storyboard:
            logging.warning("No storyboard has been generated yet")
            return JSONResponse(content=[])
       
        return JSONResponse(content=storyboard)
    except Exception as e:
        logging.error(f"Failed to get storyboard: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An internal server error occurred while retrieving the storyboard."
        )

# ===== STORYBOARD SAVE/DOWNLOAD ENDPOINTS =====

@router.post("/saveStoryboard")
async def save_storyboard(
    full_storyboard: FullStoryboard,
    background_tasks: BackgroundTasks = None
):
    """
    Save the storyboard to ADLS with proper formatting asynchronously.
    """
    try:
        # ✅ Create background job for saving storyboard
        job_id = JobStore.create_job()
        background_tasks.add_task(run_save_storyboard_job, job_id, full_storyboard)

        return {"job_id": job_id, "status": "processing", "message": "Storyboard save operation started"}
        
    except Exception as e:
        logger.error(f"Failed to initiate storyboard save job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred while initiating the storyboard save operation.")

def run_save_storyboard_job(job_id: str, full_storyboard: FullStoryboard):
    """
    Background task to save the storyboard asynchronously.
    """
    try:
        success, message, path = StoryboardService.save_storyboard(full_storyboard)
        
        if not success:
            JobStore.set_error(job_id, message)
        else:
            JobStore.set_result(job_id, {
                "status": "success", 
                "message": message, 
                "path": path
            })
            
    except Exception as e:
        logger.error(f"Failed to save storyboard in background job: {e}", exc_info=True)
        JobStore.set_error(job_id, f"An unexpected error occurred while saving the storyboard: {str(e)}")

@router.get("/saveStoryboardStatus/{job_id}")
async def get_save_storyboard_status(job_id: str):
    """
    Check the status of a storyboard save operation.
    """
    job = JobStore.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Save job not found")
    
    if job["status"] == "processing":
        return {"status": "processing"}
    elif job["status"] == "done":
        return job["result"]  # This will include status, message, and path
    else:
        return {"status": "error", "error": job.get("error", "Failed to save storyboard")}

@router.post("/downloadStoryboard")
def download_storyboard(request: DownloadRequest):
    """
    Download the latest storyboard file from Azure Data Lake Storage.
    """
    try:
        # Fetch the latest storyboard file from ADLS
        file_stream, filename = StoryboardService.download_storyboard(request)
        logger.info(f"Successfully retrieved storyboard file: {filename}")
        # Return as streaming response with proper headers
        return StreamingResponse(
            file_stream,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )
    except Exception as e:
        logger.error(f"Error downloading storyboard from ADLS: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to download storyboard: {str(e)}")
 
@router.post("/applyChanges")
def apply_changes_route(
    request: ApplyChangesRequest,
    background_tasks: BackgroundTasks = None
):
    job_id = JobStore.create_job()
    background_tasks.add_task(run_apply_changes_job, job_id, request)
    return {"job_id": job_id}
 
def run_apply_changes_job(job_id, request):
    from app.services.storyboard_service import StoryboardService
    try:
        updated_storyboard = StoryboardService.apply_chatbot_edits(request)
        JobStore.set_result(job_id, updated_storyboard)
    except Exception as e:
        JobStore.set_error(job_id, str(e))
 
@router.get("/applyChangesStatus/{job_id}")
def get_apply_changes_status(job_id: str):
    job = JobStore.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] == "processing":
        return {"status": "processing"}
    elif job["status"] == "done":
        return {"status": "done", "updated_storyboard": job["result"]}
    else:
        return {"status": "error", "error": job["error"]}

@router.post("/uploadOutlineFile")
async def upload_outline_to_adls(
    client: str = Form(...),
    project: str = Form(...),
    module: str = Form(...),
    file: UploadFile = File(...)
):
    """
    API endpoint to upload a DOCX outline file directly to ADLS.
    """
    try:
        file_bytes = await file.read()
        
        success, message, outline_name = OutlineService.upload_outline_file(
            file.filename, file_bytes, client, project, module
        )
        
        if not success:
            raise HTTPException(status_code=500, detail=message)

        return {
            "message": message,
            "outlineName": os.path.basename(outline_name) if outline_name else ""
        }

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Failed to upload outline file for {client}/{project}/{module}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An internal server error occurred during file upload."
        )
        
@router.get(
    "/getOutlineMetadata",
    response_model=OutlineMetadataResponse,
    summary="Fetch metadata from an outline file",
    response_description="Returns the user prompt and topic extracted from the outline file"
)
async def get_outline_metadata(
    client: str = Query(..., description="Client name"),
    project: str = Query(..., description="Project name"),
    module: str = Query(..., description="Module name"),
    outline_filename: str = Query(..., description="Outline DOCX filename")
):
    """
    Fetches metadata (User Prompt and Topic) from a specific outline DOCX file stored in ADLS.
 
    **Parameters**:
    - `client`: The client name
    - `project`: The project name
    - `module`: The module name
    - `outline_filename`: The name of the outline file (DOCX)
 
    **Returns**:
    - `user_prompt`: Extracted user prompt from the outline file
    - `topic`: Extracted topic from the outline file
    - `filename`: Name of the outline file
    """
    try:
        logger.info(f"Fetching outline metadata for file: {outline_filename} (Client: {client}, Project: {project}, Module: {module})")
 
        # Download the outline file from ADLS
        file_content = download_file_from_adls(
            client=client,
            project=project,
            module=module,
            filename=outline_filename
        )
 
        if not file_content:
            logger.warning(f"Outline file '{outline_filename}' not found in ADLS.")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Outline file '{outline_filename}' not found."
            )
 
        # Parse the DOCX content
        metadata = parse_outline_metadata(file_content)
        print("====================")
        print(metadata)
 
        if not metadata:
            logger.error(f"Metadata extraction failed for file '{outline_filename}'")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Unable to extract metadata from the outline file."
            )
 
        logger.info(f"Successfully fetched metadata for file: {outline_filename}")
 
        print(metadata.get("user_prompt", ""))
 
 
        # return OutlineMetadataResponse(
        #     user_prompt="Create a detailed outline with practical examples and case studies1",
        #     # topic=metadata.get("topic", ""),
        #     filename="AC_Introduction_CO.docx"
        # )
 
        return OutlineMetadataResponse(
            user_prompt=metadata.get("user_prompt", ""),
            # topic=metadata.get("topic", ""),
            filename=outline_filename
        )
 
    except HTTPException:
        raise  # Re-raise HTTP exceptions as is
    except Exception as e:
        logger.exception(f"Unexpected error while fetching outline metadata for '{outline_filename}': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An internal server error occurred while fetching outline metadata."
        )

@router.get("/styleGuideStatus/{job_id}")
async def get_style_guide_status(job_id: str):
    """Get the current status of a style guide application job"""
    job = JobStore.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return JSONResponse(content=job)

@router.get("/style-guide/{client}")
async def get_style_guide_info_route(client: str = Path(...)):
    """Get style guide information for a client"""
    try:
        # Get style guide info from project_models
        result = get_style_guide_info(client)
        
        if not result.get("exists"):
            raise HTTPException(status_code=404, detail="No style guide found")
        
        # Get rules from the corresponding TXT file
        pdf_filename = result.get("file_name", "")
        if pdf_filename:
            rules_text = get_rules_from_adls(client, pdf_filename)
            result["rules_text"] = rules_text
            result["rules_found"] = bool(rules_text)
        
        return JSONResponse(content=result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_style_guide_info_route: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/style-guide/upload")
async def upload_style_guide_route(
    client: str = Form(...),
    file: UploadFile = File(...),
    replace: bool = Form(True),  # Default to replace existing files
):
    """Upload style guide file for a client and extract rules"""
    try:
        if not file.filename or not file.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail="Only PDF files are allowed")

        file_content = await file.read()
        if len(file_content) > 20 * 1024 * 1024:
            raise HTTPException(
                status_code=400,
                detail="File size too large. Maximum 20MB allowed",
            )

        # Save temporarily for processing
        temp_path = f"/tmp/{file.filename}"
        with open(temp_path, "wb") as f:
            f.write(file_content)

        # Upload to ADLS with checks for existing style guides
        result = upload_style_guide(file.filename, file_content, client, replace)
        if not result.get("success"):
            raise HTTPException(status_code=400, detail=result.get("message"))

        # Process PDF and extract rules
        try:
            rules_text = process_pdf_and_extract_rules(temp_path, file.filename)
            logger.info(f"Extracted rules: {rules_text[:200]}...")  # Log first 200 chars
            
            # Save rules to ADLS as TXT file
            if rules_text:
                if save_rules_to_adls(rules_text, client, file.filename):
                    result["rules_saved"] = True
                    result["rules_text"] = rules_text
                else:
                    result["rules_saved"] = False
                    result["rules_text"] = None
                    logger.error("Failed to save rules to ADLS")
        except Exception as e:
            logger.error(f"Error extracting rules: {e}")
            result["rules_saved"] = False
            result["rules_text"] = None

        # Cleanup temp PDF
        os.remove(temp_path)
        return JSONResponse(content=result)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in upload_style_guide_route: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
