# /app/routes/outline_routes.py
import httpx
import json
import os
import logging
import io
import re
from typing import Optional, List
from fastapi import APIRouter, Body, Form, File, UploadFile, HTTPException, Query ,BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
import traceback
# Local imports
from app.models.outline_models import OutlineRequest, OutlineItem, FullOutline, DownloadRequest, SaveRequest
from app.services.outline_service import OutlineService
# Add the missing imports from core/save_adls.py
from core.save_adls import save_outline_to_adls, get_client_short_name
from app.models.filestructure import get_latest_outline_stream
from docx import Document
from docx.enum.table import WD_ALIGN_VERTICAL
from docx.enum.text import WD_ALIGN_PARAGRAPH
from app.services.job_store import JobStore
# Configure logger for this module
logger = logging.getLogger(__name__)
 
router = APIRouter()
 
 
import re
from langdetect import detect, DetectorFactory
 
DetectorFactory.seed = 0  # For consistent results
 
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
 
ALLOWED_EXTENSIONS = {
    # Documents
    '.doc', '.docx', '.pdf',
    # Spreadsheets
    '.xls', '.xlsx', '.csv',
    # Presentations
    '.ppt', '.pptx',
    # Audio
    '.mp3', '.wav', '.m4a', '.aac', '.flac', '.ogg',
    # Video
    '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv'
}
 
 
def is_supported_file(filename: str) -> bool:
    """Check if a file has an allowed extension."""
    ext = os.path.splitext(filename.lower())[1]
    return ext in ALLOWED_EXTENSIONS
 
# ===== MODELS =====
# Models are now imported from separate files
 
# ===== UTILITY FUNCTIONS =====
# Moved to OutlineService
 
# ===== FILE STRUCTURE ENDPOINTS =====
@router.get("/getclients")
def get_client_list():
    try:
        return JSONResponse(content=OutlineService.get_clients())
    except Exception as e:
        logging.error(f"Failed to get client list: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while retrieving the client list.")
 
@router.get("/getprojects")
def get_project_list(client: str):
    try:
        return JSONResponse(content=OutlineService.get_projects(client))
    except Exception as e:
        logging.error(f"Failed to get project list for client '{client}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while retrieving the project list.")
 
@router.get("/getmodules")
def get_module_list(client: str, project: str):
    try:
        return JSONResponse(content=OutlineService.get_modules(client, project))
    except Exception as e:
        logging.error(f"Failed to get module list for '{client}/{project}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An internal error occurred while retrieving the module list.")
 
@router.get("/getfiles")
async def get_file_list(
    client: str,
    project: str,
    module: Optional[str] = None
):
    """Get files for a client/project/module combination"""
    try:
        files = OutlineService.get_files(client, project, module)
        return JSONResponse(content=files)
       
    except Exception as e:
        logging.error(f"Failed to get files for {client}/{project}/{module}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get files: {str(e)}"
        )
 
# ===== UPDARED OUTLINE GENERATION ENDPOINTS =====
 
@router.post("/SubmitOutline")
async def generate_outline(
    input_data: OutlineRequest = Body(...),
    background_tasks: BackgroundTasks = None
):
    try:
 
        # ✅ 1. Check for gibberish context_prompt (immediate validation)
        if input_data.context_prompt and is_gibberish(input_data.context_prompt):
            return JSONResponse(
                content={
                    "status": "error",
                    "message": "The context prompt appears to contain gibberish or meaningless text. Please provide a clearer context."
                },
                status_code=400
            )
       
        # # if len(input_data.topics) != 0:
        #     input_data.topics = input_data.topics[0].split(",")
        #     print("input_data.topics {input_data.topics}")
 
       
        # ✅ 2. Check for gibberish topics (immediate validation)
        # gibberish_topics = [
        #     topic for topic in input_data.topics if is_gibberish(topic)
        # ]
        # if gibberish_topics:
        #     msg = (
        #         f"The following topic(s) appear to be gibberish: {', '.join(gibberish_topics)}. "
        #         "Please provide meaningful topic names."
        #     )
        #     return JSONResponse(
        #         content={"status": "error", "message": msg},
        #         status_code=400
        #     )
 
        # ✅ 3. Validate file types (immediate validation)
        unsupported_files = [
            file for file in input_data.files if not is_supported_file(file)
        ]
 
        if unsupported_files:
            msg = (
                f"The following file(s) are not supported: {', '.join(unsupported_files)}. "
                "Please upload only PPT, PPTX, DOC, DOCX, PDF, Excel, CSV, Audio, or Video files."
            )
            return JSONResponse(
                content={"status": "error", "message": msg},
                status_code=400
            )
 
        # ✅ 4. Create background job and return job_id
        job_id = JobStore.create_job()
        background_tasks.add_task(run_outline_job, job_id, input_data)
        return {"job_id": job_id, "status": "processing"}
 
    except Exception as e:
        logger.error(f"Failed during outline generation: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed during outline generation: {e}"
        )
 
 
# In outline_routes.py, in the run_outline_job function:
def run_outline_job(job_id: str, input_data: OutlineRequest):
    """Background worker for outline generation with comprehensive error handling"""
    try:
        outline = OutlineService.generate_outline(input_data)
 
        print("Generated outline from outline routes:", outline)
       
        # ✅ Handle empty outline
        if not outline:
            JobStore.set_error(
                job_id,
                "File has been processed incorrectly, failed to generate outline."
            )
            return
           
        # ✅ SIMPLER ERROR DETECTION: Check if first item has 'error' field
        if outline and len(outline) > 0:
            first_item = outline[0]
            
            # Check multiple ways the error could be represented
            error_message = None
            
            if "error" in first_item and first_item["error"]:
                error_message = first_item["error"]
            elif first_item.get("Topic") == "Error":
                error_message = first_item.get("content") or first_item.get("Content_without_images", "Error occurred")
            elif "No search results found" in str(first_item):
                error_message = str(first_item)
            
            if error_message:
                print(f"Outline contains error: {error_message}")
                JobStore.set_error(job_id, error_message)
                return
           
        # ✅ Success case
        JobStore.set_result(job_id, outline)
       
    except Exception as e:
        logger.error(f"Failed during outline generation in background job: {e}", exc_info=True)
        JobStore.set_error(
            job_id,
            f"An internal server error occurred during outline search.{e}"
        )
 
        
@router.get("/outlineStatus/{job_id}")
async def get_outline_status(job_id: str):
    """Check the status of an outline generation job"""
    job = JobStore.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
 
    if job["status"] == "processing":
        return {"status": "processing"}
    elif job["status"] == "done":
        return {"status": "done", "outline": job["result"]}
    else:
        error_message = job.get("error") or "Failed to generate outline"
 
        return JSONResponse(
            content={
                "status": "error",
                "message": error_message
            },
            status_code=400
        )
 
 
 
 
# @router.post("/SubmitOutline")
# def generate_outline(
#     input_data: OutlineRequest = Body(...),
#     background_tasks: BackgroundTasks = None
# ):
#     try:
#         # ✅ 1. Check for gibberish context_prompt
#         if input_data.context_prompt and is_gibberish(input_data.context_prompt):
#             return JSONResponse(
#                 content={
#                     "status": "error",
#                     "message": "The context prompt appears to contain gibberish or meaningless text. Please provide a clearer context."
#                 },
#                 status_code=400
#             )
 
#         # ✅ 2. Validate file types
#         unsupported_files = [
#             file for file in input_data.files if not is_supported_file(file)
#         ]
 
#         if unsupported_files:
#             msg = (
#                 f"The following file(s) are not supported: {', '.join(unsupported_files)}. "
#                 "Please upload only PPT, PPTX, DOC, DOCX, PDF, Excel, CSV, Audio, or Video files."
#             )
#             return JSONResponse(
#                 content={"status": "error", "message": msg},
#                 status_code=400
#             )
 
#         # ✅ 3. Create background job and return job_id
#         job_id = JobStore.create_job()
#         background_tasks.add_task(run_outline_job, job_id, input_data)
#         return {"job_id": job_id}
 
#     except Exception as e:
#         logger.error(f"Failed during outline generation: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=500,
#             detail="An internal server error occurred during outline generation."
#         )
 
 
# def run_outline_job(job_id, input_data):
#     """Background worker for outline generation"""
#     try:
#         outline = OutlineService.generate_outline(input_data)
#         if not outline:
#             JobStore.set_error(
#                 job_id,
#                 "Failed to generate outline, as file was not processed correctly."
#             )
#         else:
#             JobStore.set_result(job_id, outline)
#     except Exception as e:
#         JobStore.set_error(job_id, str(e))
 
 
# @router.get("/outlineStatus/{job_id}")
# def get_outline_status(job_id: str):
#     """Check the status of an outline generation job"""
#     job = JobStore.get_job(job_id)
#     if not job:
#         raise HTTPException(status_code=404, detail="Job not found")
 
#     if job["status"] == "processing":
#         return {"status": "processing"}
#     elif job["status"] == "done":
#         return {"status": "done", "outline": job["result"]}
#     else:
#         return {"status": "error", "error": job["error"]}
 
 
 
 
@router.get("/getOutline")
def get_sample_outline_route():
    return JSONResponse(content={"message": "Please use POST /SubmitOutline instead"})
 
 
# ===== UPDATED OUTLINE SAVE/DOWNLOAD ENDPOINTS =====
 
@router.post("/saveOutline")
def save_outline(
    full_outline: FullOutline = Body(...),
    background_tasks: BackgroundTasks = None
):
    try:
        full_outline_dict = full_outline.dict()
 
        client = full_outline.client
        project = full_outline.project
        module = full_outline.module
 
        # ✅ Build filename dynamically
        if str(module) == "project-level":
            full_outline_dict["module"] = ""
            client_short_name = get_client_short_name(client)
            filename = f"{client_short_name}_{project}_CO.docx"
        else:
            client_short_name = get_client_short_name(client)
            module_name = module.replace(" ", "")
            filename = f"{client_short_name}_{module_name}_CO.docx"
 
        # ✅ Create background job
        job_id = JobStore.create_job()
        background_tasks.add_task(run_save_outline_job, job_id, full_outline_dict, filename)
 
        return {"job_id": job_id}
 
    except Exception as e:
        logger.error(f"Failed to initialize outline save job: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An error occurred while scheduling the outline save operation."
        )
 
 
def run_save_outline_job(job_id, full_outline_dict, filename):
    """Background worker to save outline to ADLS"""
    try:
        save_outline_to_adls(full_outline_dict, filename)
        JobStore.set_result(
            job_id,
            {"status": "success", "message": f"Outline saved to ADLS as {filename}"}
        )
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(f"Error saving outline: {error_trace}")
        JobStore.set_error(job_id, f"Failed to save outline: {str(e)}")
 
 
@router.get("/saveOutlineStatus/{job_id}")
def get_save_outline_status(job_id: str):
    """Check the status of the outline save job"""
    job = JobStore.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
 
    if job["status"] == "processing":
        return {"status": "processing"}
    elif job["status"] == "done":
        return {"status": "done", "result": job["result"]}
    else:
        return {"status": "error", "error": job["error"]}
 
 
@router.post("/downloadOutline")
def download_outline(request: DownloadRequest):
    """
    Download the latest outline file from Azure Data Lake Storage.
    Handles both project-level and module-level outlines.
    """
    try:
        logger.info(f"Received download request: client={request.client}, project={request.project}, module={request.module}")
 
        # Determine filename and module path
        client_short_name = get_client_short_name(request.client)
        if str(request.module) == "project-level":
            filename = f"{client_short_name}_{request.project}_CO.docx"
            module_path = ""  # No module in path for project-level
        else:
            module_name = request.module.replace(" ", "")
            filename = f"{client_short_name}_{module_name}_CO.docx"
            module_path = request.module
 
        # Fetch the file stream from ADLS
        from app.models.filestructure import download_file_from_adls
        file_content = download_file_from_adls(
            request.client, request.project, module_path, filename
        )
        if not file_content:
            raise HTTPException(status_code=404, detail=f"Outline file '{filename}' not found.")
 
        logger.info(f"Successfully retrieved outline file: {filename}")
 
        return StreamingResponse(
            io.BytesIO(file_content),
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading outline from ADLS: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to download outline: {str(e)}")
 
# ===== OUTLINE FILE MANAGEMENT =====
@router.get("/getoutlinelist", response_model=list[str])
async def get_outline_list_from_adls(
    client: str = Query(...),
    project: str = Query(...),
    module: str = Query(...)
):
    """
    API endpoint to list all available outline files directly from ADLS
    for a specific client, project, and module.
    """
    try:
        outlines = OutlineService.get_outline_list(client, project, module)
        return outlines
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while fetching the outline list from ADLS."
        )  
 
# ===== SOURCE FILE MANAGEMENT =====
LOGIC_APP_URL = (
    "https://prod-20.northcentralus.logic.azure.com:443/workflows/"
    "eff4bd5b04d647069a733c36911c82c1/triggers/When_an_HTTP_request_is_received/"
    "paths/invoke?api-version=2016-10-01"
    "&sp=%2Ftriggers%2FWhen_an_HTTP_request_is_received%2Frun"
    "&sv=1.0"
    "&sig=Tjt4d6bUxLFxNynTa2vhmA-KxFfvncHMBcmHFMbuOVk"
)
 
@router.post("/uploadSourceFiles")
async def upload_source_files(
    files: List[UploadFile] = File(...),
    client: str = Form(...),
    project: str = Form(...),
    module: Optional[str] = Form(None)
):
    """
    Uploads source files to ADLS, constructs normalized /aptara/... paths,
    and triggers the Logic App.
    """
    try:
 
        unsupported_files = [
            file.filename for file in files if not is_supported_file(file.filename)
        ]
 
        if unsupported_files:
            msg = (
                f"The following file(s) are not supported: {', '.join(unsupported_files)}. "  # FIX: Now joining strings
                "Please upload only PPT, PPTX, DOC, DOCX, PDF, Excel, CSV, Audio, or Video files."
            )
            return JSONResponse(
                content={"status": "error", "message": msg},
                status_code=400
            )
 
        # === 1️⃣ Read uploaded files
        files_data = []
        for file in files:
            file_bytes = await file.read()
            files_data.append((file.filename, file_bytes))
 
        # === 2️⃣ Upload to ADLS (actual storage)
        success, message, uploaded_files, _ = OutlineService.upload_source_files(
            files_data, client, project, module
        )
 
        if not success:
            raise HTTPException(status_code=500, detail=message)
 
        logging.info(f"✅ Uploaded {len(uploaded_files)} file(s) for client={client}, project={project}, module={module}")
 
        # === 3️⃣ Construct logic paths (without ADLS URI)
        uploaded_paths = []
        for filename in uploaded_files:
            if module and module.strip():
                # Module-level: /aptara/client/project/module/Source/filename
                path = f"/aptara/{client}/{project}/{module}/Source/{filename}"
            else:
                # Project-level: /aptara/client/project/filename
                path = f"/aptara/{client}/{project}/{filename}"
            uploaded_paths.append(path)
 
        logging.info(f"🧩 Constructed file paths for Logic App: {uploaded_paths}")
 
        # === 4️⃣ Trigger Logic App
        logic_app_response = None
        if uploaded_paths:
            payload = {"file_paths": uploaded_paths}
            logging.info(f"🚀 Triggering Logic App with payload: {payload}")
 
            try:
                async with httpx.AsyncClient(timeout=60.0) as http_client:
                    response = await http_client.post(LOGIC_APP_URL, json=payload)
 
                if response.status_code in (200, 202):
                    logic_app_response = response.text or "Triggered successfully"
                    logging.info(f"✅ Logic App triggered successfully. Response: {logic_app_response}")
                else:
                    logic_app_response = f"Logic App trigger failed: {response.status_code} - {response.text}"
                    logging.warning(logic_app_response)
 
            except Exception as e:
                logic_app_response = f"Error calling Logic App: {str(e)}"
                logging.error(logic_app_response, exc_info=True)
        else:
            logic_app_response = "No uploaded file paths found."
 
        # === 5️⃣ Final Response
        return JSONResponse(content={
            "status": "success",
            "message": message,
            "uploaded_files": uploaded_files,
            "uploaded_paths": uploaded_paths,
            "logic_app_response": logic_app_response
        })
 
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"❌ Error in upload_source_files: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
 