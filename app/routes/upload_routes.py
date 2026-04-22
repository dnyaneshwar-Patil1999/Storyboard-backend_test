from fastapi import APIRouter
from fastapi.responses import JSONResponse
import os

router = APIRouter()

# --------------- List of files ---------------------------
##test
@router.get("/list-files")
def list_blob_files():
    
    files = ["one.pdf","two.pdf"]

    return JSONResponse(content={"files": files})


# --------------- List of modules ---------------------------

@router.get("/list")
def list_modules():
      # Update if using Azure Blob
    module_files = ["module1","module2"]

    return JSONResponse(content={"modules": module_files})
