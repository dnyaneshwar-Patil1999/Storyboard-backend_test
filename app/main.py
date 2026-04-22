from fastapi import FastAPI # type: ignore
from fastapi.middleware.cors import CORSMiddleware # type: ignore
from fastapi.responses import JSONResponse
from app.routes.api import api_router

app = FastAPI(title="GenAI Backend")

# Add CORS middleware with updated configuration
app.add_middleware(
    CORSMiddleware,
    # Update the allow_origins list to include all your frontend origins
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
    expose_headers=["Content-Disposition"]  # Explicitly expose the Content-Disposition header
)

@app.get("/health")
def health_check():
    return JSONResponse(
        status_code=200,
        content={"status": "healthy"}
    )

app.include_router(api_router)
