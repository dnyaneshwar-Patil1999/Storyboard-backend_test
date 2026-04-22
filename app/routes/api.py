from fastapi import APIRouter
from app.routes import auth_routes, filestructure_routes, outline_routes, storyboard_routes, upload_routes, style_guide_routes

api_router = APIRouter()


api_router.include_router(auth_routes.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(filestructure_routes.router, prefix="/v1/client", tags=["Project Structure & Viewer"])
api_router.include_router(outline_routes.router, prefix="/v1/outline", tags=["Outline Actions"])
api_router.include_router(storyboard_routes.router, prefix="/v1/storyboard", tags=["Storyboard Actions"])
api_router.include_router(upload_routes.router, prefix="/v1/files", tags=["File Management"])
api_router.include_router(style_guide_routes.router, prefix="/v1/storyboard", tags=["Style Guide"])

