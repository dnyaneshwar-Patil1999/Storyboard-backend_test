# /app/models/storyboard_models.py

from pydantic import BaseModel, Field
from typing import List, Union, Optional

class ComplianceModel(BaseModel):
    nasba: bool
    ukEnglish: bool

class StoryboardRequest(BaseModel):
    client: str
    project: str
    module: str
    outline: str
    blooms_level: str
    context_prompt: Optional[str] = None  # Add this field
    compliance: ComplianceModel

class StoryboardItem(BaseModel):
    Topic: str
    Learning_Objectives: str
    Screen_type: str
    On_screen_text: Union[str, dict]
    Narration: str
    On_screen_Recommendations: Union[str, List[str], None] = None
    Developer_Notes: Optional[str] = None

# class FullStoryboard(BaseModel):
#     """Complete storyboard model"""
#     client: str
#     project: str
#     module: str
#     storyboard: List[StoryboardItem]

class Storyboard(BaseModel):
    Course_Title: str
    Module_Title: str
    Topic: str
    Screen_type: str
    Blooms_Level: str
    Duration_min: str = Field(..., alias="Duration_(min)")
    Source_Images_base64: List[str] = Field(..., alias="Source_Images_(base64)")
    Learning_Objectives: List[str]
    On_screen_text: str
    Narration: str
    On_screen_Recommendations: List[str]
    Developer_Notes: List[str]
    Source_Tables: List[str] = []
 
    class Config:
        populate_by_name = True
        
class FullStoryboard(BaseModel):
    client: str
    project: str
    module: str
    storyboard: List[Storyboard]
 
class ApplyChangesRequest(BaseModel):
    client: str
    project: str
    applyToAll: bool
    prompt: str
    pageNumber: Optional[int] = None

class DownloadRequest(BaseModel):
    client: str
    project: str
    module: str
    
class OutlineMetadataResponse(BaseModel):
    user_prompt: str
    filename: str