# /app/models/outline_models.py

from pydantic import BaseModel, Field
from typing import Optional, List

class OutlineRequest(BaseModel):
    """Request model for outline generation"""
    client: str
    project: str
    module: str
    files: List[str]
    # topics: List[str]
    context_prompt: Optional[str] = None  # Added context prompt support

class OutlineItem(BaseModel):
    File: Optional[str] = None
    Source_Page: Optional[str] = None  # Fixed: Use underscore consistently
    Chapter: Optional[str] = None
    Topic: Optional[str] = None
    Subtopic: Optional[str] = None
    Full_Page_Content: Optional[str] = None  # Fixed: Use underscores consistently
    Durations_min: Optional[str] = None  # Fixed: Use consistent field name

class FullOutline(BaseModel):
    client: str
    project: str
    module: str
    outlines: List[OutlineItem]
    context_prompt: Optional[str] = None  # Added context prompt support
    topic: Optional[str] = None  # Added topic support

class DownloadRequest(BaseModel):
    client: str
    project: str
    module: str

class SaveRequest(BaseModel):
    client: str
    project: str
    module: str
    outlines: List[OutlineItem]
    context_prompt: Optional[str] = None
    # topic: Optional[str] = None

def get_sample_outline() -> dict:
    """Sample outline data for testing"""
    outlines = [{
        "Score": 2.93,
        "Topic": "TRANSITIONING TO THE GREETING GATEWAY",
        "Sub Topics": "TEMPUR FITTING EXPERIENCE, Tempur sleep systems, sleep needs, undisturbed sleep, GREETING GATEWAY, warm greeting, entire journey, TRANSITIONING, THE, point, client, store, conversation, interests, customer, problem",
        "Relevant Snippet": "TEMPUR FITTING EXPERIENCE 13 TRANSITIONING TO THE GREETING GATEWAY At this point, you've offered... **Accompanying Text**:     - Positioned below the visual display is a section titled \"TRANSITIONING TO THE GREETING GATEWAY,\" which provides guidance for store associates on how to engage customers. - **Content of the Paragraph**:         - Explains.",
        "Source_Page": 13,  # Fixed: Use underscore
        "File": "TFX 2025-Process and Scripts for eLearning.pdf",
        "Durations_min": "0.8 min",  # Fixed: Use correct field name
        "images_base64": [],
        "Full_Page_Content": "TEMPUR FITTING EXPERIENCE\n15\nLET'S TRY IT!\nWHY WE USE THE GREETING GATEWAY\nThis merchandising element puts you firmly into the driver's seat of guiding \nyour client on their journey to their deep, undisturbed sleep. Here's how a \nGreeting Gateway presentation sounds:\n\"Our journey today is to try and get you to truly \nfigure out your why. We will solve your sleep problems with a \ncomplete sleep system tailored to your needs. At Tempur-Pedic, \nwe define a sleep system as a base, mattress, pillow and sheets. \nEverything can feel different to you based on how your body's \nshape, weight and heat adjusts and this takes a little time for you \nto feel that difference.  Now we can't find a solution to your sleep \nproblems unless you share with me your why, so we need your \nhelp here: What is your why?\nOwn it, in your own words! Write a few transition statements you can ask \nyour client to make sure you clearly understand their why for looking at \nTempur-Pedic to bring them deep, undisturbed sleep. \nIf a client comes in requesting to try a specific mattress model \non our floor, it's important we acknowledge their request, assure \nthem they'll have that opportunity, and then ask questions to \nhelp you understand their why for considering that specific bed. \nWhat will that solve for them?\nPRO TIP\nBE CONFIDENT:\n85% of Tempur-Pedic® owners agree that they sleep better on their Tempur-Pedic®. \n(Tempur-Pedic Brand owner satisfaction study, 2023)\n"  # Fixed: Use underscores
    }]
    
    return outlines

def process_edited_outline(edited_outline: dict) -> dict:
    """Process edited outline data"""
    # You can write this to DB, log it, or use in LLM
    print("📝 Outline received for editing:")
    print(edited_outline)

    return {
        "message": "Outline updated successfully",
        "data": edited_outline
    }
