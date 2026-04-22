import os
import sys
import json
from openai import AzureOpenAI
from core.llm_utils import call_llm_with_retry
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

# --- Load Environment Variables ---
load_dotenv()

# --- CONFIGURATION: Screen Type Structures ---
SCREEN_TYPE_STRUCTURES = {
    "Static Screen": "A single, coherent block of text. The narration should be a direct, professional reading or summary of this text.",
    " Animated Screen": "A coherent block of text. The narration should be a direct, professional reading .",
    "Tabs Screen": """
        - On-screen text: The first line MUST be 'Click each section to explore.'. The rest of the text must be formatted as 'Tab Title 1\\nContent for Tab 1\\n\\nTab Title 2\\nContent for Tab 2...'.
        - Narration: Must be structured with an introduction for the whole slide, followed by separate narrations for each tab, formatted exactly like this:
          'This slide breaks down the key features. Click each tab to learn more.
          - [Narration for when user clicks Tab 1]...
          - [Narration for when user clicks Tab 2]...'
    """,
    "Accordion Screen": """
        - On-screen text: The first line MUST be 'Click each item to learn more.'. The rest of the text must be formatted as 'Accordion Header 1\\nContent for Header 1\\n\\nAccordion Header 2\\nContent for Header 2...'.
        - Narration: Must be structured with an introduction, followed by separate narrations for each accordion item, formatted exactly like this:
          'This slide covers the main safety protocols. Click each item to expand.
          - [Narration for when user clicks Accordion 1]...
          - [Narration for when user clicks Accordion 2]...'
    """,
    "Carousel Screen": """
        - On-screen text: The first line MUST be 'Click the Continue and the Previous buttons to learn more.'. The rest of the text must be formatted as 'Panel 1: Title\\nContent for Panel 1\\n\\nPanel 2: Title\\nContent for Panel 2...'.
        - Narration: Must be structured with an introduction, followed by separate narrations for each panel, formatted exactly like this:
          'Let's walk through the process step-by-step. Use the arrows to navigate.
          - [Narration for Panel 1]...
          - [Narration for Panel 2]...'
    """,
    "Flipcard Screen": """
        - On-screen text: Text must be formatted as 'Front: [Text for front]\\n\\nBack: [Text for back]'.
        - Narration: Must be structured with an introduction and separate narrations for the front and back, formatted exactly like this:
          'Test your knowledge with these cards.
          - [Narration for the front of the card]...
          - [Narration for the back of the card]...'
    """,
    "Interactive Quiz": "This format is for a single question per slide. The On-screen text contains the question, instructions, options, and feedback. The Narration should simply pose the question."
}


# ==============================================================================
# HELPER FUNCTIONS (File I/O, AI Prompts, AI Call)
# These are internal functions used by the main processing function.
# ==============================================================================

def read_storyboards_from_json(file_path: str) -> Optional[List[Dict[str, Any]]]:
    if not os.path.exists(file_path):
        print(f"FATAL ERROR: Input file '{file_path}' not found.")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            storyboards = json.load(f)
        if not isinstance(storyboards, list):
            print(f"FATAL ERROR: JSON file '{file_path}' does not contain a list.")
            return None
        return storyboards
    except Exception as e:
        print(f"An error occurred while reading the JSON file: {e}")
        return None


def save_storyboards_to_json(storyboards: List[Dict[str, Any]], filename: str):
    if not storyboards:
        print("No storyboards to save.")
        return
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(storyboards, f, indent=4, ensure_ascii=False)
        print(f"Successfully saved to '{filename}'.")
    except Exception as e:
        print(f"An error occurred while saving the JSON file: {e}")


def create_editing_prompt(slide_data: Dict[str, Any], user_request: str, context_slides: List[Dict[str, Any]]) -> str:
    context_prompt_section = ""
    if context_slides:
        context_parts = ["--- PREVIOUS SLIDE CONTEXT (FOR CONTINUITY) ---"]
        for i, slide in enumerate(context_slides):
            slide_num = len(context_slides) - i
            context_parts.append(f"\n**Slide (N-{slide_num}): \"{slide.get('Topic', 'Untitled')}\"**")
            context_parts.append(f"Narration: {slide.get('Narration', 'N/A')}")
        context_prompt_section = "\n".join(context_parts)

    screen_type_context = "\n".join(
        [f"- **{name}:** {description}" for name, description in SCREEN_TYPE_STRUCTURES.items()])
    original_data_json = json.dumps(slide_data, indent=2)

    prompt = f"""
You are an expert instructional design editor. Your task is to revise the following JSON data for a single e-learning slide based on a user's request.

{context_prompt_section}

**USER'S EDIT REQUEST (Apply to the 'ORIGINAL SLIDE DATA' below):**
{user_request}

**SCREEN TYPE CONTEXT & RULES:**
If the user's request involves changing the screen type, you MUST:
1. Update the "Screen_type" key.
2. COMPLETELY REFORMAT the "On_screen_text" and "Narration" to match the required structure for the new screen type, repurposing the original content.
3. Update the "Developer_notes  to also reflect the new screen type (e.g., "Develop an interactive tabs screen...").
Here are the structures:
{screen_type_context}

**CRITICAL INSTRUCTIONS:**
1.  **Use Context:** Ensure your edit creates a smooth transition from the previous slides.
2.  **Preserve Keys:** Return the complete JSON object with all original keys present.
3.  **Preserve Data:** The "Source_Images_(base64)" field is critical. DO NOT modify it.
4.  **Return Full Object:** Your output MUST be a single, raw JSON object.

**ORIGINAL SLIDE DATA (The slide to be edited):**
{original_data_json}
"""
    return prompt


def _apply_edits_with_ai_single_slide(client: AzureOpenAI, deployment_name: str, slide_data: Dict[str, Any],
                                      user_request: str, context_slides: List[Dict[str, Any]]) -> Optional[
    Dict[str, Any]]:
    slide_data_for_ai = slide_data.copy()
    preserved_images = slide_data_for_ai.pop('Source_Images_(base64)', None)

    instructions = create_editing_prompt(slide_data_for_ai, user_request, context_slides)

    try:
        response = call_llm_with_retry(
            client,
            model=deployment_name,
            messages=[
                {"role": "system",
                 "content": "You are an e-learning editor that revises slide content based on user requests, understands context from previous slides, and outputs a complete, valid JSON object."},
                {"role": "user", "content": instructions}
            ],
            temperature=0.5,
            response_format={"type": "json_object"}
        )

        edited_slide_data = json.loads(response.choices[0].message.content)

        if preserved_images is not None:
            edited_slide_data['Source_Images_(base64)'] = preserved_images

        return edited_slide_data
    except Exception as e:
        print(
            f"      - FAILED: An error occurred during AI editing for slide '{slide_data.get('Topic', 'Untitled')}': {e}")
        return None


# ==============================================================================
# MAIN PROCESSING FUNCTION (FOR FRONTEND INTEGRATION)
# ==============================================================================

def process_storyboard_edits(
        file_path: str,
        user_request: str,
        mode: str,
        slide_number: Optional[int],
        client: AzureOpenAI,
        deployment_name: str
) -> Optional[List[Dict[str, Any]]]:
    """
    The main engine for processing storyboard edits. This function is designed to be
    called from a backend server or other application. It does not use `input()`.

    Args:
        file_path: The full path to the input storyboard JSON file.
        user_request: The user's instruction for the edit (e.g., "Make narration more formal").
        mode: The mode of operation, either "one" or "all".
        slide_number: The 1-based slide number to edit (required if mode is "one").
        client: An initialized AzureOpenAI client instance.
        deployment_name: The name of the Azure OpenAI deployment.

    Returns:
        A list of dictionary objects representing the updated storyboards,
        or None if a fatal error occurs (e.g., file not found).
    """
    print(f"--- Starting storyboard processing for '{file_path}' ---")
    print(f"Request: '{user_request}' | Mode: {mode}")

    storyboards = read_storyboards_from_json(file_path)
    if not storyboards:
        return None

    total_slides = len(storyboards)

    if mode == 'one':
        if slide_number is None or not (1 <= slide_number <= total_slides):
            print(f"Error: Invalid slide number '{slide_number}'. Must be between 1 and {total_slides}.")
            return None  # Return None on validation failure

        slide_index = slide_number - 1
        slide_to_edit = storyboards[slide_index]
        context_slides = storyboards[max(0, slide_index - 2): slide_index]

        print(f"Editing Slide {slide_number}: '{slide_to_edit.get('Topic', 'Untitled')}'...")
        edited_slide = _apply_edits_with_ai_single_slide(client, deployment_name, slide_to_edit, user_request,
                                                         context_slides)

        if edited_slide:
            storyboards[slide_index] = edited_slide
            print("✅ Edit successful.")
        else:
            print("❌ Edit failed. Returning original storyboards.")

        return storyboards

    elif mode == 'all':
        print("Applying edits to all slides...")
        updated_storyboards = []
        for i, slide in enumerate(storyboards):
            print(f"  - Processing slide {i + 1}/{total_slides}...")
            context_slides = storyboards[max(0, i - 2): i]
            edited_slide = _apply_edits_with_ai_single_slide(client, deployment_name, slide, user_request,
                                                             context_slides)

            if edited_slide:
                updated_storyboards.append(edited_slide)
            else:
                print(f"    - Skipping slide {i + 1} due to an error. Keeping original content.")
                updated_storyboards.append(slide)  # Keep original on failure

        print("✅ Bulk edit process complete.")
        return updated_storyboards

    else:
        print(f"Error: Invalid mode '{mode}'. Must be 'one' or 'all'.")
        return None


# ==============================================================================
# EXAMPLE USAGE (COMMAND-LINE DRIVER)
# This block demonstrates how to use the process_storyboard_edits function.
# A real frontend would replace this with its own logic.
# ==============================================================================

if __name__ == "__main__":
    # --- 1. Initialize Azure OpenAI Client ---
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_version = os.getenv("AZURE_OPENAI_API_VERSION")
    deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")

    if not all([api_key, azure_endpoint, api_version, deployment_name]):
        raise RuntimeError("Azure OpenAI credentials missing. Check .env file.")

    client = AzureOpenAI(api_key=api_key, azure_endpoint=azure_endpoint, api_version=api_version)

    # --- 2. Gather Inputs from the User (The "Frontend" part) ---
    print("\n--- Storyboard Chatbot Editor (Command-Line Interface) ---")
    file_path_input = input("Enter the path to the storyboard JSON file to edit:\n> ")
    user_request_input = input("Enter your edit instruction (e.g., 'Make narration more formal'):\n> ")

    mode_input = ""
    while mode_input not in ['all', 'one']:
        mode_input = input("Apply this to 'all' slides or just 'one' slide? (all/one):\n> ").lower()

    slide_number_input = None
    if mode_input == 'one':
        slide_num_str = input("Which slide number do you want to edit? (e.g., 5):\n> ")
        if slide_num_str.isdigit():
            slide_number_input = int(slide_num_str)
        else:
            print("Invalid number. Exiting.")
            raise ValueError("Invalid slide number provided.")

    # --- 3. Call the Main Processing Function ---
    updated_storyboard_data = process_storyboard_edits(
        file_path=file_path_input,
        user_request=user_request_input,
        mode=mode_input,
        slide_number=slide_number_input,
        client=client,
        deployment_name=deployment_name
    )

    # --- 4. Handle the Result ---
    if updated_storyboard_data:
        print("\n--- Processing Complete ---")
        save_choice = input("Do you want to save the changes to a new file? (yes/no):\n> ").lower()
        if save_choice == 'yes':
            output_filename = input("Enter the output filename (e.g., 'Storyboard_v2.json'):\n> ")
            if not output_filename.lower().endswith('.json'):
                output_filename += '.json'
            save_storyboards_to_json(updated_storyboard_data, output_filename)
    else:
        print("\n--- Processing Failed ---")