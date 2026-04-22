import os
import sys
import json
from openai import AzureOpenAI
from core.llm_utils import call_llm_with_retry
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()


# ==============================================================================
# 1. JSON STORYBOARD PARSING
# ==============================================================================

def parse_storyboards_from_json(file_path: str) -> list[dict]:
    """Reads a JSON file and returns a list of storyboard dictionaries."""
    if not os.path.exists(file_path):
        print(f"FATAL ERROR: Input JSON file not found at '{file_path}'")
        return []

    print(f"Parsing storyboards from JSON: '{file_path}'...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            storyboards = json.load(f)
        print(f"Successfully parsed {len(storyboards)} storyboards.")
        return storyboards
    except json.JSONDecodeError as e:
        print(f"FATAL ERROR: Invalid JSON format - {e}")
        return []


# ==============================================================================
# 2. AI COMPLIANCE CHECK & FIX LOGIC
# ==============================================================================

def create_nasba_compliance_prompt(storyboard: dict) -> str:
    """Creates a detailed prompt for the AI to check a storyboard against NASBA standards."""
    learning_objective = storyboard.get("Learning_Objectives", "Not Provided")
    on_screen_text = storyboard.get("On_screen_text", "Not Provided")
    narration = storyboard.get("Narration", "Not Provided")

    prompt = f"""
You are a meticulous instructional design quality assurance (QA) analyst specializing in NASBA standards. Your task is to analyze a single e-learning slide's storyboard content and evaluate its compliance.

**VERB TAXONOMY (for Measurability Check):**
- **Recommended Measurable Verbs:** Identify, list, apply, calculate, demonstrate, analyze, compare, evaluate, create, solve, explain.
- **Verbs to Avoid (Not Measurable):** Understand, know, learn, be aware of, appreciate, become familiar with.

**CONTENT FOR ANALYSIS:**
1.  **Learning Objective:** "{learning_objective}"
2.  **On-screen Text:** "{on_screen_text}"
3.  **Narration Script:** "{narration}"

**COMPLIANCE CHECKS TO PERFORM:**
1.  **Measurable Objective:** Does the objective use a measurable verb from the recommended list?
2.  **Content-Objective Alignment:** Is the objective fully addressed by the content?
3.  **No Extraneous Content:** Does all content directly support the objective?
4.  **Instructional Elements:** Does the content present concepts, provide guidance (examples), and include a summary/review?

**OUTPUT INSTRUCTIONS:**
Your output **MUST** be a single, raw JSON object with `is_compliant` (boolean), `summary` (string), and `findings` (a list of objects with `check_name`, `status`, and `details`).
"""
    return prompt


def check_storyboard_compliance(client: AzureOpenAI, deployment_name: str, storyboard: dict) -> dict:
    """Sends a single storyboard to the AI for a NASBA compliance check."""
    instructions = create_nasba_compliance_prompt(storyboard)
    try:
        response = call_llm_with_retry(
            client, model=deployment_name, messages=[{"role": "user", "content": instructions}],
            temperature=0.1, response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        return {"is_compliant": False, "summary": "Error during check.",
                "findings": [{"check_name": "System Error", "status": "FAIL", "details": str(e)}]}


def create_storyboard_revision_prompt(storyboard: dict, compliance_report: dict) -> str:
    """Creates a prompt for the AI to revise a storyboard based on compliance findings."""
    original_objective = storyboard.get("Learning_Objectives", "Not Provided")
    original_text = storyboard.get("On_screen_text", "Not Provided")
    original_narration = storyboard.get("Narration", "Not Provided")
    findings_summary = "\n".join(
        f"- {f['check_name']} ({f['status']}): {f['details']}" for f in compliance_report.get('findings', []))

    # --- MODIFIED PROMPT ---
    return f"""
You are a senior instructional design editor. Your task is to revise the storyboard content below to fix the specific compliance issues identified in the report.

**ORIGINAL CONTENT TO BE REVISED:**
- Learning Objective: {original_objective}
- On-screen Text: {original_text}
- Narration: {original_narration}

**COMPLIANCE ISSUES TO FIX:**
{findings_summary}

**REVISION INSTRUCTIONS:**
1. Carefully review the "ISSUES TO FIX" and address every "FAIL" or "WARN" finding in your revision.
2. Change only what is absolutely necessary to achieve compliance.
3. Maintain the core topic, meaning, and original tone of the content.

**CRITICAL OUTPUT FORMATTING RULES:**
- Your output MUST be a JSON object containing **only the fields you have revised**.
- The values for `Learning_Objectives`, `On_screen_text`, and `Narration` in your output JSON **MUST BE SINGLE STRINGS**.
- If you revise content for an interactive slide (e.g., with tabs or accordions), you MUST combine all parts into a single string using newline characters (`\\n`) for separation. Do NOT create a nested JSON object.

**Example of a valid revision output:**
{{
  "Learning_Objectives": "Revised objective as a single string.",
  "On_screen_text": "Revised on-screen text as a single string, with newlines if needed."
}}
"""

def revise_storyboard_content(client: AzureOpenAI, deployment_name: str, storyboard: dict, report: dict) -> dict | None:
    """Sends the non-compliant storyboard and its report to the AI for revision."""
    instructions = create_storyboard_revision_prompt(storyboard, report)
    try:
        response = call_llm_with_retry(
            client, model=deployment_name, messages=[{"role": "user", "content": instructions}],
            temperature=0.4, response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"      - FAILED: An error occurred during content revision: {e}")
        return None


# ==============================================================================
# 3. ORCHESTRATION AND REPORTING
# ==============================================================================

def check_and_remediate_storyboards(storyboards: list[dict], client: AzureOpenAI, deployment_name: str) -> list[dict]:
    """Orchestrates the check-and-fix workflow for all storyboards."""
    print("\n--- Running NASBA Compliance Check & Remediation ---")

    for i, storyboard in enumerate(storyboards):
        slide_title = storyboard.get('Topic', f'Untitled Slide {i + 1}')
        print(f"\n- Processing slide {i + 1}/{len(storyboards)}: '{slide_title}'...")

        # Step 1: Initial Check
        print("  - Step 1: Checking for compliance...")
        report = check_storyboard_compliance(client, deployment_name, storyboard)

        # Step 2: Decide and Fix if needed
        if report.get('is_compliant'):
            print("  - Status: COMPLIANT.")
            storyboard['remediation_status'] = "Compliant"
            storyboard['compliance_report'] = report
        else:
            print("  - Status: NON-COMPLIANT. Attempting remediation...")

            # Step 2a: Attempt to revise the content
            revised_content = revise_storyboard_content(client, deployment_name, storyboard, report)

            if revised_content:
                # Update the storyboard with the fixes
                storyboard.update(revised_content)
                print("  - Step 2b: Revisions received. Verifying the fix...")

                # Step 2c: Verify the fix by re-running the check
                final_report = check_storyboard_compliance(client, deployment_name, storyboard)
                storyboard['compliance_report'] = final_report
                if final_report.get('is_compliant'):
                    print("  - Status: REMEDIATED successfully.")
                    storyboard['remediation_status'] = "Remediated"
                else:
                    print("  - Status: REMEDIATION ATTEMPTED, but still non-compliant.")
                    storyboard['remediation_status'] = "Verification Failed"
            else:
                print("  - Status: REMEDIATION FAILED (AI could not generate a fix).")
                storyboard['remediation_status'] = "Remediation Failed"
                storyboard['compliance_report'] = report  # Keep original failing report

    print("\n--- All slides processed. ---")
    return storyboards


def save_compliance_report(results: list[dict], filename: str):
    """Saves the detailed compliance and remediation findings to a text/markdown file."""
    print(f"\nSaving detailed compliance report to '{filename}'...")
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("NASBA Compliance and Remediation Report\n=========================================\n\n")

        for i, storyboard in enumerate(results):
            slide_title = storyboard.get('Topic', f'Untitled Slide {i + 1}')
            status = storyboard.get('remediation_status', 'Unknown')
            report = storyboard.get('compliance_report', {})

            f.write(f"--- Slide {i + 1}: {slide_title} ---\n")
            f.write(f"Final Status: **{status}**\n")
            f.write(f"AI Summary: {report.get('summary', 'N/A')}\n\n")

            for finding in report.get('findings', []):
                f.write(f"  - Check: {finding.get('check_name', 'N/A')}\n")
                f.write(f"    Status: {finding.get('status', 'N/A')}\n")
                f.write(f"    Details: {finding.get('details', 'N/A')}\n\n")
            f.write("\n")
    print(f"... Report saved successfully.")


def save_remediated_storyboards_to_json(storyboards: list[dict], filename: str):
    """Saves the final, potentially remediated, storyboards to a JSON file."""
    print(f"\nSaving final, remediated storyboards to '{filename}'...")
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(storyboards, f, indent=2, ensure_ascii=False)
        print("... Remediated JSON saved successfully.")
    except Exception as e:
        print(f"ERROR: Could not save remediated storyboards: {e}")


# ==============================================================================
# 4. MAIN EXECUTION BLOCK
# ==============================================================================

def main(file_to_check: str):
    """Main function to run the entire check-and-fix process."""
    api_key = os.getenv("AZURE_OPENAI_API_KEY")
    azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    api_version = os.getenv("AZURE_OPENAI_API_VERSION")
    deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")

    if not all([api_key, azure_endpoint, api_version, deployment_name]):
        raise RuntimeError("Azure OpenAI credentials missing. Check .env file.")

    client = AzureOpenAI(api_key=api_key, azure_endpoint=azure_endpoint, api_version=api_version)

    # Step 1: Parse the source JSON file
    storyboards = parse_storyboards_from_json(file_to_check)
    if not storyboards:
        print("Processing stopped as no storyboards could be parsed.")
        return

    # Step 2: Run the check-and-remediate workflow
    remediated_results = check_and_remediate_storyboards(storyboards, client, deployment_name)

    # Step 3: Save the outputs
    base_name = os.path.splitext(os.path.basename(file_to_check))[0]
    report_filename = f"{base_name}_compliance_report.md"
    remediated_json_filename = f"{base_name}_REMEDIATED.json"

    save_compliance_report(remediated_results, report_filename)
    save_remediated_storyboards_to_json(remediated_results, remediated_json_filename)


if __name__ == "__main__":
    storyboard_file_to_check = 'UTH_UP_FullCourse3_Storyboard.json'

    try:
        main(storyboard_file_to_check)
        print("\n✅✅✅ JSON COMPLIANCE WORKFLOW COMPLETE ✅✅✅")
    except Exception as e:
        print(f"\nAn unexpected error occurred during the main execution: {e}")
