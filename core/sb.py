from typing import Optional
import logging

def generate_storyboard_from_selection(selection_data: Optional[dict] = None) -> list:
    """
    DEPRECATED: This function is replaced by the dynamic orchestrator pipeline.
    Use generate_storyboard_pipeline() from orchestrator.py instead.
    
    This function now redirects to the orchestrator for backward compatibility.
    """
    try:
        if not selection_data:
            logging.warning("No selection data provided to generate_storyboard_from_selection")
            return []
            
        # Import here to avoid circular imports
        from .orchestrator import generate_storyboard_pipeline
        
        logging.info("Redirecting to dynamic storyboard generation pipeline...")
        return generate_storyboard_pipeline(selection_data)
        
    except Exception as e:
        logging.error(f"Error in generate_storyboard_from_selection: {e}", exc_info=True)
        return []
