import time
import logging
from openai import AzureOpenAI, RateLimitError, APITimeoutError, APIConnectionError

logger = logging.getLogger(__name__)


def call_llm_with_retry(client, max_retries=3, **kwargs):
    """
    Call Azure OpenAI with automatic retry on transient failures.
    Handles: 429 (rate limit), timeouts, and connection errors.
    Uses exponential backoff: wait 2s, 4s, 8s between retries.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return client.chat.completions.create(**kwargs)
        except RateLimitError as e:
            wait = 2 ** attempt
            logger.warning(f"Rate limited (attempt {attempt}/{max_retries}). Waiting {wait}s...")
            time.sleep(wait)
        except (APITimeoutError, APIConnectionError) as e:
            wait = 2 ** attempt
            logger.warning(f"Transient error (attempt {attempt}/{max_retries}): {e}. Waiting {wait}s...")
            time.sleep(wait)
        except Exception as e:
            logger.error(f"Non-retryable error: {e}")
            raise
    logger.error(f"All {max_retries} attempts failed.")
    raise Exception(f"LLM call failed after {max_retries} retries")
