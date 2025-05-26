# openai_utils.py
# Utilities for interacting with the OpenAI API.

import openai
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

MAX_RETRIES = 3
INITIAL_BACKOFF = 1
MAX_BACKOFF = 30

def get_openai_insights(prompt: str, api_key: str, model: str = "gpt-3.5-turbo") -> str:
    """
    Gets insights from OpenAI based on a given prompt.

    Args:
        prompt (str): The prompt to send to OpenAI.
        api_key (str): The OpenAI API key.
        model (str): The OpenAI model to use (default: "gpt-3.5-turbo").

    Returns:
        str: The insight text from OpenAI.

    Raises:
        ValueError: If the API key is not provided.
        Exception: If the API call fails after retries or due to other errors.
    """
    if not api_key:
        logger.error("OpenAI API key not provided to get_openai_insights.")
        raise ValueError("API key is required for OpenAI calls.")

    openai.api_key = api_key

    current_retry = 0
    backoff_period = INITIAL_BACKOFF

    while current_retry <= MAX_RETRIES:
        try:
            logger.debug(f"Attempt {current_retry + 1}/{MAX_RETRIES + 1}: Sending prompt to OpenAI (model: {model}). "
                        f"Prompt: \"{prompt[:100]}...\"")

            # Using the older OpenAI SDK structure with ChatCompletion
            response = openai.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are an AI assistant that provides concise data insights."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Ensure there are choices, a message, and content in the message
            if response.choices and \
               response.choices[0].message and \
               response.choices[0].message.content:
                insight = response.choices[0].message.content.strip()
                logger.debug(f"Successfully received insight from OpenAI: \"{insight[:100]}...\"")
                return insight
            else:
                logger.error("OpenAI response did not contain expected data (choices, message, or content).")
                # This is considered a non-retryable error for this specific issue.
                raise Exception("Invalid response structure from OpenAI.")

        except openai.APIError as e:  # Handles API errors from OpenAI (e.g., rate limits, server errors)
            logger.warning(f"OpenAI API error on attempt {current_retry + 1}: {e}")

            if current_retry == MAX_RETRIES:
                logger.error("Max retries reached. Failing OpenAI call due to APIError.")
                raise

            # Log specific warnings for common retryable statuses
            if hasattr(e, 'status_code'):
                if e.status_code == 429:  # Rate limit error
                    logger.debug(f"Rate limit hit. Retrying in {backoff_period}s...")
                elif e.status_code >= 500:  # Server-side errors (500, 502, 503, 504, etc.)
                    logger.debug(f"OpenAI server error (HTTP {e.status_code}). Retrying in {backoff_period}s...")
            
            time.sleep(backoff_period)
            backoff_period = min(backoff_period * 2, MAX_BACKOFF)  # Exponential backoff with a cap
            current_retry += 1

        except Exception as e:
            # Catches other unexpected errors during the API call or response processing
            logger.error(f"An unexpected non-API error occurred during OpenAI call on attempt {current_retry + 1}: {e}", exc_info=True)
            # Re-raise the caught exception, chained for better context
            raise Exception(f"Failed to get insights from OpenAI due to an unexpected error: {e}") from e