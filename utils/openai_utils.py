# openai_utils.py
# Utilities for interacting with the OpenAI API.

import openai
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

MAX_RETRIES = 3
INITIAL_BACKOFF = 1

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
        Exception: If the API call fails after retries.
    """
    if not api_key:
        logger.error("OpenAI API key not provided to get_openai_insights.")
        raise ValueError("API key is required for OpenAI calls.")

    # Initialize client here or ensure it's initialized globally
    # For Lambda, it's often better to initialize clients outside the handler
    openai.api_key = api_key

    current_retry = 0
    backoff_period = INITIAL_BACKOFF

    while current_retry <= MAX_RETRIES:
        try:
            logger.info(f"Sending prompt to OpenAI (model: {model}): \"{prompt[:100]}...\"")
            # Using the older OpenAI SDK structure with ChatCompletion
            response = openai.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are an AI assistant that provides concise data insights."},
                    {"role": "user", "content": prompt}
                ],
            )
            
            # Ensure there are choices and a message
            if response.choices and response.choices[0].message:
                insight = response.choices[0].message.content.strip()
                logger.info(f"Received insight from OpenAI: \"{insight[:100]}...\"")
                return insight
            else:
                logger.error("OpenAI response did not contain expected data (choices or message).")
                # This specific error might not be retryable in the same way as a network error
                raise Exception("Invalid response structure from OpenAI.")


        except openai.APIError as e: # Handles API errors, rate limits, etc.
            logger.error(f"OpenAI API error: {e} (Attempt {current_retry + 1}/{MAX_RETRIES + 1})")
            if e.status_code == 429: # Rate limit error
                logger.warning(f"Rate limit hit. Retrying in {backoff_period} seconds...")
            elif e.status_code == 500: # Server error
                 logger.warning(f"OpenAI server error. Retrying in {backoff_period} seconds...")

            if current_retry == MAX_RETRIES:
                logger.error("Max retries reached. Failing OpenAI call.")
                raise
            
            time.sleep(backoff_period)
            backoff_period *= 2  # Exponential backoff
            current_retry += 1

        except Exception as e:
            # Catch any other unexpected errors during the API call
            logger.error(f"An unexpected error occurred during OpenAI call: {e}")
            # For this generic handler, we re-raise to indicate failure.
            raise Exception(f"Failed to get insights from OpenAI: {str(e)}")

    # Should not be reached if MAX_RETRIES is handled correctly, but as a fallback:
    raise Exception("OpenAI call failed after maximum retries.")
