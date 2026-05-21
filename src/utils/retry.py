"""
Retry decorator and utilities for handling transient failures
"""
import time
import functools
from typing import Callable, Type, Tuple, Optional
from src.utils.logger import get_logger

logger = get_logger(__name__)


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable] = None
):
    """
    Retry decorator with exponential backoff

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exception types to catch and retry
        on_retry: Optional callback function called on each retry

    Example:
        @retry(max_attempts=3, delay=2, backoff=2, exceptions=(ConnectionError,))
        def fetch_data():
            # code that might fail
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_attempts:
                        logger.error(
                            f"Function {func.__name__} failed after {max_attempts} attempts. "
                            f"Last error: {str(e)}"
                        )
                        raise

                    logger.warning(
                        f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {str(e)}. "
                        f"Retrying in {current_delay}s..."
                    )

                    if on_retry:
                        on_retry(attempt, e)

                    time.sleep(current_delay)
                    current_delay *= backoff

            # Should never reach here, but just in case
            if last_exception:
                raise last_exception

        return wrapper
    return decorator


def retry_on_failure(func: Callable, max_attempts: int = 3, delay: float = 1.0) -> Callable:
    """
    Simple retry wrapper function

    Args:
        func: Function to retry
        max_attempts: Maximum retry attempts
        delay: Delay between retries

    Returns:
        Wrapped function with retry logic
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for attempt in range(1, max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_attempts:
                    logger.error(f"All {max_attempts} attempts failed: {str(e)}")
                    raise
                logger.warning(f"Attempt {attempt} failed, retrying... Error: {str(e)}")
                time.sleep(delay)
    return wrapper
