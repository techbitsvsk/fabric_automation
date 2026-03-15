import requests
from requests.adapters import HTTPAdapter, Retry

def get_retry_session(total: int = 5, backoff_factor: float = 0.5):
    """Create a requests Session with automatic retry logic.

    Retries on transient HTTP errors (429, 500, 502, 503, 504) using
    exponential backoff. Retries are applied to all common HTTP methods.

    Args:
        total: Maximum number of retry attempts per request.
        backoff_factor: Multiplier applied between retry attempts
            (e.g. 0.5 → waits 0s, 0.5s, 1s, 2s...).

    Returns:
        A configured requests.Session with retry adapters mounted for
        both http:// and https://.
    """
    session = requests.Session()
    retries = Retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT", "PATCH", "DELETE"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
