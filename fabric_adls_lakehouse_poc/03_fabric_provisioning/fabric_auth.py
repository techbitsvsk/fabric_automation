"""
03_fabric_provisioning/fabric_auth.py
======================================
Authentication helper for Microsoft Fabric REST APIs.

Uses AzureCliCredential — authenticates with your personal Azure account.
Requires: az login  (run once before starting the pipeline)

For production, swap AzureCliCredential for ClientSecretCredential (SPN).
"""

import time
import logging
import yaml
from pathlib import Path
from functools import lru_cache

import requests
from azure.identity import AzureCliCredential
from tenacity import retry, stop_after_attempt, wait_exponential

ROOT = Path(__file__).resolve().parents[1]
with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE    = "https://api.fabric.microsoft.com/.default"

log = logging.getLogger(__name__)


# ─── Credential ───────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _credential():
    """
    AzureCliCredential uses the account from `az login`.
    Tokens are refreshed automatically by the SDK when they expire.
    """
    return AzureCliCredential()


def get_token(scope: str = FABRIC_SCOPE) -> str:
    """Acquire an access token for the given scope."""
    token = _credential().get_token(scope)
    return token.token


def fabric_headers(scope: str = FABRIC_SCOPE) -> dict:
    return {
        "Authorization": f"Bearer {get_token(scope)}",
        "Content-Type":  "application/json",
    }


# ─── HTTP helpers with retry ──────────────────────────────────────────────────

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=2, max=30))
def fabric_get(path: str, scope: str = FABRIC_SCOPE) -> dict:
    url  = f"{FABRIC_API_BASE}/{path.lstrip('/')}"
    resp = requests.get(url, headers=fabric_headers(scope), timeout=60)
    _raise_for_status(resp, "GET", url)
    return resp.json() if resp.content else {}


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=2, max=30))
def fabric_post(path: str, payload: dict, scope: str = FABRIC_SCOPE,
                params: dict = None) -> dict:
    url  = f"{FABRIC_API_BASE}/{path.lstrip('/')}"
    resp = requests.post(url, json=payload, headers=fabric_headers(scope),
                         params=params, timeout=120)
    _raise_for_status(resp, "POST", url)
    return resp.json() if resp.content else {}


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=2, max=30))
def fabric_patch(path: str, payload: dict, scope: str = FABRIC_SCOPE) -> dict:
    url  = f"{FABRIC_API_BASE}/{path.lstrip('/')}"
    resp = requests.patch(url, json=payload, headers=fabric_headers(scope), timeout=60)
    _raise_for_status(resp, "PATCH", url)
    return resp.json() if resp.content else {}


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=2, max=30))
def fabric_delete(path: str, scope: str = FABRIC_SCOPE) -> bool:
    url  = f"{FABRIC_API_BASE}/{path.lstrip('/')}"
    resp = requests.delete(url, headers=fabric_headers(scope), timeout=60)
    if resp.status_code == 404:
        return False
    _raise_for_status(resp, "DELETE", url)
    return True


def _raise_for_status(resp: requests.Response, method: str, url: str):
    if resp.status_code not in (200, 201, 202, 204):
        log.error("%s %s → %d: %s", method, url, resp.status_code, resp.text[:500])
        resp.raise_for_status()


# ─── Long-running operation poller ───────────────────────────────────────────

def poll_lro(operation_url: str, timeout_sec: int = 600,
             scope: str = FABRIC_SCOPE) -> dict:
    """Poll a Fabric long-running operation until complete or timeout."""
    deadline = time.time() + timeout_sec
    interval = 5
    while time.time() < deadline:
        resp = requests.get(operation_url,
                            headers=fabric_headers(scope), timeout=30)
        resp.raise_for_status()
        data   = resp.json()
        status = data.get("status", "").lower()
        if status in ("succeeded", "completed"):
            return data
        if status in ("failed", "canceled"):
            raise RuntimeError(f"LRO failed: {data}")
        log.debug("LRO status: %s — retrying in %ds", status, interval)
        time.sleep(interval)
        interval = min(interval * 1.5, 30)   # back off up to 30s
    raise TimeoutError(f"LRO timed out after {timeout_sec}s: {operation_url}")
