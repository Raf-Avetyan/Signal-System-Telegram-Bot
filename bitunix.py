import os
import time
import hashlib
import requests
from urllib.parse import urlencode

# --- BITUNIX PARTNER API SETTINGS ---
BITUNIX_API_KEY    = os.getenv("BITUNIX_API_KEY", "")
BITUNIX_API_SECRET = os.getenv("BITUNIX_API_SECRET", "")
BITUNIX_BASE_URL   = "https://partners.bitunix.com"


def _get_parameter_type(key: str) -> int:
    """Sort priority: digit=1, lowercase=2, other(uppercase)=3."""
    if key[0].isdigit():
        return 1
    elif key[0].islower():
        return 2
    return 3


def _str_ascii_sum(s: str) -> int:
    return sum(ord(c) for c in s)


def _sign(params: dict, api_secret: str) -> str:
    """
    Build signature per Bitunix docs:
    1. Sort keys by (type, ascii_sum)  — number > lowercase > uppercase
    2. Concatenate values in that order
    3. Append api_secret
    4. SHA1 of the whole string
    """
    sorted_keys = sorted(
        params.keys(),
        key=lambda k: (_get_parameter_type(k), _str_ascii_sum(k))
    )
    concatenated = "".join(str(params[k]) for k in sorted_keys)
    return hashlib.sha1((concatenated + api_secret).encode("utf-8")).hexdigest()


def _build_url_params(params: dict) -> str:
    """Build query string sorted by Bitunix key order."""
    sorted_keys = sorted(
        params.keys(),
        key=lambda k: (_get_parameter_type(k), _str_ascii_sum(k))
    )
    return urlencode({k: params[k] for k in sorted_keys})


def verify_bitunix_user(uid: str) -> bool:
    """
    Check if a user UID is a direct referral of the current partner.

    Returns:
        True if the UID is a referral, False on any error or if not a referral.
    """
    if not BITUNIX_API_KEY or not BITUNIX_API_SECRET:
        print("[BITUNIX] API keys not configured — skipping referral check.")
        return False

    params = {
        "timestamp": int(time.time()),
        "account":   uid,
    }

    signature = _sign(params, BITUNIX_API_SECRET)
    query_str = _build_url_params(params)

    headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "apiKey":        BITUNIX_API_KEY,
        "signature":     signature,
    }

    url = f"{BITUNIX_BASE_URL}/partner/api/v2/openapi/validateUser?{query_str}"

    try:
        resp = requests.post(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if str(data.get("code")) != "0":
            print(f"[BITUNIX] API error: {data.get('msg', 'unknown')}")
            return False

        result = data.get("result", {})
        return bool(result.get("result", False))

    except requests.RequestException as e:
        print(f"[BITUNIX] Request error: {e}")
        return False
    except Exception as e:
        print(f"[BITUNIX] Unexpected error: {e}")
        return False
