import os
import time
import hmac
import hashlib
import requests
import json
from config import BITUNIX_REG_LINK, REQUIRED_DEPOSIT

# --- BITUNIX PARTNER API SETTINGS ---
# These should be in your .env file
BITUNIX_API_KEY = os.getenv("BITUNIX_API_KEY", "")
BITUNIX_API_SECRET = os.getenv("BITUNIX_API_SECRET", "")
BITUNIX_BASE_URL = "https://api.bitunix.com" # Check your partner dashboard for the exact URL

class BitunixAffiliate:
    def __init__(self, key, secret):
        self.key = key
        self.secret = secret

    def _generate_sign(self, timestamp, method, path, params=None, body=None):
        """Standard Bitunix HMAC-SHA256 signature logic."""
        query_str = ""
        if params:
            query_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        
        body_str = json.dumps(body) if body else ""
        
        payload = f"{timestamp}{method}{path}{query_str}{body_str}"
        return hmac.new(
            self.secret.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    def check_uid_status(self, uid):
        """
        Calls the Bitunix Partner API to verify referral status and deposit.
        NOTE: You must get the exact endpoint path from your Bitunix Account Manager.
        """
        if not self.key or not self.secret:
            return None # Keys not configured
            
        path = "/api/v1/affiliate/user/detail" # Example endpoint
        method = "GET"
        params = {"uid": uid}
        timestamp = str(int(time.time() * 1000))
        
        sign = self._generate_sign(timestamp, method, path, params=params)
        
        headers = {
            "api-key": self.key,
            "timestamp": timestamp,
            "nonce": str(int(time.time())),
            "sign": sign,
            "Content-Type": "application/json"
        }
        
        try:
            url = f"{BITUNIX_BASE_URL}{path}"
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # Example response parsing (adjust based on actual Bitunix API)
                if data.get("code") == 0:
                    user_data = data.get("data", {})
                    is_referral = user_data.get("is_referral", False)
                    # For Bitunix, we might need to check 'total_deposit' or similar
                    total_dep = user_data.get("total_deposit", 0)
                    has_deposited = total_dep >= REQUIRED_DEPOSIT
                    return is_referral, has_deposited
            return False, False
        except Exception as e:
            print(f"[BITUNIX API ERROR] {e}")
            return False, False

def verify_bitunix_user(uid):
    """
    Check if UID is a referral and has deposited sufficient funds.
    """
    # 1. Try real API if keys exist
    if BITUNIX_API_KEY and BITUNIX_API_SECRET:
        affiliate = BitunixAffiliate(BITUNIX_API_KEY, BITUNIX_API_SECRET)
        result = affiliate.check_uid_status(uid)
        if result is not None:
            return result

    return False, False
