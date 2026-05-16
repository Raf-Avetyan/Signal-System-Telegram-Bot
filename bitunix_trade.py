import hashlib
import json
import math
import os
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from config import (
    BITUNIX_DEFAULT_LEVERAGE,
    BITUNIX_FAPI_BASE_URL,
    BITUNIX_FAPI_KEY,
    BITUNIX_FAPI_SECRET,
    BITUNIX_FETCH_SYMBOL_RULES,
    BITUNIX_LIQUIDATION_MAX_LEVERAGE_BY_TF,
    BITUNIX_LIQUIDATION_SAFETY_BUFFER_R,
    BITUNIX_LIQUIDATION_SAFETY_ENABLED,
    BITUNIX_MARGIN_COIN,
    BITUNIX_MAX_DEPOSIT_USAGE_PCT,
    BITUNIX_MAX_OPEN_POSITIONS,
    BITUNIX_MAX_RISK_USD,
    BITUNIX_MIN_DEPOSIT_USAGE_PCT,
    BITUNIX_MIN_BASE_QTY,
    BITUNIX_MIN_NOTIONAL_USD,
    BITUNIX_POSITION_MODE,
    BITUNIX_QTY_STEP,
    BITUNIX_REQUIRED_MARGIN_MODE,
    BITUNIX_RISK_CAP_PCT,
    BITUNIX_TPSL_TRIGGER_TYPE,
    BITUNIX_TRADING_ENABLED,
    BITUNIX_TRADING_MODE,
    BREAKEVEN_FEE_BUFFER_PCT,
    SYMBOL,
    get_tp_splits_for_tf,
)


class BitunixTradeError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        endpoint: Optional[str] = None,
        response_text: Optional[str] = None,
        request_url: Optional[str] = None,
        status_code: Optional[int] = None,
        request_body: Optional[str] = None,
    ):
        super().__init__(message)
        self.endpoint = endpoint
        self.response_text = response_text
        self.request_url = request_url
        self.status_code = status_code
        self.request_body = request_body


@dataclass
class ExecutionResult:
    mode: str
    accepted: bool
    message: str
    payload: Dict[str, Any]


LIMIT_ENTRY_REPRICE_BPS = (2.0, 4.0, 7.0, 11.0, 16.0, 24.0)
LIMIT_ENTRY_MAX_RETRIES = 6
LIMIT_ENTRY_POLL_RETRIES = 8
LIMIT_ENTRY_POLL_DELAY_SEC = 0.55
LIMIT_ENTRY_FULL_FILL_TOLERANCE_PCT = 0.02
BITUNIX_PUBLIC_CACHE_TTL_SEC = {
    "/api/v1/futures/market/kline": 8.0,
    "/api/v1/futures/market/depth": 4.0,
    "/api/v1/futures/market/tickers": 4.0,
    "/api/v1/futures/market/funding_rate": 10.0,
    "/api/v1/futures/market/get_funding_rate_history": 60.0,
}
BITUNIX_PUBLIC_MIN_INTERVAL_SEC = 0.14


def _sorted_keys(d: Dict[str, Any]) -> List[str]:
    # Bitunix docs require query params sorted in ascending ASCII order by key.
    return sorted(d.keys())


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class BitunixFuturesClient:
    _public_cache: Dict[str, Dict[str, Any]] = {}
    _public_cache_lock = threading.Lock()
    _public_rate_lock = threading.Lock()
    _public_next_allowed_ts = 0.0
    _private_nonce_lock = threading.Lock()
    _private_last_nonce = 0

    def __init__(self):
        self.base_url = BITUNIX_FAPI_BASE_URL.rstrip("/")
        self.api_key = BITUNIX_FAPI_KEY
        self.api_secret = BITUNIX_FAPI_SECRET

    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def call_private(
        self,
        method: str,
        path: str,
        *,
        payload: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._request(method, path, payload=payload, query=query)

    def call_public(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._public_request(method, path, query=query)

    def _sign(self, params: Dict[str, Any]) -> str:
        raise NotImplementedError("Use _build_signature() with separated query/body parts.")

    def _build_query(self, params: Dict[str, Any]) -> str:
        ordered = {k: params[k] for k in _sorted_keys(params)}
        return urlencode(ordered)

    def _build_signature(self, nonce: str, timestamp: str, query: Dict[str, Any], body: str) -> str:
        query_params = "".join(f"{k}{query[k]}" for k in _sorted_keys(query))
        digest = _sha256_hex(nonce + timestamp + self.api_key + query_params + body)
        return _sha256_hex(digest + self.api_secret)

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self.is_configured():
            raise BitunixTradeError("Bitunix futures API credentials are not configured.")
        payload = payload or {}
        query = query or {}
        timestamp = self._next_private_nonce()
        nonce = timestamp
        body = json.dumps(payload, separators=(",", ":")) if payload else ""
        headers = {
            "api-key": self.api_key,
            "sign": self._build_signature(nonce, timestamp, query, body),
            "nonce": nonce,
            "timestamp": timestamp,
            "language": "en-US",
            "Content-Type": "application/json",
        }

        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{self._build_query(query)}"

        try:
            resp = requests.request(method, url, headers=headers, data=(body or None), timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except requests.HTTPError as e:
            text = e.response.text if e.response is not None else None
            raise BitunixTradeError(
                f"HTTP error on {method} {path}: {e}",
                endpoint=f"{method} {path}",
                response_text=text,
                request_url=url,
                status_code=(e.response.status_code if e.response is not None else None),
                request_body=body or None,
            ) from e
        except Exception as e:
            raise BitunixTradeError(
                f"Request failed on {method} {path}: {e}",
                endpoint=f"{method} {path}",
                request_url=url,
                request_body=body or None,
            ) from e
        code = str(data.get("code"))
        if code not in {"0", "200"}:
            raise BitunixTradeError(
                f"Bitunix API error {code}: {data.get('msg', 'unknown error')}",
                endpoint=f"{method} {path}",
                response_text=json.dumps(data),
                request_url=url,
                status_code=int(resp.status_code) if getattr(resp, "status_code", None) is not None else None,
                request_body=body or None,
            )
        return data

    @classmethod
    def _next_private_nonce(cls) -> str:
        with cls._private_nonce_lock:
            now_ms = int(time.time() * 1000)
            cls._private_last_nonce = max(now_ms, cls._private_last_nonce + 1)
            return str(cls._private_last_nonce)

    def _public_request(self, method: str, path: str, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        query = query or {}
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{self._build_query(query)}"
        cache_key = f"{method.upper()}::{url}"
        ttl = float(BITUNIX_PUBLIC_CACHE_TTL_SEC.get(path, 0.0) or 0.0)
        if method.upper() == "GET" and ttl > 0:
            with self._public_cache_lock:
                cached = dict(self._public_cache.get(cache_key) or {})
            cached_ts = float(cached.get("ts") or 0.0)
            if cached and cached_ts and (time.time() - cached_ts) <= ttl:
                return dict(cached.get("data") or {})

        last_error: Optional[Exception] = None
        stale_cached = None
        if method.upper() == "GET" and ttl > 0:
            with self._public_cache_lock:
                raw_cached = dict(self._public_cache.get(cache_key) or {})
            cached_ts = float(raw_cached.get("ts") or 0.0)
            if raw_cached and cached_ts and (time.time() - cached_ts) <= max(ttl * 6.0, 30.0):
                stale_cached = dict(raw_cached.get("data") or {})

        for attempt in range(2):
            try:
                self._wait_for_public_slot()
                resp = requests.request(method, url, timeout=20)
                resp.raise_for_status()
                data = resp.json()
            except requests.HTTPError as e:
                text = e.response.text if e.response is not None else None
                last_error = BitunixTradeError(
                    f"HTTP error on {method} {path}: {e}",
                    endpoint=f"{method} {path}",
                    response_text=text,
                    request_url=url,
                    status_code=(e.response.status_code if e.response is not None else None),
                )
                if attempt == 0:
                    time.sleep(0.35)
                    continue
                break
            except Exception as e:
                last_error = BitunixTradeError(
                    f"Request failed on {method} {path}: {e}",
                    endpoint=f"{method} {path}",
                    request_url=url,
                )
                if attempt == 0:
                    time.sleep(0.35)
                    continue
                break

            code = str(data.get("code"))
            if code not in {"0", "200"}:
                last_error = BitunixTradeError(
                    f"Bitunix API error {code}: {data.get('msg', 'unknown error')}",
                    endpoint=f"{method} {path}",
                    response_text=json.dumps(data),
                    request_url=url,
                    status_code=int(resp.status_code) if getattr(resp, "status_code", None) is not None else None,
                )
                if code == "800021" and attempt == 0:
                    time.sleep(0.45)
                    continue
                break

            if method.upper() == "GET" and ttl > 0:
                with self._public_cache_lock:
                    self._public_cache[cache_key] = {
                        "ts": time.time(),
                        "data": data,
                    }
            return data

        if stale_cached is not None:
            return stale_cached
        if last_error is not None:
            raise last_error
        raise BitunixTradeError(
            f"Request failed on {method} {path}: unknown error",
            endpoint=f"{method} {path}",
        )

    @classmethod
    def _wait_for_public_slot(cls) -> None:
        with cls._public_rate_lock:
            now = time.time()
            wait_sec = max(0.0, cls._public_next_allowed_ts - now)
            if wait_sec > 0:
                time.sleep(wait_sec)
                now = time.time()
            cls._public_next_allowed_ts = max(now, cls._public_next_allowed_ts) + BITUNIX_PUBLIC_MIN_INTERVAL_SEC

    def get_single_account(self, margin_coin: str = BITUNIX_MARGIN_COIN) -> Dict[str, Any]:
        return self._request("GET", "/api/v1/futures/account", query={"marginCoin": margin_coin})

    def adjust_position_margin(
        self,
        symbol: str,
        amount: float,
        *,
        margin_coin: str = BITUNIX_MARGIN_COIN,
        side: Optional[str] = None,
        position_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "marginCoin": margin_coin,
            "amount": self._fmt_num(amount),
        }
        if side:
            payload["side"] = str(side).upper()
        if position_id:
            payload["positionId"] = str(position_id)
        return self._request("POST", "/api/v1/futures/account/adjust_position_margin", payload=payload)

    def change_leverage(self, symbol: str, leverage: int, margin_coin: str = BITUNIX_MARGIN_COIN) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/v1/futures/account/change_leverage",
            payload={
                "symbol": symbol,
                "marginCoin": margin_coin,
                "leverage": str(leverage),
            },
        )

    def change_margin_mode(self, symbol: str, margin_mode: str, margin_coin: str = BITUNIX_MARGIN_COIN) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/v1/futures/account/change_margin_mode",
            payload={
                "symbol": symbol,
                "marginCoin": margin_coin,
                "marginMode": str(margin_mode).upper(),
            },
        )

    def change_position_mode(self, position_mode: str) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/api/v1/futures/account/change_position_mode",
            payload={"positionMode": str(position_mode).upper()},
        )

    def get_leverage_margin_mode(self, symbol: str, margin_coin: str = BITUNIX_MARGIN_COIN) -> Dict[str, Any]:
        return self._request(
            "GET",
            "/api/v1/futures/account/get_leverage_margin_mode",
            query={"symbol": symbol, "marginCoin": margin_coin},
        )

    def get_position_tiers(self, symbol: str) -> Dict[str, Any]:
        return self._request("GET", "/api/v1/futures/position/get_position_tiers", query={"symbol": symbol})

    def get_depth(self, symbol: str, limit: Optional[str] = None) -> Dict[str, Any]:
        query: Dict[str, Any] = {"symbol": symbol}
        if limit:
            query["limit"] = str(limit)
        return self._public_request("GET", "/api/v1/futures/market/depth", query=query)

    def get_funding_rate(self, symbol: str) -> Dict[str, Any]:
        return self._public_request("GET", "/api/v1/futures/market/funding_rate", query={"symbol": symbol})

    def get_funding_rate_batch(self) -> Dict[str, Any]:
        return self._public_request("GET", "/api/v1/futures/market/funding_rate/batch")

    def get_funding_rate_history(
        self,
        symbol: str,
        *,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {"symbol": symbol}
        if start_time is not None:
            query["starTime"] = int(start_time)
        if end_time is not None:
            query["endTime"] = int(end_time)
        if limit is not None:
            query["limit"] = int(limit)
        return self._public_request("GET", "/api/v1/futures/market/get_funding_rate_history", query=query)

    def get_kline(
        self,
        symbol: str,
        interval: str,
        *,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
        price_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {"symbol": symbol, "interval": interval}
        if start_time is not None:
            query["startTime"] = int(start_time)
        if end_time is not None:
            query["endTime"] = int(end_time)
        if limit is not None:
            query["limit"] = min(int(limit), 200)
        if price_type:
            query["type"] = str(price_type).upper()
        return self._public_request("GET", "/api/v1/futures/market/kline", query=query)

    def get_tickers(self, symbols: Optional[str] = None) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if symbols:
            query["symbols"] = symbols
        return self._public_request("GET", "/api/v1/futures/market/tickers", query=query)

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        order_type: str = "MARKET",
        price: Optional[float] = None,
        reduce_only: Optional[bool] = None,
        client_id: Optional[str] = None,
        effect: Optional[str] = "GTC",
        trade_side: Optional[str] = None,
        position_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "qty": self._fmt_num(qty),
            "side": side,
            "orderType": order_type,
        }
        if reduce_only is not None:
            payload["reduceOnly"] = bool(reduce_only)
        if client_id:
            payload["clientId"] = client_id
        if trade_side:
            payload["tradeSide"] = str(trade_side).upper()
        if position_id:
            payload["positionId"] = str(position_id)
        if order_type == "LIMIT":
            if price is None:
                raise BitunixTradeError("Limit order requires price.")
            payload["price"] = self._fmt_num(price)
            if effect:
                payload["effect"] = effect
        return self._request("POST", "/api/v1/futures/trade/place_order", payload=payload)

    def batch_order(self, symbol: str, order_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "orderList": order_list,
        }
        return self._request("POST", "/api/v1/futures/trade/batch_order", payload=payload)

    def get_pending_positions(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if symbol:
            query["symbol"] = symbol
        return self._request("GET", "/api/v1/futures/position/get_pending_positions", query=query)

    def get_order_detail(self, order_id: Optional[str] = None, client_id: Optional[str] = None) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if order_id:
            query["orderId"] = str(order_id)
        if client_id:
            query["clientId"] = str(client_id)
        if not query:
            raise BitunixTradeError("Either order_id or client_id is required for order detail lookup.")
        return self._request("GET", "/api/v1/futures/trade/get_order_detail", query=query)

    def place_position_tpsl(self, symbol: str, position_id: str, tp_price: Optional[float], sl_price: Optional[float]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "positionId": str(position_id),
        }
        if tp_price is not None:
            payload["tpPrice"] = self._fmt_num(tp_price)
            payload["tpStopType"] = BITUNIX_TPSL_TRIGGER_TYPE
        if sl_price is not None:
            payload["slPrice"] = self._fmt_num(sl_price)
            payload["slStopType"] = BITUNIX_TPSL_TRIGGER_TYPE
        return self._request("POST", "/api/v1/futures/tpsl/position/place_order", payload=payload)

    def place_tpsl_order(
        self,
        symbol: str,
        position_id: str,
        *,
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
        tp_qty: Optional[float] = None,
        sl_qty: Optional[float] = None,
        tp_order_type: str = "MARKET",
        sl_order_type: str = "MARKET",
        tp_order_price: Optional[float] = None,
        sl_order_price: Optional[float] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "positionId": str(position_id),
        }
        if tp_price is not None:
            payload["tpPrice"] = self._fmt_num(tp_price)
            payload["tpStopType"] = BITUNIX_TPSL_TRIGGER_TYPE
            payload["tpOrderType"] = str(tp_order_type).upper()
            if tp_qty is not None:
                payload["tpQty"] = self._fmt_num(tp_qty)
            if str(tp_order_type).upper() == "LIMIT":
                if tp_order_price is None:
                    raise BitunixTradeError("tpOrderPrice is required when tpOrderType is LIMIT.")
                payload["tpOrderPrice"] = self._fmt_num(tp_order_price)
        if sl_price is not None:
            payload["slPrice"] = self._fmt_num(sl_price)
            payload["slStopType"] = BITUNIX_TPSL_TRIGGER_TYPE
            payload["slOrderType"] = str(sl_order_type).upper()
            if sl_qty is not None:
                payload["slQty"] = self._fmt_num(sl_qty)
            if str(sl_order_type).upper() == "LIMIT":
                if sl_order_price is None:
                    raise BitunixTradeError("slOrderPrice is required when slOrderType is LIMIT.")
                payload["slOrderPrice"] = self._fmt_num(sl_order_price)
        return self._request("POST", "/api/v1/futures/tpsl/place_order", payload=payload)

    def get_pending_tpsl(self, symbol: str, position_id: Optional[str] = None) -> Dict[str, Any]:
        query: Dict[str, Any] = {"symbol": symbol}
        if position_id:
            query["positionId"] = position_id
        return self._request("GET", "/api/v1/futures/tpsl/get_pending_orders", query=query)

    def get_pending_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if symbol:
            query["symbol"] = symbol
        return self._request("GET", "/api/v1/futures/trade/get_pending_orders", query=query)

    def get_history_orders(
        self,
        symbol: Optional[str] = None,
        *,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if symbol:
            query["symbol"] = symbol
        if start_time is not None:
            query["startTime"] = int(start_time)
        if end_time is not None:
            query["endTime"] = int(end_time)
        return self._request("GET", "/api/v1/futures/trade/get_history_orders", query=query)

    def get_history_trades(
        self,
        symbol: Optional[str] = None,
        *,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if symbol:
            query["symbol"] = symbol
        if start_time is not None:
            query["startTime"] = int(start_time)
        if end_time is not None:
            query["endTime"] = int(end_time)
        return self._request("GET", "/api/v1/futures/trade/get_history_trades", query=query)

    def get_history_positions(
        self,
        symbol: Optional[str] = None,
        *,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if symbol:
            query["symbol"] = symbol
        if start_time is not None:
            query["startTime"] = int(start_time)
        if end_time is not None:
            query["endTime"] = int(end_time)
        return self._request("GET", "/api/v1/futures/position/get_history_positions", query=query)

    def get_history_tpsl(
        self,
        symbol: Optional[str] = None,
        *,
        position_id: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if symbol:
            query["symbol"] = symbol
        if position_id:
            query["positionId"] = str(position_id)
        if start_time is not None:
            query["startTime"] = int(start_time)
        if end_time is not None:
            query["endTime"] = int(end_time)
        return self._request("GET", "/api/v1/futures/tpsl/get_history_orders", query=query)

    def get_trading_pairs(self, symbols: Optional[str] = None) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if symbols:
            query["symbols"] = symbols
        return self._public_request("GET", "/api/v1/futures/market/trading_pairs", query=query)

    def modify_position_tpsl(self, symbol: str, position_id: str, tp_price: Optional[float], sl_price: Optional[float]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "positionId": str(position_id),
        }
        if tp_price is not None:
            payload["tpPrice"] = self._fmt_num(tp_price)
            payload["tpStopType"] = BITUNIX_TPSL_TRIGGER_TYPE
        if sl_price is not None:
            payload["slPrice"] = self._fmt_num(sl_price)
            payload["slStopType"] = BITUNIX_TPSL_TRIGGER_TYPE
        return self._request("POST", "/api/v1/futures/tpsl/position/modify_order", payload=payload)

    def modify_tpsl_order(
        self,
        order_id: str,
        *,
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
        tp_qty: Optional[float] = None,
        sl_qty: Optional[float] = None,
        tp_order_type: Optional[str] = None,
        sl_order_type: Optional[str] = None,
        tp_order_price: Optional[float] = None,
        sl_order_price: Optional[float] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"orderId": str(order_id)}
        if tp_price is not None:
            payload["tpPrice"] = self._fmt_num(tp_price)
            payload["tpStopType"] = BITUNIX_TPSL_TRIGGER_TYPE
        if sl_price is not None:
            payload["slPrice"] = self._fmt_num(sl_price)
            payload["slStopType"] = BITUNIX_TPSL_TRIGGER_TYPE
        if tp_qty is not None:
            payload["tpQty"] = self._fmt_num(tp_qty)
        if sl_qty is not None:
            payload["slQty"] = self._fmt_num(sl_qty)
        if tp_order_type:
            payload["tpOrderType"] = str(tp_order_type).upper()
        if sl_order_type:
            payload["slOrderType"] = str(sl_order_type).upper()
        if tp_order_price is not None:
            payload["tpOrderPrice"] = self._fmt_num(tp_order_price)
        if sl_order_price is not None:
            payload["slOrderPrice"] = self._fmt_num(sl_order_price)
        return self._request("POST", "/api/v1/futures/tpsl/modify_order", payload=payload)

    def cancel_tpsl(self, symbol: str, order_id: str) -> Dict[str, Any]:
        return self._request("POST", "/api/v1/futures/tpsl/cancel_order", payload={"symbol": symbol, "orderId": str(order_id)})

    def cancel_orders(self, symbol: str, order_ids: List[str]) -> Dict[str, Any]:
        order_list = [{"orderId": str(order_id)} for order_id in order_ids if order_id]
        if not order_list:
            return {"code": "0", "data": {"successList": [], "failureList": []}, "msg": "Nothing to cancel"}
        return self._request("POST", "/api/v1/futures/trade/cancel_orders", payload={"symbol": symbol, "orderList": order_list})

    def cancel_all_orders(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if symbol:
            payload["symbol"] = symbol
        return self._request("POST", "/api/v1/futures/trade/cancel_all_orders", payload=payload)

    def close_all_position(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if symbol:
            payload["symbol"] = symbol
        return self._request("POST", "/api/v1/futures/trade/close_all_position", payload=payload)

    def flash_close_position(self, position_id: str) -> Dict[str, Any]:
        return self._request("POST", "/api/v1/futures/trade/flash_close_position", payload={"positionId": str(position_id)})

    def modify_order(
        self,
        *,
        qty: float,
        price: float,
        order_id: Optional[str] = None,
        client_id: Optional[str] = None,
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
        tp_order_type: Optional[str] = None,
        sl_order_type: Optional[str] = None,
        tp_order_price: Optional[float] = None,
        sl_order_price: Optional[float] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "qty": self._fmt_num(qty),
            "price": self._fmt_num(price),
        }
        if order_id:
            payload["orderId"] = str(order_id)
        if client_id:
            payload["clientId"] = str(client_id)
        if tp_price is not None:
            payload["tpPrice"] = self._fmt_num(tp_price)
            payload["tpStopType"] = BITUNIX_TPSL_TRIGGER_TYPE
        if sl_price is not None:
            payload["slPrice"] = self._fmt_num(sl_price)
            payload["slStopType"] = BITUNIX_TPSL_TRIGGER_TYPE
        if tp_order_type:
            payload["tpOrderType"] = str(tp_order_type).upper()
        if sl_order_type:
            payload["slOrderType"] = str(sl_order_type).upper()
        if tp_order_price is not None:
            payload["tpOrderPrice"] = self._fmt_num(tp_order_price)
        if sl_order_price is not None:
            payload["slOrderPrice"] = self._fmt_num(sl_order_price)
        return self._request("POST", "/api/v1/futures/trade/modify_order", payload=payload)

    @staticmethod
    def _fmt_num(value: float) -> str:
        return f"{float(value):.8f}".rstrip("0").rstrip(".")


class TradeExecutor:
    def __init__(self):
        self.client = BitunixFuturesClient()
        self.mode = BITUNIX_TRADING_MODE
        self.enabled = BITUNIX_TRADING_ENABLED and self.mode in {"demo", "live"}
        self._symbol_rules_cache: Dict[str, Dict[str, Any]] = {}

    def _refresh_state(self):
        mode = os.getenv("BITUNIX_TRADING_MODE", self.mode).strip().lower()
        enabled_raw = os.getenv("BITUNIX_TRADING_ENABLED", "true" if self.enabled else "false").strip().lower()
        self.mode = mode
        self.enabled = enabled_raw == "true" and self.mode in {"demo", "live"}

    def _current_sl_price(self, signal: Dict[str, Any], execution: Dict[str, Any]) -> Optional[float]:
        try:
            moved = float(execution.get("sl_moved_to")) if execution.get("sl_moved_to") not in (None, "", "None") else None
        except Exception:
            moved = None
        if moved and moved > 0:
            return moved
        try:
            sl = float(signal.get("sl") or 0)
        except Exception:
            sl = 0.0
        return sl if sl > 0 else None

    def _current_position_tp_price(self, signal: Dict[str, Any], execution: Dict[str, Any]) -> Optional[float]:
        for order in execution.get("tp_orders") or []:
            if str(order.get("kind", "")).upper() == "POSITION_TP":
                try:
                    price = float(order.get("price") or 0)
                except Exception:
                    price = 0.0
                if price > 0:
                    return price
        qtys = list(execution.get("tp_qtys") or [0.0, 0.0, 0.0])
        active_indices = [i + 1 for i, q in enumerate(qtys[:3]) if float(q or 0) > 0]
        if len(active_indices) == 1:
            idx = active_indices[0]
            targets = list(execution.get("tp_targets") or [signal.get("tp1"), signal.get("tp2"), signal.get("tp3")])
            try:
                price = float(targets[idx - 1] or 0)
            except Exception:
                price = 0.0
            if price > 0:
                return price
        return None

    @staticmethod
    def _breakeven_lock_price(signal: Dict[str, Any], execution: Optional[Dict[str, Any]] = None) -> float:
        execution = execution or {}
        try:
            entry = float(
                execution.get("filled_entry_price")
                or execution.get("entry")
                or signal.get("entry")
                or 0
            )
        except Exception:
            entry = float(signal.get("entry") or 0)
        side = str(signal.get("side") or "").upper()
        buffer_pct = max(0.0, float(BREAKEVEN_FEE_BUFFER_PCT or 0.0))
        if entry <= 0:
            return 0.0
        if side == "LONG":
            return entry * (1.0 + buffer_pct / 100.0)
        if side == "SHORT":
            return entry * (1.0 - buffer_pct / 100.0)
        return entry

    @staticmethod
    def _adaptive_tp_splits_for_signal(signal: Dict[str, Any], base_splits: tuple[float, float, float]) -> tuple[float, float, float]:
        meta = (signal or {}).get("meta") or {}
        grade = str(meta.get("quality_grade") or "").upper()
        trend_aligned = bool(meta.get("trend_aligned"))
        countertrend = bool(meta.get("countertrend"))
        strategy = str(meta.get("strategy") or signal.get("strategy") or "").upper()
        if strategy == "SMART_MONEY_LIQUIDITY":
            return tuple(float(x or 0) for x in base_splits[:3])
        if grade == "A+" and trend_aligned and not countertrend:
            return (0.20, 0.30, 0.50)
        if grade == "B" or countertrend:
            return (0.40, 0.35, 0.25)
        if grade == "A":
            return (0.30, 0.40, 0.30)
        return tuple(float(x or 0) for x in base_splits[:3])

    def _update_position_stop(self, symbol: str, position_id: str, signal: Dict[str, Any], execution: Dict[str, Any], new_sl: float) -> None:
        current_tp_price = self._current_position_tp_price(signal, execution)
        if current_tp_price is not None:
            self.client.modify_position_tpsl(symbol, str(position_id), current_tp_price, new_sl)
            return
        try:
            self.client.place_position_tpsl(symbol, str(position_id), None, new_sl)
        except BitunixTradeError:
            self.client.modify_position_tpsl(symbol, str(position_id), None, new_sl)

    def can_trade(self) -> bool:
        self._refresh_state()
        return self.enabled

    def status_line(self) -> str:
        self._refresh_state()
        return f"mode={self.mode} enabled={self.enabled} configured={self.client.is_configured()}"

    @staticmethod
    def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = (payload or {}).get("data", [])
        if isinstance(data, list):
            return list(data)
        if isinstance(data, dict):
            for key in ("orderList", "tradeList", "positionList", "list", "rows", "records", "data"):
                rows = data.get(key)
                if isinstance(rows, list):
                    return list(rows)
        return []

    @classmethod
    def _data_dict(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = (payload or {}).get("data", {})
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    return item
        rows = cls._extract_rows(payload or {})
        for row in rows:
            if isinstance(row, dict):
                return row
        return {}

    @staticmethod
    def _history_num(row: Dict[str, Any], *keys: str) -> float:
        for key in keys:
            try:
                value = row.get(key)
            except Exception:
                value = None
            if value in (None, "", "None"):
                continue
            try:
                return float(value)
            except Exception:
                continue
        return 0.0

    def get_history_snapshot(
        self,
        *,
        symbol: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Dict[str, Any]:
        self._refresh_state()
        report: Dict[str, Any] = {
            "mode": self.mode,
            "configured": self.client.is_configured(),
            "orders": [],
            "trades": [],
            "positions": [],
            "tpsl": [],
            "summary": {
                "orders_total": 0,
                "orders_filled": 0,
                "orders_cancelled": 0,
                "trades_total": 0,
                "trades_realized_pnl": 0.0,
                "trades_fees": 0.0,
                "positions_total": 0,
                "positions_realized_pnl": 0.0,
                "positions_fees": 0.0,
                "positions_funding": 0.0,
                "tpsl_total": 0,
                "tpsl_tp_rows": 0,
                "tpsl_sl_rows": 0,
                "symbols": [],
            },
            "errors": [],
        }
        if self.mode != "live" or not self.client.is_configured():
            return report

        try:
            report["orders"] = self._extract_rows(
                self.client.get_history_orders(symbol=symbol, start_time=start_time, end_time=end_time)
            )
        except Exception as e:
            report["errors"].append(f"history_orders: {e}")

        try:
            report["trades"] = self._extract_rows(
                self.client.get_history_trades(symbol=symbol, start_time=start_time, end_time=end_time)
            )
        except Exception as e:
            report["errors"].append(f"history_trades: {e}")

        try:
            report["positions"] = self._extract_rows(
                self.client.get_history_positions(symbol=symbol, start_time=start_time, end_time=end_time)
            )
        except Exception as e:
            report["errors"].append(f"history_positions: {e}")

        try:
            report["tpsl"] = self._extract_rows(
                self.client.get_history_tpsl(symbol=symbol, start_time=start_time, end_time=end_time)
            )
        except Exception as e:
            report["errors"].append(f"history_tpsl: {e}")

        summary = report["summary"]
        symbol_set = set()

        for row in report["orders"]:
            summary["orders_total"] += 1
            status = str(row.get("status") or row.get("orderStatus") or "").upper()
            if "FILLED" in status:
                summary["orders_filled"] += 1
            if "CANCEL" in status:
                summary["orders_cancelled"] += 1
            symbol_text = str(row.get("symbol") or "").upper().strip()
            if symbol_text:
                symbol_set.add(symbol_text)

        for row in report["trades"]:
            summary["trades_total"] += 1
            summary["trades_realized_pnl"] += self._history_num(
                row, "realizedPnl", "realizedPNL", "profit", "pnl", "tradeProfit"
            )
            summary["trades_fees"] += self._history_num(
                row, "fee", "tradeFee", "makerFee", "takerFee"
            )
            symbol_text = str(row.get("symbol") or "").upper().strip()
            if symbol_text:
                symbol_set.add(symbol_text)

        for row in report["positions"]:
            summary["positions_total"] += 1
            summary["positions_realized_pnl"] += self._history_num(
                row, "realizedPnl", "realizedPNL", "closeProfit", "profit", "pnl"
            )
            summary["positions_fees"] += self._history_num(
                row, "fee", "closeFee", "tradingFee", "tradeFee"
            )
            summary["positions_funding"] += self._history_num(
                row, "fundingFee", "fundFee"
            )
            symbol_text = str(row.get("symbol") or "").upper().strip()
            if symbol_text:
                symbol_set.add(symbol_text)

        for row in report["tpsl"]:
            summary["tpsl_total"] += 1
            if self._history_num(row, "tpPrice") > 0:
                summary["tpsl_tp_rows"] += 1
            if self._history_num(row, "slPrice") > 0:
                summary["tpsl_sl_rows"] += 1
            symbol_text = str(row.get("symbol") or "").upper().strip()
            if symbol_text:
                symbol_set.add(symbol_text)

        summary["symbols"] = sorted(symbol_set)
        return report

    def startup_self_check(self) -> Dict[str, Any]:
        """
        Return startup connectivity snapshot for logging:
        - configured/auth state
        - balance endpoint result
        - current open positions
        """
        self._refresh_state()
        info: Dict[str, Any] = {
            "mode": self.mode,
            "enabled": self.enabled,
            "configured": self.client.is_configured(),
            "auth_ok": False,
            "balance_available": 0.0,
            "balance_total": 0.0,
            "balance_used": 0.0,
            "open_positions": 0,
            "position_sides": [],
            "required_position_mode": str(BITUNIX_POSITION_MODE or "ONE_WAY").strip().upper(),
            "errors": [],
        }
        if not self.client.is_configured():
            info["errors"].append("Bitunix futures API key/secret missing.")
            return info

        bal = self._safe_get_balance()
        info["balance_available"] = float(bal.get("available", 0) or 0)
        info["balance_total"] = float(bal.get("total", 0) or 0)
        info["balance_used"] = float(bal.get("used", 0) or 0)
        raw = bal.get("raw") or {}
        info["position_mode"] = str(raw.get("positionMode") or BITUNIX_POSITION_MODE or "UNKNOWN").strip().upper()
        info["position_mode_ok"] = info["position_mode"] == info["required_position_mode"]
        info["leverage"] = int(raw.get("leverage") or BITUNIX_DEFAULT_LEVERAGE or 0)
        info["required_margin_mode"] = BITUNIX_REQUIRED_MARGIN_MODE
        if bal.get("error"):
            info["errors"].append(f"balance: {bal.get('error')}")
            if bal.get("endpoint"):
                info["balance_endpoint"] = bal.get("endpoint")
            if bal.get("response_text"):
                info["balance_response"] = bal.get("response_text")
        else:
            info["auth_ok"] = True

        pos = self._safe_get_open_positions()
        info["open_positions"] = int(pos.get("count", 0) or 0)
        info["position_sides"] = pos.get("sides", [])
        position_margin_used = float(pos.get("used_margin", 0) or 0)
        if position_margin_used > 0:
            if info["balance_used"] <= 0:
                info["balance_used"] = position_margin_used
            if info["balance_total"] <= info["balance_available"]:
                info["balance_total"] = float(info["balance_available"] or 0) + position_margin_used
        if pos.get("error"):
            info["errors"].append(f"positions: {pos.get('error')}")
            if pos.get("endpoint"):
                info["positions_endpoint"] = pos.get("endpoint")
            if pos.get("response_text"):
                info["positions_response"] = pos.get("response_text")
        elif info["auth_ok"]:
            info["auth_ok"] = True

        if self.client.is_configured():
            try:
                lm = self.client.get_leverage_margin_mode(SYMBOL, BITUNIX_MARGIN_COIN).get("data", {}) or {}
                info["margin_mode"] = str(lm.get("marginMode") or "").strip().upper()
                info["margin_mode_ok"] = info["margin_mode"] == BITUNIX_REQUIRED_MARGIN_MODE
            except Exception as e:
                info["errors"].append(f"margin_mode: {e}")

        rules = self._get_symbol_rules(SYMBOL)
        info["symbol_rules"] = rules

        return info

    def execute_signal(self, signal: Dict[str, Any], open_positions_count: int = 0) -> ExecutionResult:
        self._refresh_state()
        if not self.enabled:
            return ExecutionResult(self.mode, False, "Trading disabled.", {})
        exchange_open_positions = self.get_exchange_open_position_count() if self.mode == "live" else open_positions_count
        if exchange_open_positions >= BITUNIX_MAX_OPEN_POSITIONS:
            return ExecutionResult(
                self.mode,
                False,
                "Max open positions reached.",
                {"exchange_open_positions": exchange_open_positions},
            )
        if self.mode == "live" and self.client.is_configured():
            position_guard = self._ensure_required_position_mode(exchange_open_positions=exchange_open_positions)
            if not position_guard.get("ok", False):
                return ExecutionResult(self.mode, False, position_guard.get("message", "Required position mode not satisfied."), position_guard)
            margin_guard = self._ensure_required_margin_mode(SYMBOL, exchange_open_positions=exchange_open_positions)
            if not margin_guard.get("ok", False):
                return ExecutionResult(self.mode, False, margin_guard.get("message", "Required margin mode not satisfied."), margin_guard)

        plan = self._build_plan(signal)
        plan["exchange_open_positions"] = exchange_open_positions
        if plan["balance_available"] <= 0:
            return ExecutionResult(self.mode, False, "No available margin balance.", plan)
        if plan["qty"] <= 0 or plan["notional"] <= 0:
            return ExecutionResult(self.mode, False, "Calculated order size is zero after balance/leverage cap.", plan)
        if BITUNIX_LIQUIDATION_SAFETY_ENABLED and not plan.get("pre_liq_safe", True):
            return ExecutionResult(self.mode, False, f"Pre-trade liquidation safety failed: {plan.get('pre_liq_reason')}", plan)
        if self.mode == "demo":
            return ExecutionResult(self.mode, True, "Demo execution planned.", plan)
        if not self.client.is_configured():
            return ExecutionResult(self.mode, False, "Missing Bitunix live credentials.", plan)

        symbol = plan["symbol"]
        entry_side = plan["entry_side"]
        exit_side = plan["exit_side"]
        signal_id = signal.get("signal_id") or new_signal_id()
        existing_positions = [pos for pos in self._pending_positions_list(symbol) if self._position_qty(pos) > 0]
        same_side_position = self._match_position_side(existing_positions, str(signal.get("side") or ""))
        same_side_add_allowed = bool(((signal.get("meta") or {}).get("same_side_add_allowed")))
        if same_side_position:
            if same_side_add_allowed:
                meta = signal.get("meta") or {}
                owner_qty = float(meta.get("same_side_owner_qty") or 0)
                owner_entry = float(meta.get("same_side_owner_entry") or 0)
                owner_stop = float(meta.get("same_side_owner_stop") or 0)
                new_entry = float(signal.get("entry") or plan.get("entry") or 0)
                new_qty = float(plan.get("qty") or 0)
                total_qty = owner_qty + new_qty
                if owner_qty <= 0 or owner_entry <= 0 or owner_stop <= 0 or new_entry <= 0 or new_qty <= 0 or total_qty <= 0:
                    return ExecutionResult(
                        self.mode,
                        False,
                        "Same-side add was requested, but the protected owner-position context is incomplete.",
                        plan,
                    )
                projected_avg_entry = ((owner_entry * owner_qty) + (new_entry * new_qty)) / total_qty
                if str(signal.get("side") or "").upper() == "LONG":
                    projected_risk_usd = max(0.0, projected_avg_entry - owner_stop) * total_qty
                else:
                    projected_risk_usd = max(0.0, owner_stop - projected_avg_entry) * total_qty
                plan["same_side_add_projected_avg_entry"] = projected_avg_entry
                plan["same_side_add_projected_risk_usd"] = projected_risk_usd
                if projected_risk_usd > float(plan.get("risk_budget_usd") or 0):
                    return ExecutionResult(
                        self.mode,
                        False,
                        "Same-side add failed the merged-position risk cap check.",
                        plan,
                    )
            else:
                existing_position_id = str(same_side_position.get("positionId") or same_side_position.get("id") or "").strip()
                block_payload = dict(plan)
                if existing_position_id:
                    block_payload["existing_position_id"] = existing_position_id
                return ExecutionResult(
                    self.mode,
                    False,
                    f"Bitunix already has an open {signal['side']} {symbol} position. "
                    f"Opening another would merge into the same exchange position, so it was skipped.",
                    block_payload,
                )
            existing_position_id = str(same_side_position.get("positionId") or same_side_position.get("id") or "").strip()
            if existing_position_id:
                plan["existing_position_id"] = existing_position_id
        if str(plan.get("position_mode") or "").upper() == "ONE_WAY":
            if existing_positions and not same_side_add_allowed:
                return ExecutionResult(
                    self.mode,
                    False,
                    f"One-way mode already has an open {symbol} position on Bitunix. Close it before opening another live trade.",
                    plan,
                )

        try:
            self.client.change_leverage(symbol, int(plan["leverage"]))
            entry_order_data, position = self._place_limit_entry_until_filled(
                plan=plan,
                signal=signal,
                signal_id=signal_id,
            )
            entry_order_id = entry_order_data.get("orderId") or entry_order_data.get("id")
            entry_client_id = entry_order_data.get("clientId") or f"{signal_id}-entry-1"
            plan["entry_order_id"] = entry_order_id
            plan["entry_client_id"] = entry_client_id
            if not position:
                order_detail = self._safe_get_order_detail(order_id=entry_order_id, client_id=entry_client_id)
                if order_detail:
                    plan["entry_status"] = order_detail.get("status")
                    plan["entry_trade_qty"] = order_detail.get("tradeQty")
                raise BitunixTradeError(f"Entry accepted but no pending {signal['side']} position was returned.")
            self._apply_filled_position_to_plan(plan, position)
            live_stop_risk = abs(float(plan.get("filled_entry_price") or plan.get("entry") or 0) - float(signal["sl"])) * float(plan.get("qty") or 0)
            plan["risk_budget_usd"] = max(0.0, live_stop_risk)
            plan["estimated_stop_risk_usd"] = max(0.0, live_stop_risk)
            if float(plan.get("leverage") or 0) > 0 and float(plan.get("notional") or 0) > 0:
                margin_budget_live = float(plan.get("notional") or 0) / (float(plan.get("leverage") or 1) * 0.98)
            else:
                margin_budget_live = float(plan.get("margin_budget_usd") or 0)
            plan["margin_budget_usd"] = max(0.0, margin_budget_live)
            balance_available = float(plan.get("balance_available") or 0)
            plan["margin_usage_pct"] = (margin_budget_live / balance_available) if balance_available > 0 and margin_budget_live > 0 else 0.0

            position_id = str(position.get("positionId") or "")
            if not position_id:
                raise BitunixTradeError("Bitunix positionId missing after entry.")

            liq_info = self._evaluate_liquidation_safety(position, signal, plan)
            plan["liq_price"] = liq_info.get("liq_price")
            plan["liq_safe"] = liq_info.get("safe")
            plan["liq_reason"] = liq_info.get("reason")
            plan["liq_buffer_price"] = liq_info.get("buffer_price")
            if BITUNIX_LIQUIDATION_SAFETY_ENABLED and not liq_info.get("safe", True):
                try:
                    self.client.flash_close_position(position_id)
                except Exception:
                    pass
                raise BitunixTradeError(f"Liquidation safety failed: {liq_info.get('reason', 'unsafe liq distance')}")

            protection_warnings: List[str] = []
            if plan.get("tp_split_warning"):
                protection_warnings.append(str(plan.get("tp_split_warning")))
            sl_order = self._place_initial_stop_or_close(
                symbol=symbol,
                position_id=position_id,
                signal=signal,
                plan=plan,
                signal_id=signal_id,
            )

            tp_orders = []
            missing_tp_indices = []
            active_tp_indices = [i + 1 for i, q in enumerate(plan["tp_qtys"]) if float(q or 0) > 0]
            single_tp_index = active_tp_indices[0] if len(active_tp_indices) == 1 else None
            for idx, (qty_part, tp_price) in enumerate(zip(plan["tp_qtys"], [signal["tp1"], signal["tp2"], signal["tp3"]]), start=1):
                if qty_part <= 0:
                    continue
                tp_record, tp_warning = self._place_take_profit_order(
                    symbol=symbol,
                    position_id=position_id,
                    exit_side=exit_side,
                    qty_part=float(qty_part),
                    tp_price=float(tp_price),
                    signal_id=signal_id,
                    tp_index=idx,
                    plan=plan,
                    use_position_tp=(single_tp_index == idx),
                    current_sl_price=float(signal["sl"]),
                )
                if tp_warning:
                    protection_warnings.append(tp_warning)
                if tp_record is None:
                    missing_tp_indices.append(idx)
                    protection_warnings.append(f"TP{idx} could not be pre-placed; runtime fallback close will be used.")
                else:
                    tp_orders.append(tp_record)
            signal["execution"] = {
                "signal_id": signal_id,
                "mode": self.mode,
                "symbol": symbol,
                "side": signal["side"],
                "position_id": position_id,
                "entry_order": entry_order_data,
                "tp_orders": tp_orders,
                "sl_order": sl_order,
                "missing_tp_indices": missing_tp_indices,
                "protection_warnings": protection_warnings,
                "protection_ready": bool(sl_order) and len(tp_orders) == len([q for q in plan["tp_qtys"] if q > 0]),
                "qty": plan["qty"],
                "tp_qtys": plan["tp_qtys"],
                "tp_targets": [float(signal["tp1"]), float(signal["tp2"]), float(signal["tp3"])],
                "tp_splits": list(plan.get("tp_splits") or get_tp_splits_for_tf(signal.get("tf"), str((signal.get("meta") or {}).get("strategy") or signal.get("strategy") or ""))),
                "leverage": plan["leverage"],
                "entry_order_type": "LIMIT",
                "entry_attempts": plan.get("entry_attempts") or [],
                "filled_entry_price": plan.get("filled_entry_price"),
                "risk_budget_usd": plan["risk_budget_usd"],
                "configured_risk_budget_usd": plan.get("configured_risk_budget_usd"),
                "balance_available": plan["balance_available"],
                "balance_total": plan.get("balance_total"),
                "margin_budget_usd": plan.get("margin_budget_usd"),
                "margin_usage_pct": plan.get("margin_usage_pct"),
                "min_deposit_usage_pct": plan.get("min_deposit_usage_pct"),
                "max_deposit_usage_pct": plan.get("max_deposit_usage_pct"),
                "estimated_stop_risk_usd": plan.get("estimated_stop_risk_usd"),
                "position_mode": plan.get("position_mode"),
                "liq_price": plan.get("liq_price"),
                "liq_safe": plan.get("liq_safe"),
                "liq_reason": plan.get("liq_reason"),
                "entry_side": plan.get("entry_side"),
                "entry_trade_side": plan.get("entry_trade_side"),
                "exit_side": plan.get("exit_side"),
                "exit_trade_side": plan.get("exit_trade_side"),
                "exit_reduce_only": plan.get("exit_reduce_only"),
                "risk_qty": plan.get("risk_qty"),
                "affordable_qty": plan.get("affordable_qty"),
                "affordable_notional": plan.get("affordable_notional"),
                "active": True,
            }
            message = "Live Bitunix execution placed with full protection."
            if protection_warnings:
                message = "Live Bitunix execution placed, but some TP legs are using runtime fallback."
            return ExecutionResult(self.mode, True, message, signal["execution"])
        except BitunixTradeError as e:
            setattr(e, "plan", plan)
            raise

    def sync_outcome(self, signal: Dict[str, Any], event_type: str) -> ExecutionResult:
        self._refresh_state()
        execution = (signal or {}).get("execution") or {}
        if not execution:
            return ExecutionResult(self.mode, False, "No exchange execution attached to signal.", {})
        if not execution.get("active"):
            return ExecutionResult(self.mode, True, "Execution already inactive.", execution)
        if self.mode != "live":
            if event_type in {"TP3", "SL", "ENTRY_CLOSE", "PROFIT_SL"}:
                execution["active"] = False
            return ExecutionResult(self.mode, True, f"Demo sync {event_type}.", execution)

        symbol = execution["symbol"]
        sl_order = execution.get("sl_order") or {}
        sl_order_id = sl_order.get("orderId") or sl_order.get("id")
        position_id = execution.get("position_id")

        if event_type == "TP1" and position_id:
            self._ensure_tp_leg_closed(signal, execution, 1)
            qtys = list(execution.get("tp_qtys") or [0.0, 0.0, 0.0])
            remaining_qty = max(0.0, float(execution.get("qty", 0) or 0) - float(qtys[0] or 0))
            if remaining_qty <= 1e-12:
                self._cancel_remaining_protection(execution)
                if sl_order_id:
                    try:
                        self.client.cancel_tpsl(symbol, str(sl_order_id))
                    except Exception:
                        pass
                execution["active"] = False
                execution["sl_moved_to"] = None
                return ExecutionResult(self.mode, True, "Take profit closed the full position.", execution)
            try:
                new_sl = float(
                    execution.get("filled_entry_price")
                    or signal.get("entry")
                    or 0
                )
            except Exception:
                new_sl = float(signal.get("entry") or 0)
            if new_sl <= 0:
                new_sl = float(signal.get("entry") or 0)
            self._update_position_stop(symbol, str(position_id), signal, execution, new_sl)
            signal["sl"] = new_sl
            execution["sl_moved_to"] = new_sl
            return ExecutionResult(self.mode, True, "Moved SL to entry after TP1.", execution)

        if event_type == "TP2" and position_id:
            self._ensure_tp_leg_closed(signal, execution, 2)
            qtys = list(execution.get("tp_qtys") or [0.0, 0.0, 0.0])
            remaining_qty = max(
                0.0,
                float(execution.get("qty", 0) or 0) - float(qtys[0] or 0) - float(qtys[1] or 0),
            )
            if remaining_qty <= 1e-12:
                self._cancel_remaining_protection(execution)
                if sl_order_id:
                    try:
                        self.client.cancel_tpsl(symbol, str(sl_order_id))
                    except Exception:
                        pass
                execution["active"] = False
                execution["sl_moved_to"] = None
                return ExecutionResult(self.mode, True, "Take profit closed the full position.", execution)
            new_sl = self._breakeven_lock_price(signal, execution)
            self._update_position_stop(symbol, str(position_id), signal, execution, new_sl)
            signal["sl"] = new_sl
            execution["sl_moved_to"] = new_sl
            return ExecutionResult(self.mode, True, "Moved SL to protected breakeven after TP2.", execution)

        if event_type in {"TP3", "SL", "ENTRY_CLOSE", "PROFIT_SL"}:
            if event_type == "TP3":
                self._ensure_tp_leg_closed(signal, execution, 3)
            self._cancel_remaining_protection(execution)
            if sl_order_id:
                try:
                    self.client.cancel_tpsl(symbol, str(sl_order_id))
                except Exception:
                    pass
            execution["active"] = False
            return ExecutionResult(self.mode, True, f"Closed exchange execution on {event_type}.", execution)

        return ExecutionResult(self.mode, True, f"No exchange action for {event_type}.", execution)

    def _build_plan(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        entry = float(signal["entry"])
        sl = float(signal["sl"])
        risk_per_unit = abs(entry - sl)
        if risk_per_unit <= 0:
            raise BitunixTradeError("Signal SL equals entry; risk is zero.")

        balance_data = self._safe_get_balance()
        balance_available = float(balance_data.get("available", 0) or 0)
        balance_total = float(balance_data.get("total", 0) or 0)
        raw_account = balance_data.get("raw") or {}
        position_mode = str(raw_account.get("positionMode") or BITUNIX_POSITION_MODE or "ONE_WAY").strip().upper()
        if position_mode not in {"ONE_WAY", "HEDGE"}:
            position_mode = str(BITUNIX_POSITION_MODE or "ONE_WAY").strip().upper()
        tf_name = str(signal.get("tf") or signal.get("execution_tf") or "5m")
        symbol = str(signal.get("symbol") or (signal.get("meta") or {}).get("symbol") or SYMBOL).upper()
        symbol_rules = self._get_symbol_rules(symbol)
        min_base_qty = float(symbol_rules.get("min_base_qty") or BITUNIX_MIN_BASE_QTY)
        qty_step = float(symbol_rules.get("qty_step") or BITUNIX_QTY_STEP)
        desired_leverage = int(BITUNIX_DEFAULT_LEVERAGE or 1)
        meta = signal.get("meta") or {}
        manual_margin_usd = meta.get("manual_margin_usd")
        manual_leverage = meta.get("manual_leverage")
        try:
            manual_margin_usd = float(manual_margin_usd) if manual_margin_usd is not None else None
        except Exception:
            manual_margin_usd = None
        try:
            manual_leverage = int(float(manual_leverage)) if manual_leverage is not None else None
        except Exception:
            manual_leverage = None
        if manual_leverage is not None and manual_leverage > 0:
            desired_leverage = int(manual_leverage)
        leverage = int(raw_account.get("leverage") or desired_leverage or 1)
        margin_mode = "ISOLATION"
        if self.client.is_configured():
            try:
                lev_mode = self.client.get_leverage_margin_mode(symbol, BITUNIX_MARGIN_COIN).get("data", {}) or {}
                leverage = int(lev_mode.get("leverage") or leverage or 1)
                margin_mode = str(lev_mode.get("marginMode") or margin_mode).strip().upper()
            except Exception:
                margin_mode = str(raw_account.get("marginMode") or margin_mode).strip().upper()
        tf_max_leverage = int(BITUNIX_LIQUIDATION_MAX_LEVERAGE_BY_TF.get(tf_name, leverage) or leverage)
        target_leverage = max(1, min(desired_leverage, tf_max_leverage))
        leverage = target_leverage
        signal_size_pct = meta.get("size", signal.get("size", 0))
        try:
            signal_size_pct = float(signal_size_pct or 0)
        except Exception:
            signal_size_pct = 0.0
        min_deposit_usage_pct = max(0.0, float(BITUNIX_MIN_DEPOSIT_USAGE_PCT or 0.0))
        max_deposit_usage_pct = max(min_deposit_usage_pct, min(1.0, float(BITUNIX_MAX_DEPOSIT_USAGE_PCT or 1.0)))
        deposit_band_enabled = min_deposit_usage_pct > 0 or max_deposit_usage_pct < 0.999999
        auto_usage_quality = self._auto_usage_quality(signal)
        auto_margin_usage_pct = (
            min_deposit_usage_pct + (auto_usage_quality * max(0.0, max_deposit_usage_pct - min_deposit_usage_pct))
        ) if deposit_band_enabled else 0.0
        if deposit_band_enabled:
            auto_margin_usage_pct = self._clamp(auto_margin_usage_pct, min_deposit_usage_pct, max_deposit_usage_pct)
        fallback_risk_cap_pct = float(BITUNIX_RISK_CAP_PCT or 0.0)
        risk_from_balance = balance_available * fallback_risk_cap_pct
        configured_risk_budget = min(BITUNIX_MAX_RISK_USD, risk_from_balance) if balance_available > 0 else 0.0
        risk_qty = (configured_risk_budget / risk_per_unit) if configured_risk_budget > 0 else 0.0
        min_margin_budget = (balance_available * min_deposit_usage_pct) if balance_available > 0 else 0.0
        max_margin_budget = (balance_available * max_deposit_usage_pct) if balance_available > 0 else 0.0
        risk_margin_budget = ((risk_qty * entry) / leverage) if risk_qty > 0 and leverage > 0 else 0.0
        margin_budget = risk_margin_budget
        if manual_margin_usd is not None and manual_margin_usd > 0:
            margin_budget = float(manual_margin_usd)
        elif deposit_band_enabled and balance_available > 0:
            margin_budget = balance_available * auto_margin_usage_pct
        elif risk_margin_budget <= 0 and min_margin_budget > 0:
            margin_budget = min_margin_budget
        if balance_available <= 0:
            margin_budget = 0.0
        if balance_available > 0:
            margin_budget = max(0.0, float(margin_budget or 0.0))
            if max_margin_budget > 0:
                margin_budget = min(max_margin_budget, margin_budget)
            if min_margin_budget > 0:
                margin_budget = max(min_margin_budget, margin_budget)
            margin_budget = min(balance_available, margin_budget)
        affordable_notional = max(0.0, margin_budget * leverage * 0.98)
        affordable_qty = (affordable_notional / entry) if entry > 0 else 0.0
        qty = affordable_qty if affordable_qty > 0 else 0.0
        if (
            qty > 0
            and balance_available >= BITUNIX_MIN_NOTIONAL_USD
            and affordable_notional >= BITUNIX_MIN_NOTIONAL_USD
            and qty * entry < BITUNIX_MIN_NOTIONAL_USD
        ):
            qty = min(BITUNIX_MIN_NOTIONAL_USD / entry, affordable_qty)
        qty = self._round_qty_down(qty, qty_step)
        if qty < min_base_qty:
            qty = min_base_qty if affordable_qty >= min_base_qty else 0.0
        qty = self._round_qty_down(qty, qty_step)
        estimated_stop_risk_usd = max(0.0, qty * risk_per_unit)
        margin_usage_pct = (margin_budget / balance_available) if balance_available > 0 and margin_budget > 0 else 0.0
        effective_risk_cap_pct = (estimated_stop_risk_usd / balance_available) if balance_available > 0 and estimated_stop_risk_usd > 0 else 0.0
        strategy_name = str(
            signal.get("strategy")
            or (signal.get("meta") or {}).get("strategy")
            or ""
        ).upper()
        tp_splits = self._adaptive_tp_splits_for_signal(
            signal,
            tuple(get_tp_splits_for_tf(tf_name, strategy_name)),
        )
        tp_qtys, tp_split_warning = self._split_qty(
            qty,
            min_base_qty=min_base_qty,
            step=qty_step,
            splits=tp_splits,
        )
        is_hedge = position_mode == "HEDGE"
        entry_side = "BUY" if signal["side"] == "LONG" else "SELL"
        exit_side = entry_side if is_hedge else ("SELL" if signal["side"] == "LONG" else "BUY")
        pre_liq = self._estimate_pretrade_liquidation(
            symbol=symbol,
            side=str(signal["side"]).upper(),
            entry=entry,
            sl=sl,
            notional=qty * entry,
            leverage=leverage,
            margin_mode=margin_mode,
        )

        return {
            "symbol": symbol,
            "signal_type": str(signal.get("type", "SCALP")).upper(),
            "side": signal["side"],
            "entry": entry,
            "sl": sl,
            "tp1": float(signal["tp1"]),
            "tp2": float(signal["tp2"]),
            "tp3": float(signal["tp3"]),
            "entry_side": entry_side,
            "entry_trade_side": "OPEN" if is_hedge else None,
            "exit_side": exit_side,
            "exit_trade_side": "CLOSE" if is_hedge else None,
            "exit_reduce_only": None if is_hedge else True,
            "qty": qty,
            "tp_qtys": tp_qtys,
            "tp_split_warning": tp_split_warning,
            "tp_splits": list(tp_splits),
            "leverage": leverage,
            "current_exchange_leverage": int(raw_account.get("leverage") or 0),
            "target_leverage": target_leverage,
            "risk_budget_usd": estimated_stop_risk_usd,
            "configured_risk_budget_usd": configured_risk_budget,
            "risk_cap_pct": effective_risk_cap_pct,
            "fallback_risk_cap_pct": fallback_risk_cap_pct,
            "signal_size_pct": signal_size_pct,
            "manual_margin_usd": manual_margin_usd,
            "manual_leverage": manual_leverage,
            "risk_qty": risk_qty,
            "affordable_qty": affordable_qty,
            "affordable_notional": affordable_notional,
            "balance_available": balance_available,
            "balance_total": balance_total,
            "margin_budget_usd": margin_budget,
            "margin_usage_pct": margin_usage_pct,
            "auto_margin_usage_pct": auto_margin_usage_pct,
            "auto_usage_quality": auto_usage_quality,
            "deposit_band_enabled": deposit_band_enabled,
            "min_deposit_usage_pct": min_deposit_usage_pct,
            "max_deposit_usage_pct": max_deposit_usage_pct,
            "estimated_stop_risk_usd": estimated_stop_risk_usd,
            "position_mode": position_mode,
            "margin_mode": margin_mode,
            "required_margin_mode": BITUNIX_REQUIRED_MARGIN_MODE,
            "tf": tf_name,
            "notional": qty * entry,
            "symbol_rules": symbol_rules,
            "pre_liq_estimate": pre_liq.get("liq_price"),
            "pre_liq_safe": pre_liq.get("safe"),
            "pre_liq_reason": pre_liq.get("reason"),
            "maintenance_margin_rate": pre_liq.get("maintenance_margin_rate"),
        }

    def _safe_get_balance(self) -> Dict[str, Any]:
        if self.mode == "demo":
            available = max(BITUNIX_MIN_NOTIONAL_USD * 5, BITUNIX_MAX_RISK_USD * 10)
            return {"available": available, "total": available, "used": 0.0}
        if not self.client.is_configured():
            return {"available": 0.0, "total": 0.0, "used": 0.0}
        try:
            raw = self.client.get_single_account(BITUNIX_MARGIN_COIN).get("data", {})
            if isinstance(raw, list):
                raw = raw[0] if raw else {}
            available = (
                raw.get("available")
                or raw.get("availableBalance")
                or raw.get("canTransfer")
                or raw.get("marginBalance")
                or 0
            )
            total = (
                raw.get("marginBalance")
                or raw.get("accountEquity")
                or raw.get("equity")
                or raw.get("walletBalance")
                or raw.get("balance")
                or available
                or 0
            )
            position_margin = (
                raw.get("positionMargin")
                or raw.get("maintMargin")
                or raw.get("usedMargin")
                or raw.get("frozenMargin")
                or 0
            )
            available_f = float(available or 0)
            total_f = float(total or 0)
            used_f = float(position_margin or 0)
            if used_f <= 0 and total_f > 0:
                used_f = max(0.0, total_f - available_f)
            return {"available": available_f, "total": total_f, "used": used_f, "raw": raw}
        except Exception as e:
            return {
                "available": 0.0,
                "total": 0.0,
                "used": 0.0,
                "error": str(e),
                "endpoint": getattr(e, "endpoint", "GET /api/v1/futures/account"),
                "response_text": getattr(e, "response_text", None),
            }

    def _find_position(self, symbol: str, side: str) -> Optional[Dict[str, Any]]:
        return self._find_position_with_retry(symbol, side, retries=1, delay_sec=0.0)

    def _find_position_with_retry(self, symbol: str, side: str, retries: int = 6, delay_sec: float = 0.5) -> Optional[Dict[str, Any]]:
        target = side.upper()
        attempts = max(1, int(retries))
        for attempt in range(attempts):
            positions = self._pending_positions_list(symbol)
            pos = self._match_position_side(positions, target)
            if pos:
                return pos
            if attempt < attempts - 1 and delay_sec > 0:
                time.sleep(delay_sec)
        return None

    def _pending_positions_list(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        data = self.client.get_pending_positions(symbol).get("data", [])
        if isinstance(data, dict):
            data = data.get("positionList") or data.get("data") or []
        return list(data or [])

    @staticmethod
    def _position_qty(pos: Dict[str, Any]) -> float:
        return float(pos.get("qty") or pos.get("positionQty") or 0)

    @staticmethod
    def _position_sort_key(pos: Dict[str, Any]):
        ts = pos.get("mtime") or pos.get("utime") or pos.get("ctime") or pos.get("openTime") or 0
        try:
            ts_val = float(ts)
        except Exception:
            ts_val = 0.0
        return (ts_val, abs(float(pos.get("qty") or pos.get("positionQty") or 0) or 0))

    @staticmethod
    def _clamp(value: float, lower: float, upper: float) -> float:
        return max(float(lower), min(float(upper), float(value)))

    def _auto_usage_quality(self, signal: Dict[str, Any]) -> float:
        signal = signal or {}
        meta = signal.get("meta") or {}
        signal_type = str(signal.get("type") or "SCALP").strip().upper()
        strategy = str(meta.get("strategy") or signal.get("strategy") or "").strip().upper()
        base = {
            "SCALP": 0.46,
            "STRONG": 0.68,
            "EXTREME": 0.84,
        }.get(signal_type, 0.52)
        if strategy == "SMART_HEDGE":
            base = min(base, 0.34)
        elif strategy == "SMART_MONEY_LIQUIDITY":
            base = max(base, 0.74)
        elif strategy == "SCENARIO_PLAN":
            base = max(base, 0.58)

        score_val = 0.0
        try:
            raw_score = float(meta.get("score") or 0)
            if raw_score > 0:
                score_val = self._clamp(raw_score / 10.0, 0.0, 1.0)
        except Exception:
            score_val = 0.0

        prob_val = 0.0
        try:
            raw_prob = float(meta.get("scenario_probability") or meta.get("probability") or 0)
            if raw_prob > 0:
                prob_val = self._clamp((raw_prob - 45.0) / 35.0, 0.0, 1.0)
        except Exception:
            prob_val = 0.0

        size_hint_val = 0.0
        try:
            raw_size = float(meta.get("size") or signal.get("signal_size_pct") or 0)
            if raw_size > 0:
                size_hint_val = self._clamp((raw_size - 4.0) / 6.0, 0.0, 1.0)
        except Exception:
            size_hint_val = 0.0

        quality = (base * 0.55) + (score_val * 0.20) + (prob_val * 0.20) + (size_hint_val * 0.05)
        return self._clamp(quality, 0.20, 1.0)

    def _match_position_side(self, positions: List[Dict[str, Any]], target_side: str) -> Optional[Dict[str, Any]]:
        target = str(target_side or "").upper()
        candidates = []
        for pos in positions or []:
            qty = self._position_qty(pos)
            if qty <= 0:
                continue
            pos_side = str(pos.get("side") or pos.get("positionSide") or "").upper()
            if pos_side == target:
                candidates.append(pos)
        if candidates:
            candidates.sort(key=self._position_sort_key, reverse=True)
            return candidates[0]
        return None

    def _find_position_from_entry(
        self,
        *,
        symbol: str,
        side: str,
        order_id: Optional[str] = None,
        client_id: Optional[str] = None,
        retries: int = 20,
        delay_sec: float = 0.75,
    ) -> Optional[Dict[str, Any]]:
        target = str(side or "").upper()
        latest_symbol_position: Optional[Dict[str, Any]] = None
        attempts = max(1, int(retries))

        for attempt in range(attempts):
            positions = self._pending_positions_list(symbol)
            matched = self._match_position_side(positions, target)
            if matched:
                return matched

            active_positions = [pos for pos in positions if self._position_qty(pos) > 0]
            if active_positions:
                active_positions.sort(key=self._position_sort_key, reverse=True)
                latest_symbol_position = active_positions[0]
                if len(active_positions) == 1:
                    return latest_symbol_position

            order_detail = self._safe_get_order_detail(order_id=order_id, client_id=client_id)
            status = str((order_detail or {}).get("status") or "").upper()
            trade_qty = float((order_detail or {}).get("tradeQty") or 0)
            if trade_qty > 0 and latest_symbol_position is not None:
                return latest_symbol_position

            if attempt < attempts - 1 and delay_sec > 0:
                time.sleep(delay_sec)

        return latest_symbol_position

    @staticmethod
    def _price_step(symbol_rules: Dict[str, Any]) -> float:
        quote_precision = int((symbol_rules or {}).get("quote_precision") or 0)
        if quote_precision > 0:
            return float(f"1e-{quote_precision}")
        return 1.0

    @staticmethod
    def _round_price(price: float, step: float, *, side: str) -> float:
        price_val = max(0.0, float(price or 0))
        step_val = max(float(step or 0), 1e-9)
        if str(side or "").upper() == "BUY":
            return max(step_val, math.ceil((price_val / step_val) - 1e-12) * step_val)
        return max(step_val, math.floor((price_val / step_val) + 1e-12) * step_val)

    def _live_entry_book(self, symbol: str, fallback_price: float) -> Dict[str, float]:
        best_bid = 0.0
        best_ask = 0.0
        last_price = 0.0

        try:
            depth = self.client.get_depth(symbol, "20").get("data", {}) or {}
            bids = depth.get("bids") or []
            asks = depth.get("asks") or []
            if bids:
                best_bid = float((bids[0] or [0])[0] or 0)
            if asks:
                best_ask = float((asks[0] or [0])[0] or 0)
        except Exception:
            pass

        try:
            tickers = self.client.get_tickers(symbol).get("data", []) or []
            if isinstance(tickers, dict):
                tickers = tickers.get("list") or tickers.get("data") or []
            ticker = tickers[0] if tickers else {}
            last_price = float(ticker.get("lastPrice") or ticker.get("last") or 0)
            if best_bid <= 0:
                best_bid = float(ticker.get("bidPrice") or ticker.get("bid") or last_price or 0)
            if best_ask <= 0:
                best_ask = float(ticker.get("askPrice") or ticker.get("ask") or last_price or 0)
        except Exception:
            pass

        if best_bid <= 0:
            best_bid = float(last_price or fallback_price or 0)
        if best_ask <= 0:
            best_ask = float(last_price or fallback_price or 0)
        if last_price <= 0:
            last_price = float(best_ask or best_bid or fallback_price or 0)

        return {
            "best_bid": float(best_bid or 0),
            "best_ask": float(best_ask or 0),
            "last_price": float(last_price or 0),
        }

    def _marketable_limit_price(
        self,
        *,
        entry_side: str,
        fallback_price: float,
        symbol_rules: Dict[str, Any],
        attempt: int,
        book: Dict[str, float],
    ) -> float:
        side = str(entry_side or "").upper()
        price_step = self._price_step(symbol_rules)
        buffer_bps = LIMIT_ENTRY_REPRICE_BPS[min(max(0, int(attempt)), len(LIMIT_ENTRY_REPRICE_BPS) - 1)]
        best_bid = float((book or {}).get("best_bid") or 0)
        best_ask = float((book or {}).get("best_ask") or 0)
        fallback = float(fallback_price or 0)

        if side == "BUY":
            reference = best_ask or best_bid or fallback
            if reference <= 0:
                raise BitunixTradeError("Unable to determine a live Bitunix ask price for limit entry.")
            raw_price = reference * (1.0 + buffer_bps / 10000.0)
        else:
            reference = best_bid or best_ask or fallback
            if reference <= 0:
                raise BitunixTradeError("Unable to determine a live Bitunix bid price for limit entry.")
            raw_price = reference * (1.0 - buffer_bps / 10000.0)

        return self._round_price(raw_price, price_step, side=side)

    def _cancel_entry_order(self, symbol: str, order_id: Optional[str]) -> None:
        if not order_id:
            return
        try:
            self.client.cancel_orders(symbol, [str(order_id)])
        except Exception:
            pass

    def _apply_filled_position_to_plan(self, plan: Dict[str, Any], position: Dict[str, Any]) -> None:
        actual_qty = self._round_qty_down(
            self._position_qty(position),
            float(plan.get("symbol_rules", {}).get("qty_step") or BITUNIX_QTY_STEP),
        )
        if actual_qty <= 0:
            return
        tp_qtys, tp_split_warning = self._split_qty(
            actual_qty,
            min_base_qty=float(plan.get("symbol_rules", {}).get("min_base_qty") or BITUNIX_MIN_BASE_QTY),
            step=float(plan.get("symbol_rules", {}).get("qty_step") or BITUNIX_QTY_STEP),
            splits=plan.get("tp_splits"),
        )
        if tp_split_warning:
            existing = str(plan.get("tp_split_warning") or "").strip()
            plan["tp_split_warning"] = tp_split_warning if not existing else f"{existing} | {tp_split_warning}"
        avg_entry = (
            position.get("avgOpenPrice")
            or position.get("avgPrice")
            or position.get("entryPrice")
            or position.get("openPrice")
            or plan.get("entry")
        )
        plan["qty"] = float(actual_qty)
        plan["tp_qtys"] = tp_qtys
        plan["filled_entry_price"] = float(avg_entry or plan.get("entry") or 0)
        plan["notional"] = float(actual_qty) * float(plan.get("filled_entry_price") or plan.get("entry") or 0)

    def _place_limit_entry_until_filled(
        self,
        *,
        plan: Dict[str, Any],
        signal: Dict[str, Any],
        signal_id: str,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        symbol = str(plan["symbol"])
        entry_side = str(plan["entry_side"]).upper()
        signal_side = str(signal["side"]).upper()
        symbol_rules = plan.get("symbol_rules") or self._get_symbol_rules(symbol)
        qty_step = float(symbol_rules.get("qty_step") or BITUNIX_QTY_STEP)
        min_base_qty = float(symbol_rules.get("min_base_qty") or BITUNIX_MIN_BASE_QTY)
        target_qty = self._round_qty_down(float(plan["qty"]), qty_step)
        fallback_price = float(plan.get("entry") or 0)
        full_fill_tolerance_qty = max(qty_step, target_qty * LIMIT_ENTRY_FULL_FILL_TOLERANCE_PCT)

        latest_order_data: Dict[str, Any] = {}
        latest_position: Dict[str, Any] = {}
        latest_order_detail: Dict[str, Any] = {}
        attempts_meta: List[Dict[str, Any]] = []
        remaining_qty = target_qty

        for attempt in range(LIMIT_ENTRY_MAX_RETRIES):
            if remaining_qty < min_base_qty:
                break
            book = self._live_entry_book(symbol, fallback_price)
            limit_price = self._marketable_limit_price(
                entry_side=entry_side,
                fallback_price=fallback_price,
                symbol_rules=symbol_rules,
                attempt=attempt,
                book=book,
            )
            client_id = f"{signal_id}-entry-{attempt + 1}"
            entry_order = self.client.place_order(
                symbol=symbol,
                side=entry_side,
                qty=remaining_qty,
                order_type="LIMIT",
                price=limit_price,
                reduce_only=None,
                client_id=client_id,
                effect="GTC",
                trade_side=plan.get("entry_trade_side"),
            )
            entry_order_data = self._data_dict(entry_order)
            entry_order_id = entry_order_data.get("orderId") or entry_order_data.get("id")
            latest_order_data = entry_order_data or latest_order_data

            attempt_meta: Dict[str, Any] = {
                "attempt": attempt + 1,
                "price": float(limit_price),
                "qty": float(remaining_qty),
                "best_bid": float(book.get("best_bid") or 0),
                "best_ask": float(book.get("best_ask") or 0),
                "order_id": str(entry_order_id or ""),
                "client_id": client_id,
            }

            for _ in range(LIMIT_ENTRY_POLL_RETRIES):
                matched = self._find_position_from_entry(
                    symbol=symbol,
                    side=signal_side,
                    order_id=entry_order_id,
                    client_id=client_id,
                    retries=1,
                    delay_sec=0.0,
                )
                if matched:
                    latest_position = matched
                order_detail = self._safe_get_order_detail(order_id=entry_order_id, client_id=client_id)
                if order_detail:
                    latest_order_detail = order_detail
                filled_qty = self._round_qty_down(self._position_qty(latest_position), qty_step) if latest_position else 0.0
                trade_qty = float((order_detail or {}).get("tradeQty") or 0)
                status = str((order_detail or {}).get("status") or "").upper()
                attempt_meta["status"] = status
                attempt_meta["trade_qty"] = float(trade_qty)
                attempt_meta["filled_qty"] = float(filled_qty)
                if filled_qty >= max(min_base_qty, target_qty - full_fill_tolerance_qty):
                    attempt_meta["outcome"] = "filled"
                    attempts_meta.append(attempt_meta)
                    plan["entry_attempts"] = attempts_meta
                    self._cancel_entry_order(symbol, entry_order_id)
                    return latest_order_data, latest_position
                if status in {"FILLED", "FULLY_FILLED"} and (filled_qty > 0 or trade_qty > 0):
                    attempt_meta["outcome"] = "filled"
                    attempts_meta.append(attempt_meta)
                    plan["entry_attempts"] = attempts_meta
                    self._cancel_entry_order(symbol, entry_order_id)
                    if latest_position:
                        return latest_order_data, latest_position
                    break
                time.sleep(LIMIT_ENTRY_POLL_DELAY_SEC)

            self._cancel_entry_order(symbol, entry_order_id)
            latest_position = self._find_position_with_retry(symbol, signal_side, retries=2, delay_sec=0.25) or latest_position
            filled_qty = self._round_qty_down(self._position_qty(latest_position), qty_step) if latest_position else 0.0
            attempt_meta["filled_qty"] = float(filled_qty)
            if filled_qty >= max(min_base_qty, target_qty - full_fill_tolerance_qty):
                attempt_meta["outcome"] = "filled"
                attempts_meta.append(attempt_meta)
                plan["entry_attempts"] = attempts_meta
                return latest_order_data, latest_position
            if filled_qty > 0:
                remaining_qty = self._round_qty_down(max(0.0, target_qty - filled_qty), qty_step)
                attempt_meta["outcome"] = "partial"
            else:
                remaining_qty = target_qty
                attempt_meta["outcome"] = "reprice"
            attempts_meta.append(attempt_meta)

        plan["entry_attempts"] = attempts_meta
        if latest_order_detail:
            plan["entry_status"] = latest_order_detail.get("status")
            plan["entry_trade_qty"] = latest_order_detail.get("tradeQty")
        if latest_position and self._position_qty(latest_position) >= min_base_qty:
            plan["entry_attempts"] = attempts_meta
            return latest_order_data, latest_position
        raise BitunixTradeError("Limit entry was not filled after multiple reprices.")

    def _safe_get_order_detail(self, order_id: Optional[str] = None, client_id: Optional[str] = None) -> Dict[str, Any]:
        if not order_id and not client_id:
            return {}
        try:
            data = self.client.get_order_detail(order_id=order_id, client_id=client_id).get("data", {}) or {}
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _place_initial_stop_or_close(
        self,
        *,
        symbol: str,
        position_id: str,
        signal: Dict[str, Any],
        plan: Dict[str, Any],
        signal_id: str,
    ) -> Dict[str, Any]:
        last_error: Optional[BitunixTradeError] = None
        for _ in range(4):
            try:
                sl_res = self.client.place_position_tpsl(symbol, position_id, None, float(signal["sl"]))
                sl_order = self._data_dict(sl_res)
                if sl_order.get("orderId") or sl_order.get("id"):
                    return sl_order
            except BitunixTradeError as e:
                last_error = e
                time.sleep(0.35)

        # If we cannot secure a stop, do not leave the position naked.
        try:
            self.client.place_order(
                symbol=symbol,
                side=plan["exit_side"],
                qty=float(plan["qty"]),
                order_type="MARKET",
                reduce_only=plan.get("exit_reduce_only"),
                client_id=f"{signal_id}-panic-close",
                trade_side=plan.get("exit_trade_side"),
                position_id=position_id if plan.get("exit_trade_side") else None,
            )
        except Exception:
            pass

        if last_error is not None:
            raise last_error
        raise BitunixTradeError("Failed to place initial Bitunix stop-loss protection.")

    def _place_take_profit_order(
        self,
        *,
        symbol: str,
        position_id: str,
        exit_side: str,
        qty_part: float,
        tp_price: float,
        signal_id: str,
        tp_index: int,
        plan: Dict[str, Any],
        use_position_tp: bool = False,
        current_sl_price: Optional[float] = None,
    ) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        try:
            tp_res = self.client.place_order(
                symbol=symbol,
                side=exit_side,
                qty=float(qty_part),
                order_type="LIMIT",
                price=float(tp_price),
                reduce_only=plan.get("exit_reduce_only"),
                client_id=f"{signal_id}-tp{tp_index}",
                trade_side=plan.get("exit_trade_side"),
                position_id=position_id if plan.get("exit_trade_side") else None,
                effect="GTC",
            )
            data = self._data_dict(tp_res)
            return {
                "index": tp_index,
                "kind": "LIMIT_CLOSE",
                "qty": float(qty_part),
                "price": float(tp_price),
                "orderId": data.get("orderId") or data.get("id"),
                "clientId": data.get("clientId") or f"{signal_id}-tp{tp_index}",
                "raw": data,
            }, (
                "Compressed TP legs to one limit close order."
                if use_position_tp else None
            )
        except BitunixTradeError as limit_error:
            return None, f"TP{tp_index} limit close order failed ({limit_error})."

    def _ensure_tp_leg_closed(self, signal: Dict[str, Any], execution: Dict[str, Any], tp_index: int) -> None:
        executed = set(execution.get("executed_tp_indices") or [])
        if tp_index in executed:
            return
        existing = {int(o.get("index")) for o in (execution.get("tp_orders") or []) if o.get("index") is not None}
        if tp_index in existing:
            return

        qtys = execution.get("tp_qtys") or []
        if tp_index < 1 or tp_index > len(qtys):
            return
        qty = float(qtys[tp_index - 1] or 0)
        if qty <= 0:
            return

        self.client.place_order(
            symbol=execution["symbol"],
            side=execution["exit_side"],
            qty=qty,
            order_type="MARKET",
            reduce_only=execution.get("exit_reduce_only"),
            client_id=f"{execution.get('signal_id', new_signal_id())}-tp{tp_index}-fallback",
            trade_side=execution.get("exit_trade_side"),
            position_id=execution.get("position_id") if execution.get("exit_trade_side") else None,
        )
        execution.setdefault("executed_tp_indices", []).append(tp_index)

    def _cancel_remaining_protection(self, execution: Dict[str, Any]) -> None:
        symbol = execution.get("symbol")
        if not symbol:
            return

        tpsl_ids = []
        limit_ids = []
        for order in execution.get("tp_orders") or []:
            order_id = order.get("orderId") or order.get("id")
            if not order_id:
                continue
            if str(order.get("kind", "")).upper() in {"TPSL", "POSITION_TP"}:
                tpsl_ids.append(str(order_id))
            else:
                limit_ids.append(str(order_id))

        for order_id in tpsl_ids:
            try:
                self.client.cancel_tpsl(symbol, order_id)
            except Exception:
                pass
        if limit_ids:
            try:
                self.client.cancel_orders(symbol, limit_ids)
            except Exception:
                pass

    def rebuild_position_protection(self, signal: Dict[str, Any], *, reason: str = "position change") -> ExecutionResult:
        self._refresh_state()
        execution = (signal or {}).get("execution") or {}
        if not execution or not execution.get("active"):
            return ExecutionResult(self.mode, False, "No active exchange execution found for protection rebuild.", {})
        symbol = str(execution.get("symbol") or signal.get("symbol") or SYMBOL).upper()
        position_id = str(execution.get("position_id") or "").strip()
        if not symbol or not position_id:
            return ExecutionResult(self.mode, False, "Missing exchange position reference for rebuild.", execution)
        if self.mode != "live":
            return ExecutionResult(self.mode, False, "Protection rebuild is only available in live mode.", execution)

        live_position = None
        for pos in self._pending_positions_list(symbol):
            candidate_id = str(pos.get("positionId") or pos.get("id") or "").strip()
            if candidate_id == position_id and self._position_qty(pos) > 0:
                live_position = pos
                break
        if live_position is None:
            fallback = self._find_position(symbol, str(signal.get("side") or "").upper())
            if fallback and str(fallback.get("positionId") or fallback.get("id") or "").strip():
                live_position = fallback
                execution["position_id"] = str(fallback.get("positionId") or fallback.get("id") or "").strip()
                position_id = str(execution.get("position_id") or "").strip()
        if live_position is None:
            execution["active"] = False
            return ExecutionResult(self.mode, False, "Live Bitunix position was not found during protection rebuild.", execution)

        symbol_rules = self._get_symbol_rules(symbol)
        qty_step = float(symbol_rules.get("qty_step") or BITUNIX_QTY_STEP)
        min_base_qty = float(symbol_rules.get("min_base_qty") or BITUNIX_MIN_BASE_QTY)
        actual_qty = self._round_qty_down(self._position_qty(live_position), qty_step)
        if actual_qty <= 0:
            execution["active"] = False
            return ExecutionResult(self.mode, False, "Live Bitunix position quantity is zero during protection rebuild.", execution)

        avg_entry = (
            live_position.get("avgOpenPrice")
            or live_position.get("avgPrice")
            or live_position.get("entryPrice")
            or live_position.get("openPrice")
            or execution.get("filled_entry_price")
            or signal.get("entry")
        )
        try:
            avg_entry = float(avg_entry or 0)
        except Exception:
            avg_entry = float(signal.get("entry") or 0)
        execution["qty"] = float(actual_qty)
        execution["filled_entry_price"] = float(avg_entry or signal.get("entry") or 0)
        execution["entry"] = execution["filled_entry_price"]
        execution["notional"] = float(actual_qty) * float(execution["filled_entry_price"] or 0)

        base_splits = tuple(
            execution.get("tp_splits")
            or self._adaptive_tp_splits_for_signal(
                signal,
                tuple(get_tp_splits_for_tf(signal.get("tf"), str((signal.get("meta") or {}).get("strategy") or signal.get("strategy") or ""))),
            )
        )
        splits = tuple(base_splits)
        execution["tp_splits"] = list(splits)
        tp_targets = list(execution.get("tp_targets") or [signal.get("tp1"), signal.get("tp2"), signal.get("tp3")])
        while len(tp_targets) < 3:
            tp_targets.append(None)
        executed_indices = {
            int(idx)
            for idx in (execution.get("executed_tp_indices") or [])
            if int(idx or 0) in {1, 2, 3}
        }
        active_target_indices = [
            idx
            for idx in (1, 2, 3)
            if idx not in executed_indices and float(tp_targets[idx - 1] or 0) > 0
        ]
        rebuilt_tp_qtys, rebuild_warning = self._split_qty_for_indices(
            actual_qty,
            active_indices=active_target_indices,
            min_base_qty=min_base_qty,
            step=qty_step,
            splits=splits,
        )

        current_sl_price = self._current_sl_price(signal, execution)
        if current_sl_price is None or current_sl_price <= 0:
            current_sl_price = float(signal.get("sl") or 0)
        if current_sl_price > 0:
            live_stop_risk = abs(float(execution.get("filled_entry_price") or 0) - float(current_sl_price or 0)) * float(actual_qty)
            execution["risk_budget_usd"] = max(0.0, live_stop_risk)
            execution["estimated_stop_risk_usd"] = max(0.0, live_stop_risk)

        leverage = float(execution.get("leverage") or 0)
        balance_available = float(execution.get("balance_available") or 0)
        if leverage > 0 and float(execution.get("notional") or 0) > 0:
            margin_budget_live = float(execution.get("notional") or 0) / (leverage * 0.98)
            execution["margin_budget_usd"] = max(0.0, margin_budget_live)
            execution["margin_usage_pct"] = (margin_budget_live / balance_available) if balance_available > 0 and margin_budget_live > 0 else 0.0

        if not execution.get("sl_order") and current_sl_price > 0:
            try:
                sl_row = self._data_dict(self.client.place_position_tpsl(symbol, position_id, None, float(current_sl_price)))
                if sl_row.get("orderId") or sl_row.get("id"):
                    execution["sl_order"] = sl_row
            except Exception:
                pass

        self._cancel_remaining_protection(execution)
        execution["tp_qtys"] = rebuilt_tp_qtys
        execution["tp_orders"] = []
        execution["missing_tp_indices"] = []
        protection_warnings: List[str] = []
        if rebuild_warning:
            protection_warnings.append(rebuild_warning)

        plan = {
            "exit_reduce_only": execution.get("exit_reduce_only"),
            "exit_trade_side": execution.get("exit_trade_side"),
        }
        active_rebuilt_indices = [idx for idx, qty_part in enumerate(rebuilt_tp_qtys[:3], start=1) if float(qty_part or 0) > 0]
        single_tp_index = active_rebuilt_indices[0] if len(active_rebuilt_indices) == 1 else None
        for idx in active_rebuilt_indices:
            target_price = float(tp_targets[idx - 1] or signal.get(f"tp{idx}") or 0)
            if target_price <= 0:
                continue
            tp_record, tp_warning = self._place_take_profit_order(
                symbol=symbol,
                position_id=position_id,
                exit_side=execution.get("exit_side"),
                qty_part=float(rebuilt_tp_qtys[idx - 1]),
                tp_price=target_price,
                signal_id=execution.get("signal_id", new_signal_id()),
                tp_index=idx,
                plan=plan,
                use_position_tp=(single_tp_index == idx),
                current_sl_price=current_sl_price if current_sl_price and current_sl_price > 0 else None,
            )
            if tp_warning:
                protection_warnings.append(tp_warning)
            if tp_record is None:
                execution["missing_tp_indices"].append(idx)
            else:
                execution["tp_orders"].append(tp_record)
        execution["protection_warnings"] = protection_warnings
        execution["protection_ready"] = bool(execution.get("sl_order")) and len(execution.get("tp_orders") or []) == len(active_rebuilt_indices)
        return ExecutionResult(
            self.mode,
            True,
            f"Rebuilt Bitunix protection after {reason} with merged qty {actual_qty:.6f}.",
            execution,
        )

    def _cancel_tp_order_record(self, execution: Dict[str, Any], order: Dict[str, Any]) -> None:
        symbol = execution.get("symbol")
        if not symbol or not order:
            return
        order_id = order.get("orderId") or order.get("id")
        if not order_id:
            return
        kind = str(order.get("kind", "")).upper()
        if kind == "TPSL":
            self.client.cancel_tpsl(symbol, str(order_id))
        elif kind == "LIMIT":
            self.client.cancel_orders(symbol, [str(order_id)])

    def manual_move_stop(self, signal: Dict[str, Any], new_sl: float) -> ExecutionResult:
        self._refresh_state()
        execution = (signal or {}).get("execution") or {}
        if not execution or not execution.get("active"):
            return ExecutionResult(self.mode, False, "No active exchange execution found for this signal.", {})
        try:
            new_sl = float(new_sl)
        except Exception:
            return ExecutionResult(self.mode, False, "Stop price is invalid.", execution)
        if new_sl <= 0:
            return ExecutionResult(self.mode, False, "Stop price must be greater than 0.", execution)
        symbol = execution.get("symbol")
        position_id = execution.get("position_id")
        if not symbol or not position_id:
            return ExecutionResult(self.mode, False, "Missing exchange position reference.", execution)
        if self.mode != "live":
            signal["sl"] = new_sl
            execution["sl_moved_to"] = new_sl
            return ExecutionResult(self.mode, True, "Demo stop moved.", execution)
        self._update_position_stop(symbol, str(position_id), signal, execution, new_sl)
        signal["sl"] = new_sl
        execution["sl_moved_to"] = new_sl
        return ExecutionResult(self.mode, True, f"Moved SL to {new_sl:.2f}.", execution)

    def manual_move_stop_to_breakeven(self, signal: Dict[str, Any]) -> ExecutionResult:
        self._refresh_state()
        execution = (signal or {}).get("execution") or {}
        if not execution or not execution.get("active"):
            return ExecutionResult(self.mode, False, "No active exchange execution found for this signal.", {})
        new_sl = self._breakeven_lock_price(signal, execution)
        if new_sl <= 0:
            return ExecutionResult(self.mode, False, "Protected breakeven price is invalid.", execution)
        symbol = execution.get("symbol")
        position_id = execution.get("position_id")
        if not symbol or not position_id:
            return ExecutionResult(self.mode, False, "Missing exchange position reference.", execution)
        if self.mode != "live":
            signal["sl"] = new_sl
            execution["sl_moved_to"] = new_sl
            return ExecutionResult(self.mode, True, "Demo stop moved to protected breakeven.", execution)
        self._update_position_stop(symbol, str(position_id), signal, execution, new_sl)
        signal["sl"] = new_sl
        execution["sl_moved_to"] = new_sl
        return ExecutionResult(self.mode, True, f"Moved SL to protected breakeven at {new_sl:.2f}.", execution)

    def manual_set_tp(self, signal: Dict[str, Any], tp_index: int, new_price: float) -> ExecutionResult:
        self._refresh_state()
        execution = (signal or {}).get("execution") or {}
        if not execution or not execution.get("active"):
            return ExecutionResult(self.mode, False, "No active exchange execution found for this signal.", {})
        if tp_index not in {1, 2, 3}:
            return ExecutionResult(self.mode, False, "TP index must be 1, 2, or 3.", execution)
        try:
            new_price = float(new_price)
        except Exception:
            return ExecutionResult(self.mode, False, f"TP{tp_index} price is invalid.", execution)
        if new_price <= 0:
            return ExecutionResult(self.mode, False, f"TP{tp_index} price must be greater than 0.", execution)
        if signal.get(f"tp{tp_index}_hit"):
            return ExecutionResult(self.mode, False, f"TP{tp_index} is already marked as hit.", execution)
        qtys = list(execution.get("tp_qtys") or [0.0, 0.0, 0.0])
        if tp_index > len(qtys) or float(qtys[tp_index - 1] or 0) <= 0:
            return ExecutionResult(self.mode, False, f"TP{tp_index} has no active quantity left.", execution)

        symbol = execution.get("symbol")
        position_id = execution.get("position_id")
        if not symbol or not position_id:
            return ExecutionResult(self.mode, False, "Missing exchange position reference.", execution)

        tp_orders = list(execution.get("tp_orders") or [])
        existing = next((o for o in tp_orders if int(o.get("index", 0) or 0) == tp_index), None)
        if self.mode != "live":
            signal[f"tp{tp_index}"] = float(new_price)
            execution.setdefault("tp_targets", [signal.get("tp1"), signal.get("tp2"), signal.get("tp3")])[tp_index - 1] = float(new_price)
            return ExecutionResult(self.mode, True, f"Demo TP{tp_index} updated.", execution)

        if existing and str(existing.get("kind", "")).upper() == "POSITION_TP":
            current_sl_price = self._current_sl_price(signal, execution)
            self.client.modify_position_tpsl(symbol, str(position_id), float(new_price), current_sl_price)
            existing["price"] = float(new_price)
        else:
            if existing:
                self._cancel_tp_order_record(execution, existing)
            plan = {
                "exit_reduce_only": execution.get("exit_reduce_only"),
                "exit_trade_side": execution.get("exit_trade_side"),
            }
            active_indices = [i + 1 for i, q in enumerate(qtys) if float(q or 0) > 0]
            use_position_tp = len(active_indices) == 1 and active_indices[0] == tp_index
            new_order, warning = self._place_take_profit_order(
                symbol=symbol,
                position_id=str(position_id),
                exit_side=execution.get("exit_side"),
                qty_part=float(qtys[tp_index - 1]),
                tp_price=float(new_price),
                signal_id=execution.get("signal_id", new_signal_id()),
                tp_index=tp_index,
                plan=plan,
                use_position_tp=use_position_tp,
                current_sl_price=self._current_sl_price(signal, execution),
            )
            if new_order is None:
                return ExecutionResult(self.mode, False, warning or f"Failed to update TP{tp_index}.", execution)
            tp_orders = [o for o in tp_orders if int(o.get("index", 0) or 0) != tp_index]
            tp_orders.append(new_order)
            execution["tp_orders"] = tp_orders
            if warning:
                execution.setdefault("protection_warnings", []).append(warning)

        signal[f"tp{tp_index}"] = float(new_price)
        targets = list(execution.get("tp_targets") or [signal.get("tp1"), signal.get("tp2"), signal.get("tp3")])
        while len(targets) < 3:
            targets.append(None)
        targets[tp_index - 1] = float(new_price)
        execution["tp_targets"] = targets
        return ExecutionResult(self.mode, True, f"Updated TP{tp_index} to {float(new_price):.2f}.", execution)

    def manual_set_single_tp(self, signal: Dict[str, Any], new_price: float) -> ExecutionResult:
        self._refresh_state()
        execution = (signal or {}).get("execution") or {}
        if not execution or not execution.get("active"):
            return ExecutionResult(self.mode, False, "No active exchange execution found for this signal.", {})
        try:
            new_price = float(new_price)
        except Exception:
            return ExecutionResult(self.mode, False, "Take-profit price is invalid.", execution)
        if new_price <= 0:
            return ExecutionResult(self.mode, False, "Take-profit price must be greater than 0.", execution)

        symbol = execution.get("symbol")
        position_id = execution.get("position_id")
        total_qty = float(execution.get("qty", 0) or 0)
        if not symbol or not position_id:
            return ExecutionResult(self.mode, False, "Missing exchange position reference.", execution)
        if total_qty <= 0:
            return ExecutionResult(self.mode, False, "Position quantity is invalid.", execution)

        tp_orders = list(execution.get("tp_orders") or [])
        single_tp_index = self._preferred_single_tp_index(signal, execution)
        if self.mode == "live":
            for order in tp_orders:
                try:
                    self._cancel_tp_order_record(execution, order)
                except Exception:
                    pass
            current_sl_price = self._current_sl_price(signal, execution)
            try:
                tp_res = self.client.modify_position_tpsl(symbol, str(position_id), float(new_price), current_sl_price)
            except BitunixTradeError:
                tp_res = self.client.place_position_tpsl(symbol, str(position_id), float(new_price), current_sl_price)
            data = self._data_dict(tp_res)
            execution["tp_orders"] = [{
                "index": single_tp_index,
                "kind": "POSITION_TP",
                "qty": float(total_qty),
                "price": float(new_price),
                "orderId": data.get("orderId") or data.get("id"),
                "raw": data,
            }]
        else:
            execution["tp_orders"] = [{
                "index": single_tp_index,
                "kind": "POSITION_TP",
                "qty": float(total_qty),
                "price": float(new_price),
            }]

        qtys = [0.0, 0.0, 0.0]
        qtys[single_tp_index - 1] = float(total_qty)
        execution["tp_qtys"] = qtys
        targets = [None, None, None]
        targets[single_tp_index - 1] = float(new_price)
        execution["tp_targets"] = targets
        execution["missing_tp_indices"] = [i for i in (1, 2, 3) if i != single_tp_index]
        execution["tp_mode"] = "POSITION_TP"
        execution["protection_ready"] = bool(execution.get("sl_order"))
        signal[f"tp{single_tp_index}"] = float(new_price)
        return ExecutionResult(self.mode, True, f"Set take profit to {float(new_price):.2f}.", execution)

    def manual_set_tp_splits(self, signal: Dict[str, Any], splits: List[float]) -> ExecutionResult:
        self._refresh_state()
        execution = (signal or {}).get("execution") or {}
        if not execution or not execution.get("active"):
            return ExecutionResult(self.mode, False, "No active exchange execution found for this signal.", {})

        raw = [float(x or 0) for x in list(splits or [])[:3]]
        if len(raw) != 3 or any(x <= 0 for x in raw):
            return ExecutionResult(self.mode, False, "TP split needs three positive values, for example 20/30/50.", execution)
        total = sum(raw)
        if total <= 0:
            return ExecutionResult(self.mode, False, "TP split total must be greater than zero.", execution)

        normalized = [float(x / total) for x in raw]
        execution["tp_splits"] = normalized
        execution["tp_mode"] = "LIMIT_CLOSE"

        if self.mode != "live":
            qty = float(execution.get("qty") or 0)
            rules = self._get_symbol_rules(str(execution.get("symbol") or signal.get("symbol") or SYMBOL).upper())
            rebuilt_tp_qtys, warning = self._split_qty_for_indices(
                qty,
                active_indices=[1, 2, 3],
                min_base_qty=float(rules.get("min_base_qty") or BITUNIX_MIN_BASE_QTY),
                step=float(rules.get("qty_step") or BITUNIX_QTY_STEP),
                splits=tuple(normalized),
            )
            execution["tp_qtys"] = rebuilt_tp_qtys
            if warning:
                execution.setdefault("protection_warnings", []).append(warning)
            split_text = "/".join(str(int(round(x * 100))) for x in normalized)
            return ExecutionResult(self.mode, True, f"Demo TP split updated to {split_text}.", execution)

        result = self.rebuild_position_protection(signal, reason="manual TP split change")
        if result.payload:
            result.payload["tp_splits"] = normalized
        split_text = "/".join(str(int(round(x * 100))) for x in normalized)
        if result.accepted:
            result.message = f"Updated TP split to {split_text} and rebuilt remaining close orders."
        return result

    def manual_cancel_tp(self, signal: Dict[str, Any], tp_index: int) -> ExecutionResult:
        self._refresh_state()
        execution = (signal or {}).get("execution") or {}
        if not execution or not execution.get("active"):
            return ExecutionResult(self.mode, False, "No active exchange execution found for this signal.", {})
        if tp_index not in {1, 2, 3}:
            return ExecutionResult(self.mode, False, "TP index must be 1, 2, or 3.", execution)
        if signal.get(f"tp{tp_index}_hit"):
            return ExecutionResult(self.mode, False, f"TP{tp_index} is already marked as hit.", execution)

        qtys = list(execution.get("tp_qtys") or [0.0, 0.0, 0.0])
        if tp_index > len(qtys) or float(qtys[tp_index - 1] or 0) <= 0:
            return ExecutionResult(self.mode, False, f"TP{tp_index} is not active.", execution)

        tp_orders = list(execution.get("tp_orders") or [])
        existing = next((o for o in tp_orders if int(o.get("index", 0) or 0) == tp_index), None)
        if self.mode == "live" and existing:
            self._cancel_tp_order_record(execution, existing)

        execution["tp_orders"] = [o for o in tp_orders if int(o.get("index", 0) or 0) != tp_index]
        execution["tp_qtys"][tp_index - 1] = 0.0
        targets = list(execution.get("tp_targets") or [signal.get("tp1"), signal.get("tp2"), signal.get("tp3")])
        while len(targets) < 3:
            targets.append(None)
        execution["tp_targets"] = targets
        signal[f"tp{tp_index}"] = float(targets[tp_index - 1] or signal.get(f"tp{tp_index}") or 0)

        missing = set(int(i) for i in (execution.get("missing_tp_indices") or []))
        missing.discard(tp_index)
        execution["missing_tp_indices"] = sorted(missing)
        active_tp_count = len([q for q in execution.get("tp_qtys", []) if float(q or 0) > 0])
        execution["protection_ready"] = bool(execution.get("sl_order")) and len(execution.get("tp_orders") or []) == active_tp_count
        return ExecutionResult(self.mode, True, f"Cancelled TP{tp_index}.", execution)

    def manual_cancel_all_tps(self, signal: Dict[str, Any]) -> ExecutionResult:
        self._refresh_state()
        execution = (signal or {}).get("execution") or {}
        if not execution or not execution.get("active"):
            return ExecutionResult(self.mode, False, "No active exchange execution found for this signal.", {})

        tp_orders = list(execution.get("tp_orders") or [])
        qtys = list(execution.get("tp_qtys") or [0.0, 0.0, 0.0])
        if not tp_orders and not any(float(q or 0) > 0 for q in qtys[:3]):
            return ExecutionResult(self.mode, False, "No active take-profit orders were found.", execution)

        if self.mode == "live":
            for order in tp_orders:
                try:
                    self._cancel_tp_order_record(execution, order)
                except Exception:
                    pass

        execution["tp_orders"] = []
        execution["tp_qtys"] = [0.0, 0.0, 0.0]
        execution["missing_tp_indices"] = [1, 2, 3]
        execution["protection_ready"] = bool(execution.get("sl_order"))
        execution["tp_mode"] = "NONE"
        return ExecutionResult(self.mode, True, "Cancelled all take-profit orders.", execution)

    def manual_close_position(self, signal: Dict[str, Any], fraction: float = 1.0) -> ExecutionResult:
        self._refresh_state()
        execution = (signal or {}).get("execution") or {}
        if not execution or not execution.get("active"):
            return ExecutionResult(self.mode, False, "No active exchange execution found for this signal.", {})
        symbol = execution.get("symbol")
        position_id = execution.get("position_id")
        if not symbol or not position_id:
            return ExecutionResult(self.mode, False, "Missing exchange position reference.", execution)

        fraction = max(0.0, min(1.0, float(fraction or 0)))
        if fraction <= 0:
            return ExecutionResult(self.mode, False, "Close fraction must be greater than zero.", execution)

        if self.mode != "live":
            if fraction >= 0.999:
                execution["active"] = False
                signal["status"] = "CLOSED"
                signal["closed_at"] = datetime.now(timezone.utc).isoformat()
                return ExecutionResult(self.mode, True, "Demo full close completed.", execution)
            return ExecutionResult(self.mode, True, "Demo partial close recorded.", execution)

        rules = self._get_symbol_rules(symbol)
        step = float(rules.get("qty_step") or BITUNIX_QTY_STEP)
        min_qty = float(rules.get("min_base_qty") or BITUNIX_MIN_BASE_QTY)
        total_qty = self._round_qty_down(float(execution.get("qty", 0) or 0), step)
        if total_qty <= 0:
            return ExecutionResult(self.mode, False, "Tracked position quantity is zero.", execution)

        if fraction >= 0.999:
            self.client.flash_close_position(str(position_id))
            self._cancel_remaining_protection(execution)
            sl_order = execution.get("sl_order") or {}
            sl_order_id = sl_order.get("orderId") or sl_order.get("id")
            if sl_order_id:
                try:
                    self.client.cancel_tpsl(symbol, str(sl_order_id))
                except Exception:
                    pass
            execution["active"] = False
            signal["status"] = "CLOSED"
            signal["closed_at"] = datetime.now(timezone.utc).isoformat()
            return ExecutionResult(self.mode, True, "Closed full position on Bitunix.", execution)

        if signal.get("tp1_hit") or signal.get("tp2_hit") or signal.get("tp3_hit"):
            return ExecutionResult(
                self.mode,
                False,
                "Partial manual close after TP progression is not supported yet; close full or adjust TP/SL instead.",
                execution,
            )

        close_qty = self._round_qty_down(total_qty * fraction, step)
        if close_qty < min_qty:
            return ExecutionResult(self.mode, False, "Requested partial close is below Bitunix minimum quantity.", execution)
        remaining_qty = self._round_qty_down(total_qty - close_qty, step)
        if remaining_qty < min_qty:
            return self.manual_close_position(signal, 1.0)

        self.client.place_order(
            symbol=symbol,
            side=execution.get("exit_side"),
            qty=float(close_qty),
            order_type="MARKET",
            reduce_only=execution.get("exit_reduce_only"),
            client_id=f"{execution.get('signal_id', new_signal_id())}-manual-close",
            trade_side=execution.get("exit_trade_side"),
            position_id=str(position_id) if execution.get("exit_trade_side") else None,
        )
        self._cancel_remaining_protection(execution)
        execution["qty"] = float(remaining_qty)
        tp_targets = list(execution.get("tp_targets") or [signal.get("tp1"), signal.get("tp2"), signal.get("tp3")])
        new_tp_qtys, warning = self._split_qty(remaining_qty, min_base_qty=min_qty, step=step)
        execution["tp_qtys"] = new_tp_qtys
        execution["tp_orders"] = []
        execution["missing_tp_indices"] = []
        protection_warnings = []
        if warning:
            protection_warnings.append(warning)
        active_tp_indices = [i + 1 for i, q in enumerate(new_tp_qtys) if float(q or 0) > 0]
        single_tp_index = active_tp_indices[0] if len(active_tp_indices) == 1 else None
        plan = {
            "exit_reduce_only": execution.get("exit_reduce_only"),
            "exit_trade_side": execution.get("exit_trade_side"),
        }
        for idx, qty_part in enumerate(new_tp_qtys, start=1):
            if qty_part <= 0:
                continue
            target_price = float(tp_targets[idx - 1] or signal.get(f"tp{idx}") or 0)
            if target_price <= 0:
                continue
            tp_record, tp_warning = self._place_take_profit_order(
                symbol=symbol,
                position_id=str(position_id),
                exit_side=execution.get("exit_side"),
                qty_part=float(qty_part),
                tp_price=target_price,
                signal_id=execution.get("signal_id", new_signal_id()),
                tp_index=idx,
                plan=plan,
                use_position_tp=(single_tp_index == idx),
            )
            if tp_warning:
                protection_warnings.append(tp_warning)
            if tp_record is None:
                execution["missing_tp_indices"].append(idx)
            else:
                execution["tp_orders"].append(tp_record)
        execution["protection_warnings"] = protection_warnings
        execution["protection_ready"] = bool(execution.get("sl_order")) and len(execution["tp_orders"]) == len(active_tp_indices)
        return ExecutionResult(self.mode, True, f"Closed {close_qty:.6f} and rebuilt protection for remaining position.", execution)

    def get_exchange_open_position_count(self) -> int:
        self._refresh_state()
        if self.mode != "live" or not self.client.is_configured():
            return 0
        return int(self._safe_get_open_positions().get("count", 0) or 0)

    def _safe_get_open_positions(self) -> Dict[str, Any]:
        if not self.client.is_configured():
            return {"count": 0, "sides": [], "used_margin": 0.0, "error": "Bitunix futures API credentials are not configured."}
        try:
            data = self.client.get_pending_positions().get("data", [])
            if isinstance(data, dict):
                data = data.get("positionList") or data.get("data") or []
            count = 0
            sides = []
            used_margin = 0.0
            for pos in data or []:
                qty = float(pos.get("qty") or pos.get("positionQty") or 0)
                if qty > 0:
                    count += 1
                    side = str(pos.get("side") or pos.get("positionSide") or "").upper()
                    if side:
                        sides.append(side)
                    pos_margin = (
                        pos.get("positionMargin")
                        or pos.get("margin")
                        or pos.get("initialMargin")
                        or pos.get("marginFrozen")
                        or pos.get("positionValue")
                        or 0
                    )
                    try:
                        used_margin += float(pos_margin or 0)
                    except Exception:
                        pass
            return {"count": count, "sides": sides, "used_margin": used_margin}
        except Exception as e:
            return {
                "count": 0,
                "sides": [],
                "used_margin": 0.0,
                "error": str(e),
                "endpoint": getattr(e, "endpoint", "GET /api/v1/futures/position/get_pending_positions"),
                "response_text": getattr(e, "response_text", None),
            }

    @staticmethod
    def _round_qty_down(qty: float, step: float) -> float:
        if step <= 0:
            return max(0.0, float(qty))
        return max(0.0, int(float(qty) / step) * step)

    @classmethod
    def _split_qty(
        cls,
        qty: float,
        *,
        min_base_qty: Optional[float] = None,
        step: Optional[float] = None,
        splits: Optional[tuple[float, float, float]] = None,
    ) -> tuple[List[float], Optional[str]]:
        step = max(float(step or BITUNIX_QTY_STEP or 0), 0.00000001)
        min_leg = max(float(min_base_qty or BITUNIX_MIN_BASE_QTY or 0), step)
        total = cls._round_qty_down(float(qty), step)
        if total <= 0:
            return [0.0, 0.0, 0.0], "TP legs skipped: quantity rounded to zero."

        if total < min_leg:
            return [0.0, 0.0, 0.0], (
                f"TP legs skipped: total qty {total:.8f} is below Bitunix min leg {min_leg:.8f}."
            )

        a, b, c = splits or get_tp_splits_for_tf(None, "")
        q1 = cls._round_qty_down(total * a, step)
        q2 = cls._round_qty_down(total * b, step)
        q3 = cls._round_qty_down(max(0.0, total - q1 - q2), step)

        nonzero = [q for q in (q1, q2, q3) if q > 0]
        if len(nonzero) == 3 and min(nonzero) >= min_leg and abs((q1 + q2 + q3) - total) < (step + 1e-12):
            return [q1, q2, q3], None

        return [0.0, total, 0.0], (
            f"Compressed TP legs to a single full TP2 because partial TP legs fall below Bitunix min qty {min_leg:.8f}."
        )

    @classmethod
    def _split_qty_for_indices(
        cls,
        qty: float,
        *,
        active_indices: List[int],
        min_base_qty: Optional[float] = None,
        step: Optional[float] = None,
        splits: Optional[tuple[float, float, float]] = None,
    ) -> tuple[List[float], Optional[str]]:
        active = [int(i) for i in active_indices if int(i) in {1, 2, 3}]
        if not active:
            return [0.0, 0.0, 0.0], "No active TP targets remain for protection rebuild."

        step = max(float(step or BITUNIX_QTY_STEP or 0), 0.00000001)
        min_leg = max(float(min_base_qty or BITUNIX_MIN_BASE_QTY or 0), step)
        total = cls._round_qty_down(float(qty), step)
        if total <= 0:
            return [0.0, 0.0, 0.0], "TP rebuild skipped: quantity rounded to zero."
        if total < min_leg:
            return [0.0, 0.0, 0.0], (
                f"TP rebuild skipped: total qty {total:.8f} is below Bitunix min leg {min_leg:.8f}."
            )

        raw_splits = list(splits or get_tp_splits_for_tf(None, ""))
        while len(raw_splits) < 3:
            raw_splits.append(0.0)
        weights = [max(0.0, float(raw_splits[idx - 1] or 0.0)) for idx in active]
        if sum(weights) <= 0:
            weights = [1.0] * len(active)
        total_weight = sum(weights)

        rebuilt = [0.0, 0.0, 0.0]
        remaining = total
        for idx, weight in list(zip(active, weights))[:-1]:
            leg_qty = cls._round_qty_down(total * (weight / total_weight), step)
            rebuilt[idx - 1] = leg_qty
            remaining = max(0.0, remaining - leg_qty)
        rebuilt[active[-1] - 1] = cls._round_qty_down(remaining, step)

        active_qtys = [rebuilt[idx - 1] for idx in active if rebuilt[idx - 1] > 0]
        if (
            len(active_qtys) == len(active)
            and min(active_qtys) >= min_leg
            and abs(sum(rebuilt) - total) < (step + 1e-12)
        ):
            return rebuilt, None

        strongest_idx = active[max(range(len(active)), key=lambda i: weights[i])]
        compressed = [0.0, 0.0, 0.0]
        compressed[strongest_idx - 1] = total
        return compressed, (
            f"Compressed rebuilt TP legs to TP{strongest_idx} because one or more remaining legs fell below Bitunix min qty {min_leg:.8f}."
        )

    @staticmethod
    def _preferred_single_tp_index(signal: Dict[str, Any], execution: Optional[Dict[str, Any]] = None) -> int:
        execution = execution or {}
        qtys = list(execution.get("tp_qtys") or [0.0, 0.0, 0.0])
        while len(qtys) < 3:
            qtys.append(0.0)
        active_indices = [i + 1 for i, q in enumerate(qtys[:3]) if float(q or 0) > 0]
        if len(active_indices) == 1:
            return active_indices[0]

        targets = list(execution.get("tp_targets") or [signal.get("tp1"), signal.get("tp2"), signal.get("tp3")])
        while len(targets) < 3:
            targets.append(None)
        active_target_indices = [i + 1 for i, price in enumerate(targets[:3]) if float(price or 0) > 0]
        if len(active_target_indices) == 1:
            return active_target_indices[0]
        if 2 in active_target_indices:
            return 2
        return 2

    def _get_symbol_rules(self, symbol: str) -> Dict[str, Any]:
        symbol_key = str(symbol or SYMBOL).upper()
        cached = self._symbol_rules_cache.get(symbol_key)
        if cached:
            return cached

        rules = {
            "symbol": symbol_key,
            "min_base_qty": float(BITUNIX_MIN_BASE_QTY),
            "qty_step": float(BITUNIX_QTY_STEP),
        }
        if not BITUNIX_FETCH_SYMBOL_RULES:
            self._symbol_rules_cache[symbol_key] = rules
            return rules

        try:
            data = self.client.get_trading_pairs(symbol_key).get("data", []) or []
            if isinstance(data, dict):
                data = data.get("list") or data.get("data") or []
            row = next((item for item in data if str(item.get("symbol", "")).upper() == symbol_key), None)
            if row:
                base_precision = int(row.get("basePrecision") or 0)
                qty_step = float(f"1e-{base_precision}") if base_precision > 0 else float(BITUNIX_QTY_STEP)
                min_trade_volume = float(row.get("minTradeVolume") or BITUNIX_MIN_BASE_QTY)
                rules.update({
                    "min_base_qty": max(float(BITUNIX_MIN_BASE_QTY), min_trade_volume),
                    "qty_step": max(float(BITUNIX_QTY_STEP), qty_step),
                    "max_leverage": int(row.get("maxLeverage") or 0),
                    "min_leverage": int(row.get("minLeverage") or 0),
                    "default_leverage": int(row.get("defaultLeverage") or 0),
                    "base_precision": base_precision,
                    "quote_precision": int(row.get("quotePrecision") or 0),
                    "symbol_status": str(row.get("symbolStatus") or ""),
                })
        except Exception:
            pass

        self._symbol_rules_cache[symbol_key] = rules
        return rules

    def reconcile_execution_state(self, signals: List[Dict[str, Any]]) -> Dict[str, Any]:
        self._refresh_state()
        report: Dict[str, Any] = {
            "mode": self.mode,
            "configured": self.client.is_configured(),
            "matched": 0,
            "inactive_marked": 0,
            "state_updated": 0,
            "orphan_positions": [],
            "missing_protection": [],
            "errors": [],
        }
        if self.mode != "live" or not self.client.is_configured():
            return report

        try:
            positions = self._pending_positions_list(SYMBOL)
        except Exception as e:
            report["errors"].append(f"positions: {e}")
            return report

        try:
            pending_tpsl = self.client.get_pending_tpsl(SYMBOL).get("data", []) or []
            if isinstance(pending_tpsl, dict):
                pending_tpsl = pending_tpsl.get("orderList") or pending_tpsl.get("data") or []
        except Exception as e:
            pending_tpsl = []
            report["errors"].append(f"tpsl: {e}")

        try:
            pending_orders = self.client.get_pending_orders(SYMBOL).get("data", []) or []
            if isinstance(pending_orders, dict):
                pending_orders = pending_orders.get("orderList") or pending_orders.get("data") or []
        except Exception as e:
            pending_orders = []
            report["errors"].append(f"orders: {e}")

        live_positions = {
            str(pos.get("positionId")): pos
            for pos in positions
            if str(pos.get("positionId") or "").strip() and self._position_qty(pos) > 0
        }
        referenced_positions = set()
        live_tpsl_ids = {str(o.get("orderId") or o.get("id")) for o in pending_tpsl if str(o.get("orderId") or o.get("id") or "").strip()}
        live_order_ids = {str(o.get("orderId") or o.get("id")) for o in pending_orders if str(o.get("orderId") or o.get("id") or "").strip()}

        for sig in signals or []:
            execution = (sig or {}).get("execution") or {}
            if not execution:
                continue
            position_id = str(execution.get("position_id") or "").strip()
            if not position_id:
                continue
            if not execution.get("active", True):
                continue
            referenced_positions.add(position_id)
            if position_id not in live_positions:
                execution["active"] = False
                report["inactive_marked"] += 1
                continue

            report["matched"] += 1
            position_tpsl_rows = [
                row for row in pending_tpsl
                if str(row.get("positionId") or row.get("position_id") or "").strip() == position_id
            ]

            def _has_live_price(row: Dict[str, Any], key: str) -> bool:
                raw = row.get(key)
                if raw in (None, "", "0", 0, 0.0):
                    return False
                try:
                    return float(raw) > 0
                except Exception:
                    return bool(str(raw).strip())

            live_sl_rows = [row for row in position_tpsl_rows if _has_live_price(row, "slPrice")]
            live_tp_rows = [row for row in position_tpsl_rows if _has_live_price(row, "tpPrice")]

            sl_order = execution.get("sl_order") or {}
            sl_id = str(sl_order.get("orderId") or sl_order.get("id") or "").strip()
            tp_orders = execution.get("tp_orders") or []
            tp_ids = {
                str(order.get("orderId") or order.get("id") or "").strip()
                for order in tp_orders
                if str(order.get("orderId") or order.get("id") or "").strip()
            }
            has_sl = bool(live_sl_rows) if sl_order else False
            has_any_tp = bool(live_tp_rows) if any(str(o.get("kind", "")).upper() == "POSITION_TP" for o in tp_orders) else ((not tp_ids) or any((tp_id in live_tpsl_ids or tp_id in live_order_ids) for tp_id in tp_ids))
            if sl_order and not live_sl_rows:
                execution["sl_order"] = {}
                execution["sl_moved_to"] = None
                report["state_updated"] += 1
            elif (not sl_order) and live_sl_rows:
                row = live_sl_rows[0]
                execution["sl_order"] = {
                    "id": row.get("id") or row.get("orderId"),
                    "orderId": row.get("id") or row.get("orderId"),
                    "price": row.get("slPrice"),
                    "raw": row,
                }
                report["state_updated"] += 1
            if tp_orders:
                live_tp_orders = []
                removed_tp_indices = []
                for order in tp_orders:
                    order_id = str(order.get("orderId") or order.get("id") or "").strip()
                    kind = str(order.get("kind", "")).upper()
                    is_live = False
                    if kind == "POSITION_TP":
                        is_live = bool(live_tp_rows)
                    elif order_id and (order_id in live_tpsl_ids or order_id in live_order_ids):
                        is_live = True
                    if is_live:
                        live_tp_orders.append(order)
                    else:
                        try:
                            idx = int(order.get("index") or 0)
                        except Exception:
                            idx = 0
                        if idx in {1, 2, 3}:
                            removed_tp_indices.append(idx)
                if len(live_tp_orders) != len(tp_orders):
                    execution["tp_orders"] = live_tp_orders
                    qtys = list(execution.get("tp_qtys") or [0.0, 0.0, 0.0])
                    while len(qtys) < 3:
                        qtys.append(0.0)
                    for idx in removed_tp_indices:
                        qtys[idx - 1] = 0.0
                    execution["tp_qtys"] = qtys
                    missing = set(int(i) for i in (execution.get("missing_tp_indices") or []))
                    missing.update(removed_tp_indices)
                    execution["missing_tp_indices"] = sorted(i for i in missing if i in {1, 2, 3})
                    execution["protection_ready"] = bool(execution.get("sl_order")) and len(live_tp_orders) == len([q for q in qtys if float(q or 0) > 0])
                    if not live_tp_orders and not any(float(q or 0) > 0 for q in qtys[:3]):
                        execution["tp_mode"] = "NONE"
                    report["state_updated"] += 1
            elif live_tp_rows:
                row = live_tp_rows[0]
                try:
                    tp_qty = float(row.get("tpQty") or execution.get("qty") or 0)
                except Exception:
                    tp_qty = float(execution.get("qty") or 0)
                single_tp_index = self._preferred_single_tp_index(sig, execution)
                execution["tp_orders"] = [{
                    "index": single_tp_index,
                    "kind": "POSITION_TP",
                    "qty": tp_qty,
                    "price": float(row.get("tpPrice") or 0),
                    "orderId": row.get("id") or row.get("orderId"),
                    "id": row.get("id") or row.get("orderId"),
                    "raw": row,
                }]
                qtys = [0.0, 0.0, 0.0]
                qtys[single_tp_index - 1] = tp_qty
                execution["tp_qtys"] = qtys
                targets = [None, None, None]
                targets[single_tp_index - 1] = float(row.get("tpPrice") or 0)
                execution["tp_targets"] = targets
                execution["tp_mode"] = "POSITION_TP"
                execution["protection_ready"] = bool(execution.get("sl_order"))
                report["state_updated"] += 1
            has_sl = bool(execution.get("sl_order"))
            has_any_tp = bool(execution.get("tp_orders"))
            if not has_sl or not has_any_tp:
                report["missing_protection"].append({
                    "signal_id": execution.get("signal_id") or sig.get("signal_id"),
                    "position_id": position_id,
                    "side": sig.get("side"),
                    "tf": sig.get("tf"),
                    "missing_sl": not has_sl,
                    "missing_tp": not has_any_tp,
                })

        for position_id, pos in live_positions.items():
            if position_id in referenced_positions:
                continue
            report["orphan_positions"].append({
                "position_id": position_id,
                "side": str(pos.get("side") or pos.get("positionSide") or ""),
                "qty": float(pos.get("qty") or pos.get("positionQty") or 0),
                "symbol": str(pos.get("symbol") or SYMBOL),
            })

        return report

    def _evaluate_liquidation_safety(self, position: Dict[str, Any], signal: Dict[str, Any], plan: Dict[str, Any]) -> Dict[str, Any]:
        liq_raw = position.get("liqPrice") or position.get("liquidationPrice") or position.get("liq_price")
        try:
            liq_price = float(liq_raw)
        except Exception:
            liq_price = 0.0

        if liq_price <= 0:
            return {"safe": True, "liq_price": liq_price, "reason": "Bitunix did not return liqPrice."}

        entry = float(signal["entry"])
        sl = float(signal["sl"])
        risk = abs(entry - sl)
        if risk <= 0:
            return {"safe": True, "liq_price": liq_price, "reason": "Zero-risk setup."}

        buffer_price = risk * float(BITUNIX_LIQUIDATION_SAFETY_BUFFER_R)
        side = str(signal.get("side", "")).upper()
        if side == "LONG":
            safe = liq_price < (sl - buffer_price)
            reason = f"liq={liq_price:.2f}, sl={sl:.2f}, required_below={sl - buffer_price:.2f}"
        else:
            safe = liq_price > (sl + buffer_price)
            reason = f"liq={liq_price:.2f}, sl={sl:.2f}, required_above={sl + buffer_price:.2f}"

        return {
            "safe": bool(safe),
            "liq_price": liq_price,
            "buffer_price": buffer_price,
            "reason": reason,
            "tf": plan.get("tf"),
        }

    def _estimate_pretrade_liquidation(
        self,
        *,
        symbol: str,
        side: str,
        entry: float,
        sl: float,
        notional: float,
        leverage: int,
        margin_mode: str,
    ) -> Dict[str, Any]:
        risk = abs(entry - sl)
        if entry <= 0 or risk <= 0 or leverage <= 0 or notional <= 0:
            return {"safe": True, "liq_price": None, "reason": "Insufficient inputs for pre-trade estimate."}

        mmr = self._maintenance_margin_rate(symbol, notional, leverage)
        if mmr is None:
            return {"safe": True, "liq_price": None, "reason": "Position tier data unavailable."}

        liq_move_pct = max(0.0, (1.0 / float(leverage)) - float(mmr))
        if liq_move_pct <= 0:
            return {"safe": False, "liq_price": None, "maintenance_margin_rate": mmr, "reason": "Leverage too high for maintenance margin tier."}

        if side == "LONG":
            liq_price = entry * (1.0 - liq_move_pct)
            safe = liq_price < (sl - risk * float(BITUNIX_LIQUIDATION_SAFETY_BUFFER_R))
            required = sl - risk * float(BITUNIX_LIQUIDATION_SAFETY_BUFFER_R)
            reason = f"pre_liq={liq_price:.2f}, sl={sl:.2f}, required_below={required:.2f}, mmr={mmr:.4f}, margin_mode={margin_mode}"
        else:
            liq_price = entry * (1.0 + liq_move_pct)
            safe = liq_price > (sl + risk * float(BITUNIX_LIQUIDATION_SAFETY_BUFFER_R))
            required = sl + risk * float(BITUNIX_LIQUIDATION_SAFETY_BUFFER_R)
            reason = f"pre_liq={liq_price:.2f}, sl={sl:.2f}, required_above={required:.2f}, mmr={mmr:.4f}, margin_mode={margin_mode}"

        return {
            "safe": bool(safe),
            "liq_price": float(liq_price),
            "maintenance_margin_rate": float(mmr),
            "reason": reason,
        }

    def _maintenance_margin_rate(self, symbol: str, notional: float, leverage: int) -> Optional[float]:
        if not self.client.is_configured():
            return None
        try:
            tiers = self.client.get_position_tiers(symbol).get("data", []) or []
        except Exception:
            return None
        chosen = None
        value = float(notional)
        for tier in tiers:
            try:
                start = float(tier.get("startValue") or 0)
                end_raw = tier.get("endValue")
                end = float(end_raw) if end_raw not in (None, "", "0") else float("inf")
                tier_leverage = int(tier.get("leverage") or 0)
                if value >= start and value < end:
                    chosen = tier
                    if tier_leverage <= 0 or leverage <= tier_leverage:
                        break
            except Exception:
                continue
        if chosen is None and tiers:
            chosen = tiers[-1]
        if not chosen:
            return None
        try:
            return float(chosen.get("maintenanceMarginRate"))
        except Exception:
            return None

    def _ensure_required_margin_mode(self, symbol: str, exchange_open_positions: int = 0) -> Dict[str, Any]:
        required = str(BITUNIX_REQUIRED_MARGIN_MODE or "ISOLATION").strip().upper()
        try:
            current_data = self.client.get_leverage_margin_mode(symbol, BITUNIX_MARGIN_COIN).get("data", {}) or {}
        except Exception as e:
            return {
                "ok": False,
                "required_margin_mode": required,
                "error": str(e),
                "message": f"Could not verify Bitunix margin mode: {e}",
            }

        current_mode = str(current_data.get("marginMode") or "").strip().upper()
        if current_mode == required:
            return {"ok": True, "margin_mode": current_mode, "required_margin_mode": required}

        if exchange_open_positions > 0:
            return {
                "ok": False,
                "margin_mode": current_mode,
                "required_margin_mode": required,
                "message": f"Margin mode is {current_mode}; required {required}. Close open positions before switching.",
            }

        try:
            self.client.change_margin_mode(symbol, required, BITUNIX_MARGIN_COIN)
            confirmed = self.client.get_leverage_margin_mode(symbol, BITUNIX_MARGIN_COIN).get("data", {}) or {}
            confirmed_mode = str(confirmed.get("marginMode") or "").strip().upper()
        except Exception as e:
            return {
                "ok": False,
                "margin_mode": current_mode,
                "required_margin_mode": required,
                "error": str(e),
                "message": f"Failed to switch Bitunix margin mode to {required}: {e}",
            }

        if confirmed_mode != required:
            return {
                "ok": False,
                "margin_mode": confirmed_mode,
                "required_margin_mode": required,
                "message": f"Bitunix margin mode stayed {confirmed_mode}; required {required}.",
            }

        return {
            "ok": True,
            "margin_mode": confirmed_mode,
            "required_margin_mode": required,
            "message": f"Bitunix margin mode switched to {required}.",
        }

    def _ensure_required_position_mode(self, exchange_open_positions: int = 0) -> Dict[str, Any]:
        required = str(BITUNIX_POSITION_MODE or "ONE_WAY").strip().upper()
        try:
            account = self.client.get_single_account(BITUNIX_MARGIN_COIN).get("data", {}) or {}
            if isinstance(account, list):
                account = account[0] if account else {}
        except Exception as e:
            return {
                "ok": False,
                "required_position_mode": required,
                "error": str(e),
                "message": f"Could not verify Bitunix position mode: {e}",
            }

        current_mode = str(account.get("positionMode") or "").strip().upper()
        if current_mode == required:
            return {"ok": True, "position_mode": current_mode, "required_position_mode": required}

        if exchange_open_positions > 0:
            return {
                "ok": False,
                "position_mode": current_mode,
                "required_position_mode": required,
                "message": f"Position mode is {current_mode}; required {required}. Close open positions before switching.",
            }

        try:
            self.client.change_position_mode(required)
            confirm = self.client.get_single_account(BITUNIX_MARGIN_COIN).get("data", {}) or {}
            if isinstance(confirm, list):
                confirm = confirm[0] if confirm else {}
            confirmed_mode = str(confirm.get("positionMode") or "").strip().upper()
        except Exception as e:
            return {
                "ok": False,
                "position_mode": current_mode,
                "required_position_mode": required,
                "error": str(e),
                "message": f"Failed to switch Bitunix position mode to {required}: {e}",
            }

        if confirmed_mode != required:
            return {
                "ok": False,
                "position_mode": confirmed_mode,
                "required_position_mode": required,
                "message": f"Bitunix position mode stayed {confirmed_mode}; required {required}.",
            }

        return {
            "ok": True,
            "position_mode": confirmed_mode,
            "required_position_mode": required,
            "message": f"Bitunix position mode switched to {required}.",
        }


def new_signal_id() -> str:
    return uuid.uuid4().hex[:16]
