import hashlib
import json
import os
import secrets
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from config import (
    BITUNIX_DEFAULT_LEVERAGE,
    BITUNIX_FAPI_BASE_URL,
    BITUNIX_FAPI_KEY,
    BITUNIX_FAPI_SECRET,
    BITUNIX_MARGIN_COIN,
    BITUNIX_MAX_OPEN_POSITIONS,
    BITUNIX_MAX_RISK_USD,
    BITUNIX_MIN_NOTIONAL_USD,
    BITUNIX_POSITION_MODE,
    BITUNIX_RISK_CAP_PCT,
    BITUNIX_TPSL_TRIGGER_TYPE,
    BITUNIX_TP_SPLITS,
    BITUNIX_TRADING_ENABLED,
    BITUNIX_TRADING_MODE,
    SYMBOL,
)


class BitunixTradeError(RuntimeError):
    def __init__(self, message: str, *, endpoint: Optional[str] = None, response_text: Optional[str] = None):
        super().__init__(message)
        self.endpoint = endpoint
        self.response_text = response_text


@dataclass
class ExecutionResult:
    mode: str
    accepted: bool
    message: str
    payload: Dict[str, Any]


def _get_parameter_type(key: str) -> int:
    if key and key[0].isdigit():
        return 1
    if key and key[0].islower():
        return 2
    return 3


def _str_ascii_sum(s: str) -> int:
    return sum(ord(c) for c in s)


def _sorted_keys(d: Dict[str, Any]) -> List[str]:
    return sorted(d.keys(), key=lambda k: (_get_parameter_type(k), _str_ascii_sum(k)))


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class BitunixFuturesClient:
    def __init__(self):
        self.base_url = BITUNIX_FAPI_BASE_URL.rstrip("/")
        self.api_key = BITUNIX_FAPI_KEY
        self.api_secret = BITUNIX_FAPI_SECRET

    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

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
        nonce = secrets.token_hex(16)
        timestamp = nonce
        timestamp = str(int(time.time() * 1000))
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
            ) from e
        except Exception as e:
            raise BitunixTradeError(
                f"Request failed on {method} {path}: {e}",
                endpoint=f"{method} {path}",
            ) from e
        code = str(data.get("code"))
        if code not in {"0", "200"}:
            raise BitunixTradeError(
                f"Bitunix API error {code}: {data.get('msg', 'unknown error')}",
                endpoint=f"{method} {path}",
                response_text=json.dumps(data),
            )
        return data

    def get_single_account(self, margin_coin: str = BITUNIX_MARGIN_COIN) -> Dict[str, Any]:
        return self._request("GET", "/api/v1/futures/account", query={"marginCoin": margin_coin})

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

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        order_type: str = "MARKET",
        price: Optional[float] = None,
        reduce_only: bool = False,
        client_id: Optional[str] = None,
        effect: str = "GTC",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "symbol": symbol,
            "qty": self._fmt_num(qty),
            "side": side,
            "orderType": order_type,
            "effect": effect,
            "reduceOnly": bool(reduce_only),
        }
        if client_id:
            payload["clientId"] = client_id
        if order_type == "LIMIT":
            if price is None:
                raise BitunixTradeError("Limit order requires price.")
            payload["price"] = self._fmt_num(price)
        return self._request("POST", "/api/v1/futures/trade/place_order", payload=payload)

    def get_pending_positions(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        if symbol:
            query["symbol"] = symbol
        return self._request("GET", "/api/v1/futures/position/get_pending_positions", query=query)

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

    def get_pending_tpsl(self, symbol: str, position_id: Optional[str] = None) -> Dict[str, Any]:
        query: Dict[str, Any] = {"symbol": symbol}
        if position_id:
            query["positionId"] = position_id
        return self._request("GET", "/api/v1/futures/tpsl/get_pending_orders", query=query)

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

    def cancel_tpsl(self, symbol: str, order_id: str) -> Dict[str, Any]:
        return self._request("POST", "/api/v1/futures/tpsl/cancel_order", payload={"symbol": symbol, "orderId": str(order_id)})

    @staticmethod
    def _fmt_num(value: float) -> str:
        return f"{float(value):.8f}".rstrip("0").rstrip(".")


class TradeExecutor:
    def __init__(self):
        self.client = BitunixFuturesClient()
        self.mode = BITUNIX_TRADING_MODE
        self.enabled = BITUNIX_TRADING_ENABLED and self.mode in {"demo", "live"}

    def _refresh_state(self):
        mode = os.getenv("BITUNIX_TRADING_MODE", self.mode).strip().lower()
        enabled_raw = os.getenv("BITUNIX_TRADING_ENABLED", "true" if self.enabled else "false").strip().lower()
        self.mode = mode
        self.enabled = enabled_raw == "true" and self.mode in {"demo", "live"}

    def can_trade(self) -> bool:
        self._refresh_state()
        return self.enabled

    def status_line(self) -> str:
        self._refresh_state()
        return f"mode={self.mode} enabled={self.enabled} configured={self.client.is_configured()}"

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
            "open_positions": 0,
            "position_sides": [],
            "errors": [],
        }
        if not self.client.is_configured():
            info["errors"].append("Bitunix futures API key/secret missing.")
            return info

        bal = self._safe_get_balance()
        info["balance_available"] = float(bal.get("available", 0) or 0)
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
        if pos.get("error"):
            info["errors"].append(f"positions: {pos.get('error')}")
            if pos.get("endpoint"):
                info["positions_endpoint"] = pos.get("endpoint")
            if pos.get("response_text"):
                info["positions_response"] = pos.get("response_text")
        elif info["auth_ok"]:
            info["auth_ok"] = True

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

        plan = self._build_plan(signal)
        plan["exchange_open_positions"] = exchange_open_positions
        if plan["balance_available"] <= 0:
            return ExecutionResult(self.mode, False, "No available margin balance.", plan)
        if self.mode == "demo":
            return ExecutionResult(self.mode, True, "Demo execution planned.", plan)
        if not self.client.is_configured():
            return ExecutionResult(self.mode, False, "Missing Bitunix live credentials.", plan)

        symbol = plan["symbol"]
        entry_side = plan["entry_side"]
        exit_side = plan["exit_side"]
        signal_id = signal.get("signal_id") or new_signal_id()

        self.client.change_leverage(symbol, int(plan["leverage"]))
        entry_order = self.client.place_order(
            symbol=symbol,
            side=entry_side,
            qty=plan["qty"],
            order_type="MARKET",
            reduce_only=False,
            client_id=f"{signal_id}-entry",
        )

        position = self._find_position(symbol, signal["side"])
        if not position:
            raise BitunixTradeError(f"Entry accepted but no pending {signal['side']} position was returned.")

        position_id = str(position.get("positionId") or "")
        if not position_id:
            raise BitunixTradeError("Bitunix positionId missing after entry.")

        tp_orders = []
        for idx, (qty_part, tp_price) in enumerate(zip(plan["tp_qtys"], [signal["tp1"], signal["tp2"], signal["tp3"]]), start=1):
            if qty_part <= 0:
                continue
            tp_res = self.client.place_order(
                symbol=symbol,
                side=exit_side,
                qty=qty_part,
                order_type="LIMIT",
                price=float(tp_price),
                reduce_only=True,
                client_id=f"{signal_id}-tp{idx}",
            )
            tp_orders.append(tp_res.get("data", {}))

        sl_res = self.client.place_position_tpsl(symbol, position_id, None, float(signal["sl"]))
        sl_order = sl_res.get("data", {})
        signal["execution"] = {
            "signal_id": signal_id,
            "mode": self.mode,
            "symbol": symbol,
            "side": signal["side"],
            "position_id": position_id,
            "entry_order": entry_order.get("data", {}),
            "tp_orders": tp_orders,
            "sl_order": sl_order,
            "qty": plan["qty"],
            "tp_qtys": plan["tp_qtys"],
            "leverage": plan["leverage"],
            "risk_budget_usd": plan["risk_budget_usd"],
            "balance_available": plan["balance_available"],
            "active": True,
        }
        return ExecutionResult(self.mode, True, "Live Bitunix execution placed.", signal["execution"])

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
            self.client.modify_position_tpsl(symbol, str(position_id), None, float(signal["entry"]))
            execution["sl_moved_to"] = float(signal["entry"])
            return ExecutionResult(self.mode, True, "Moved SL to entry after TP1.", execution)

        if event_type == "TP2" and position_id:
            self.client.modify_position_tpsl(symbol, str(position_id), None, float(signal["tp1"]))
            execution["sl_moved_to"] = float(signal["tp1"])
            return ExecutionResult(self.mode, True, "Moved SL to TP1 after TP2.", execution)

        if event_type in {"TP3", "SL", "ENTRY_CLOSE", "PROFIT_SL"}:
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
        risk_from_balance = balance_available * BITUNIX_RISK_CAP_PCT
        risk_budget = min(BITUNIX_MAX_RISK_USD, risk_from_balance) if balance_available > 0 else 0.0
        qty = (risk_budget / risk_per_unit) if risk_budget > 0 else 0.0
        if qty > 0 and balance_available >= BITUNIX_MIN_NOTIONAL_USD and qty * entry < BITUNIX_MIN_NOTIONAL_USD:
            qty = BITUNIX_MIN_NOTIONAL_USD / entry
        tp_qtys = self._split_qty(qty)

        return {
            "symbol": SYMBOL,
            "signal_type": str(signal.get("type", "SCALP")).upper(),
            "side": signal["side"],
            "entry": entry,
            "sl": sl,
            "tp1": float(signal["tp1"]),
            "tp2": float(signal["tp2"]),
            "tp3": float(signal["tp3"]),
            "entry_side": "BUY" if signal["side"] == "LONG" else "SELL",
            "exit_side": "SELL" if signal["side"] == "LONG" else "BUY",
            "qty": qty,
            "tp_qtys": tp_qtys,
            "leverage": BITUNIX_DEFAULT_LEVERAGE,
            "risk_budget_usd": risk_budget,
            "risk_cap_pct": BITUNIX_RISK_CAP_PCT,
            "balance_available": balance_available,
            "notional": qty * entry,
        }

    def _safe_get_balance(self) -> Dict[str, Any]:
        if self.mode == "demo":
            return {"available": max(BITUNIX_MIN_NOTIONAL_USD * 5, BITUNIX_MAX_RISK_USD * 10)}
        if not self.client.is_configured():
            return {"available": 0}
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
            return {"available": float(available or 0), "raw": raw}
        except Exception as e:
            return {
                "available": 0,
                "error": str(e),
                "endpoint": getattr(e, "endpoint", "GET /api/v1/futures/account"),
                "response_text": getattr(e, "response_text", None),
            }

    def _find_position(self, symbol: str, side: str) -> Optional[Dict[str, Any]]:
        data = self.client.get_pending_positions(symbol).get("data", [])
        if isinstance(data, dict):
            data = data.get("positionList") or data.get("data") or []
        target = side.upper()
        for pos in data or []:
            pos_side = str(pos.get("side") or pos.get("positionSide") or "").upper()
            qty = float(pos.get("qty") or pos.get("positionQty") or 0)
            if pos_side == target and qty > 0:
                return pos
        return None

    def get_exchange_open_position_count(self) -> int:
        self._refresh_state()
        if self.mode != "live" or not self.client.is_configured():
            return 0
        return int(self._safe_get_open_positions().get("count", 0) or 0)

    def _safe_get_open_positions(self) -> Dict[str, Any]:
        if not self.client.is_configured():
            return {"count": 0, "sides": [], "error": "Bitunix futures API credentials are not configured."}
        try:
            data = self.client.get_pending_positions().get("data", [])
            if isinstance(data, dict):
                data = data.get("positionList") or data.get("data") or []
            count = 0
            sides = []
            for pos in data or []:
                qty = float(pos.get("qty") or pos.get("positionQty") or 0)
                if qty > 0:
                    count += 1
                    side = str(pos.get("side") or pos.get("positionSide") or "").upper()
                    if side:
                        sides.append(side)
            return {"count": count, "sides": sides}
        except Exception as e:
            return {
                "count": 0,
                "sides": [],
                "error": str(e),
                "endpoint": getattr(e, "endpoint", "GET /api/v1/futures/position/get_pending_positions"),
                "response_text": getattr(e, "response_text", None),
            }

    @staticmethod
    def _split_qty(qty: float) -> List[float]:
        a, b, c = BITUNIX_TP_SPLITS
        q1 = qty * a
        q2 = qty * b
        q3 = max(0.0, qty - q1 - q2)
        return [q1, q2, q3]


def new_signal_id() -> str:
    return uuid.uuid4().hex[:16]
