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
    BITUNIX_LIQUIDATION_MAX_LEVERAGE_BY_TF,
    BITUNIX_LIQUIDATION_SAFETY_BUFFER_R,
    BITUNIX_LIQUIDATION_SAFETY_ENABLED,
    BITUNIX_MARGIN_COIN,
    BITUNIX_MAX_OPEN_POSITIONS,
    BITUNIX_MAX_RISK_USD,
    BITUNIX_MIN_BASE_QTY,
    BITUNIX_MIN_NOTIONAL_USD,
    BITUNIX_POSITION_MODE,
    BITUNIX_QTY_STEP,
    BITUNIX_REQUIRED_MARGIN_MODE,
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


def _sorted_keys(d: Dict[str, Any]) -> List[str]:
    # Bitunix docs require query params sorted in ascending ASCII order by key.
    return sorted(d.keys())


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

    def get_leverage_margin_mode(self, symbol: str, margin_coin: str = BITUNIX_MARGIN_COIN) -> Dict[str, Any]:
        return self._request(
            "GET",
            "/api/v1/futures/account/get_leverage_margin_mode",
            query={"symbol": symbol, "marginCoin": margin_coin},
        )

    def get_position_tiers(self, symbol: str) -> Dict[str, Any]:
        return self._request("GET", "/api/v1/futures/position/get_position_tiers", query={"symbol": symbol})

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

    def cancel_orders(self, symbol: str, order_ids: List[str]) -> Dict[str, Any]:
        order_list = [{"orderId": str(order_id)} for order_id in order_ids if order_id]
        if not order_list:
            return {"code": "0", "data": {"successList": [], "failureList": []}, "msg": "Nothing to cancel"}
        return self._request("POST", "/api/v1/futures/trade/cancel_orders", payload={"symbol": symbol, "orderList": order_list})

    def flash_close_position(self, position_id: str) -> Dict[str, Any]:
        return self._request("POST", "/api/v1/futures/trade/flash_close_position", payload={"positionId": str(position_id)})

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
        raw = bal.get("raw") or {}
        info["position_mode"] = str(raw.get("positionMode") or BITUNIX_POSITION_MODE or "UNKNOWN").strip().upper()
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

        try:
            self.client.change_leverage(symbol, int(plan["leverage"]))
            entry_order = self.client.place_order(
                symbol=symbol,
                side=entry_side,
                qty=plan["qty"],
                order_type="MARKET",
                reduce_only=None,
                client_id=f"{signal_id}-entry",
                trade_side=plan.get("entry_trade_side"),
            )
            entry_order_data = entry_order.get("data", {}) or {}
            entry_order_id = entry_order_data.get("orderId") or entry_order_data.get("id")
            entry_client_id = entry_order_data.get("clientId") or f"{signal_id}-entry"
            plan["entry_order_id"] = entry_order_id
            plan["entry_client_id"] = entry_client_id

            position = self._find_position_from_entry(
                symbol=symbol,
                side=signal["side"],
                order_id=entry_order_id,
                client_id=entry_client_id,
                retries=20,
                delay_sec=0.75,
            )
            if not position:
                order_detail = self._safe_get_order_detail(order_id=entry_order_id, client_id=entry_client_id)
                if order_detail:
                    plan["entry_status"] = order_detail.get("status")
                    plan["entry_trade_qty"] = order_detail.get("tradeQty")
                raise BitunixTradeError(f"Entry accepted but no pending {signal['side']} position was returned.")

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
            for idx, (qty_part, tp_price) in enumerate(zip(plan["tp_qtys"], [signal["tp1"], signal["tp2"], signal["tp3"]]), start=1):
                if qty_part <= 0:
                    continue
                tp_record = self._place_take_profit_order(
                    symbol=symbol,
                    position_id=position_id,
                    exit_side=exit_side,
                    qty_part=float(qty_part),
                    tp_price=float(tp_price),
                    signal_id=signal_id,
                    tp_index=idx,
                    plan=plan,
                )
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
                "entry_order": entry_order.get("data", {}),
                "tp_orders": tp_orders,
                "sl_order": sl_order,
                "missing_tp_indices": missing_tp_indices,
                "protection_warnings": protection_warnings,
                "protection_ready": bool(sl_order) and len(tp_orders) == len([q for q in plan["tp_qtys"] if q > 0]),
                "qty": plan["qty"],
                "tp_qtys": plan["tp_qtys"],
                "tp_targets": [float(signal["tp1"]), float(signal["tp2"]), float(signal["tp3"])],
                "leverage": plan["leverage"],
                "risk_budget_usd": plan["risk_budget_usd"],
                "balance_available": plan["balance_available"],
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
            remaining_qty = max(0.0, float(execution.get("qty", 0) or 0) - float((execution.get("tp_qtys") or [0.0])[0] or 0))
            if remaining_qty <= 1e-12:
                self._cancel_remaining_protection(execution)
                if sl_order_id:
                    try:
                        self.client.cancel_tpsl(symbol, str(sl_order_id))
                    except Exception:
                        pass
                execution["active"] = False
                execution["sl_moved_to"] = None
                return ExecutionResult(self.mode, True, "TP1 closed the full position.", execution)
            self.client.modify_position_tpsl(symbol, str(position_id), None, float(signal["entry"]))
            execution["sl_moved_to"] = float(signal["entry"])
            return ExecutionResult(self.mode, True, "Moved SL to entry after TP1.", execution)

        if event_type == "TP2" and position_id:
            self._ensure_tp_leg_closed(signal, execution, 2)
            return ExecutionResult(self.mode, True, "TP2 reached; SL remains at entry.", execution)

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
        raw_account = balance_data.get("raw") or {}
        position_mode = str(raw_account.get("positionMode") or BITUNIX_POSITION_MODE or "ONE_WAY").strip().upper()
        if position_mode not in {"ONE_WAY", "HEDGE"}:
            position_mode = str(BITUNIX_POSITION_MODE or "ONE_WAY").strip().upper()
        tf_name = str(signal.get("tf") or signal.get("execution_tf") or "5m")
        symbol = SYMBOL
        desired_leverage = int(BITUNIX_DEFAULT_LEVERAGE or 1)
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
        meta = signal.get("meta") or {}
        signal_size_pct = meta.get("size", signal.get("size", 0))
        try:
            signal_size_pct = float(signal_size_pct or 0)
        except Exception:
            signal_size_pct = 0.0
        effective_risk_cap_pct = (signal_size_pct / 100.0) if signal_size_pct > 0 else float(BITUNIX_RISK_CAP_PCT)
        risk_from_balance = balance_available * effective_risk_cap_pct
        risk_budget = min(BITUNIX_MAX_RISK_USD, risk_from_balance) if balance_available > 0 else 0.0
        risk_qty = (risk_budget / risk_per_unit) if risk_budget > 0 else 0.0
        affordable_notional = max(0.0, balance_available * leverage * 0.98)
        affordable_qty = (affordable_notional / entry) if entry > 0 else 0.0
        qty = min(risk_qty, affordable_qty) if affordable_qty > 0 else 0.0
        if (
            qty > 0
            and balance_available >= BITUNIX_MIN_NOTIONAL_USD
            and affordable_notional >= BITUNIX_MIN_NOTIONAL_USD
            and qty * entry < BITUNIX_MIN_NOTIONAL_USD
        ):
            qty = min(BITUNIX_MIN_NOTIONAL_USD / entry, affordable_qty)
        tp_qtys, tp_split_warning = self._split_qty(qty)
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
            "leverage": leverage,
            "current_exchange_leverage": int(raw_account.get("leverage") or 0),
            "target_leverage": target_leverage,
            "risk_budget_usd": risk_budget,
            "risk_cap_pct": effective_risk_cap_pct,
            "signal_size_pct": signal_size_pct,
            "risk_qty": risk_qty,
            "affordable_qty": affordable_qty,
            "affordable_notional": affordable_notional,
            "balance_available": balance_available,
            "position_mode": position_mode,
            "margin_mode": margin_mode,
            "required_margin_mode": BITUNIX_REQUIRED_MARGIN_MODE,
            "tf": tf_name,
            "notional": qty * entry,
            "pre_liq_estimate": pre_liq.get("liq_price"),
            "pre_liq_safe": pre_liq.get("safe"),
            "pre_liq_reason": pre_liq.get("reason"),
            "maintenance_margin_rate": pre_liq.get("maintenance_margin_rate"),
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
                sl_order = sl_res.get("data", {}) or {}
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
    ) -> Optional[Dict[str, Any]]:
        # Preferred: true Bitunix TP/SL trigger order so it shows up as TP on the exchange.
        try:
            tp_res = self.client.place_tpsl_order(
                symbol=symbol,
                position_id=position_id,
                tp_price=float(tp_price),
                tp_qty=float(qty_part),
                tp_order_type="MARKET",
            )
            data = tp_res.get("data", {}) or {}
            return {
                "index": tp_index,
                "kind": "TPSL",
                "qty": float(qty_part),
                "price": float(tp_price),
                "orderId": data.get("orderId") or data.get("id"),
                "raw": data,
            }
        except BitunixTradeError:
            pass

        # Fallback: reduce-only limit take-profit order.
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
            )
            data = tp_res.get("data", {}) or {}
            return {
                "index": tp_index,
                "kind": "LIMIT",
                "qty": float(qty_part),
                "price": float(tp_price),
                "orderId": data.get("orderId") or data.get("id"),
                "clientId": data.get("clientId") or f"{signal_id}-tp{tp_index}",
                "raw": data,
            }
        except BitunixTradeError:
            return None

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
            if str(order.get("kind", "")).upper() == "TPSL":
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
    def _round_qty_down(qty: float, step: float) -> float:
        if step <= 0:
            return max(0.0, float(qty))
        return max(0.0, int(float(qty) / step) * step)

    @classmethod
    def _split_qty(cls, qty: float) -> tuple[List[float], Optional[str]]:
        step = max(float(BITUNIX_QTY_STEP or 0), 0.00000001)
        min_leg = max(float(BITUNIX_MIN_BASE_QTY or 0), step)
        total = cls._round_qty_down(float(qty), step)
        if total <= 0:
            return [0.0, 0.0, 0.0], "TP legs skipped: quantity rounded to zero."

        if total < min_leg:
            return [0.0, 0.0, 0.0], (
                f"TP legs skipped: total qty {total:.8f} is below Bitunix min leg {min_leg:.8f}."
            )

        a, b, c = BITUNIX_TP_SPLITS
        q1 = cls._round_qty_down(total * a, step)
        q2 = cls._round_qty_down(total * b, step)
        q3 = cls._round_qty_down(max(0.0, total - q1 - q2), step)

        nonzero = [q for q in (q1, q2, q3) if q > 0]
        if len(nonzero) == 3 and min(nonzero) >= min_leg and abs((q1 + q2 + q3) - total) < (step + 1e-12):
            return [q1, q2, q3], None

        if total >= (2 * min_leg):
            q1 = min_leg
            q2 = 0.0
            q3 = cls._round_qty_down(total - q1, step)
            if q3 >= min_leg:
                return [q1, q2, q3], (
                    f"Compressed TP legs to TP1+TP3 because 3-way split falls below Bitunix min qty {min_leg:.8f}."
                )

        return [total, 0.0, 0.0], (
            f"Compressed TP legs to TP1-only because partial TP legs fall below Bitunix min qty {min_leg:.8f}."
        )

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


def new_signal_id() -> str:
    return uuid.uuid4().hex[:16]
