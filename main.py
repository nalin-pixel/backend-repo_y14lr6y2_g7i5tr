import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Terminal ID → Human name mapping
TERMINAL_MAP: Dict[str, str] = {
    "15d25714-7526-cc14-0193-897eb2667d2e": "Просвещения",
    "1a0a9f8b-74fa-f826-0175-2d06ca025115": "Мурино",
    "1f85b040-6446-64ca-015e-9f51010a895b": "Искровский",
    "38eb58e7-b049-596b-016e-6bf17621dac5": "Королева",
    "39b92096-e4b6-e0ed-015b-9133d592e611": "Ленинский",
    "39b92096-e4b6-e0ed-015b-944d281c22bb": "Ветеранов",
    "39b92096-e4b6-e0ed-015b-944d281c22ed": "Рыбацкое",
    "39b92096-e4b6-e0ed-015b-944d281c8a0a": "Капитанская",
    "46c6cc6e-6735-8107-0167-21149ac0331f": "Блюхера",
    "4ed2b309-6ec2-2fd3-0193-93b97757c893": "Голикова",
    "62cd0a1a-231e-4134-0162-75c691ce2b65": "Фурштатская",
    "6f8c05e8-40e6-2039-0166-919ba0f42e0d": "Кудрово",
    "8eeeab47-be14-6e80-0173-36235e931afd": "Стародеревенская",
    "e2c0c0c8-df59-a16e-0164-832fbe6a8c96": "Варшавская",
    "eaa99ae2-ec68-3bfd-018e-bf1d68878993": "Купчино",
}

EXTERNAL_BASE = os.getenv("JPS_BASE_URL", "https://adminjps.joyspizza.ru")
EXTERNAL_SUMMARY = f"{EXTERNAL_BASE}/api/external/orders/summary"
# Prefer env var; fallback to provided token string for convenience
DEFAULT_TOKEN = "jps_630ef4b590b00d1a355fd8e62d15591f2897422bccfac6c6"
API_TOKEN = os.getenv("JPS_API_TOKEN", DEFAULT_TOKEN)


def _auth_headers(token: Optional[str] = None) -> Dict[str, str]:
    tok = token or API_TOKEN
    # Try both common auth styles to maximize compatibility
    return {
        "Authorization": f"Bearer {tok}",
        "X-API-KEY": tok,
        "Accept": "application/json",
    }


@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI Backend!"}


@app.get("/api/hello")
def hello():
    return {"message": "Hello from the backend API!"}


@app.get("/test")
def test_database():
    """Simple health check for backend"""
    return {"backend": "✅ Running"}


def _parse_number(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).replace(" ", "").replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


def _safe_str(v: Any) -> str:
    return "" if v is None else str(v)


def _terminal_name(terminal_id: Optional[str]) -> str:
    if not terminal_id:
        return "—"
    return TERMINAL_MAP.get(terminal_id, terminal_id)


@app.get("/api/jps/orders")
def get_orders_summary(
    start: str = Query(..., description="ISO datetime start (e.g. 2025-01-01T00:00:00)"),
    end: str = Query(..., description="ISO datetime end (e.g. 2025-01-01T23:59:59)"),
    token: Optional[str] = Query(None, description="Override API token (optional)"),
    terminal: Optional[str] = Query(None, description="Filter by terminal id (optional)"),
):
    """
    Proxy to Joys Pizza external orders summary and normalize data by courier.

    Response structure:
    {
      "rows": [ { courier, terminalId, terminalName, orders, amount } ],
      "totals": { "orders": int, "couriers": int, "amount": float },
      "terminals": { id: name, ... }
    }
    """
    # Validate dates are parseable
    try:
        _ = datetime.fromisoformat(start)
        _ = datetime.fromisoformat(end)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid datetime format. Use ISO like 2025-01-01T00:00:00")

    params = {"start": start, "end": end}
    try:
        resp = requests.get(EXTERNAL_SUMMARY, params=params, headers=_auth_headers(token), timeout=20)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Upstream connection error: {str(e)}")

    if resp.status_code == 401:
        raise HTTPException(status_code=401, detail="Unauthorized: invalid API token")
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=f"Upstream error: {resp.text[:200]}")

    try:
        data = resp.json()
    except ValueError:
        raise HTTPException(status_code=502, detail="Invalid JSON from upstream")

    items: List[Dict[str, Any]] = []
    # The upstream may return either a list or a dict with 'data' key
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        if "data" in data and isinstance(data["data"], list):
            items = data["data"]
        elif "items" in data and isinstance(data["items"], list):
            items = data["items"]
        else:
            # If it's already aggregated per courier
            items = data.get("results", []) if isinstance(data.get("results"), list) else []

    # Aggregate by courier + terminal
    agg: Dict[str, Dict[str, Any]] = {}

    for it in items:
        # Try to detect fields across possible schemas
        courier = (
            _safe_str(it.get("courier"))
            or _safe_str(it.get("courierName"))
            or _safe_str(it.get("employee"))
            or _safe_str(it.get("driver"))
        )
        terminal_id = (
            _safe_str(it.get("terminalId"))
            or _safe_str(it.get("terminal"))
            or _safe_str(it.get("storeId"))
        )

        # If the item is already a summary row
        orders_count = it.get("ordersCount") or it.get("count") or it.get("orders")
        amount_val = it.get("totalAmount") or it.get("sum") or it.get("amount") or it.get("revenue")

        # If it's an order-level row
        if orders_count is None:
            orders_count = 1
        if amount_val is None:
            amount_val = it.get("totalPrice") or it.get("price") or it.get("orderSum")

        orders_count = int(_parse_number(orders_count)) if orders_count is not None else 0
        amount_val = _parse_number(amount_val)

        key = f"{courier}|{terminal_id}"
        if terminal and terminal_id != terminal:
            continue
        if key not in agg:
            agg[key] = {
                "courier": courier or "—",
                "terminalId": terminal_id or "",
                "orders": 0,
                "amount": 0.0,
            }
        agg[key]["orders"] += max(1, orders_count)
        agg[key]["amount"] += amount_val

    rows = []
    for v in agg.values():
        rows.append({
            **v,
            "terminalName": _terminal_name(v.get("terminalId")),
        })

    # Sort by terminal name then courier
    rows.sort(key=lambda r: (r.get("terminalName") or "", r.get("courier") or ""))

    totals = {
        "orders": int(sum(r.get("orders", 0) for r in rows)),
        "couriers": len({r.get("courier") for r in rows if r.get("courier")}),
        "amount": round(sum(r.get("amount", 0.0) for r in rows), 2),
    }

    return {
        "rows": rows,
        "totals": totals,
        "terminals": TERMINAL_MAP,
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
