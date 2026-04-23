import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from playerokapi import Account
from playerokapi.enums import ItemDealDirections, ItemDealStatuses

APP_TITLE = "Playerok Bridge API"
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "45"))
PLAYEROK_TOKEN = os.getenv("PLAYEROK_TOKEN", "").strip()
PLAYEROK_USER_AGENT = os.getenv("PLAYEROK_USER_AGENT", "").strip()
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "*").strip()

if not PLAYEROK_TOKEN:
    raise RuntimeError("PLAYEROK_TOKEN is not set")
if not PLAYEROK_USER_AGENT:
    raise RuntimeError("PLAYEROK_USER_AGENT is not set")

app = FastAPI(title=APP_TITLE)

if FRONTEND_ORIGIN == "*":
    allow_origins = ["*"]
else:
    allow_origins = [x.strip() for x in FRONTEND_ORIGIN.split(",") if x.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

lock = threading.Lock()
account: Optional[Account] = None
orders_cache: List[Dict[str, Any]] = []
stats_cache: Dict[str, Any] = {
    "reviews": 0,
    "rating": 0,
    "balance_total": 0,
    "balance_available": 0,
    "balance_frozen": 0,
    "balance_withdrawable": 0,
    "pending_income": 0,
    "pending_orders": 0,
    "completed_today": 0,
    "last_sync_at": None,
    "sync_ok": False,
}
last_error: Optional[str] = None


def enum_to_str(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "name"):
        return str(value.name)
    return str(value)


def parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def to_iso(value: Any) -> Optional[str]:
    dt = parse_dt(value)
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.isoformat()


def is_today(value: Any) -> bool:
    dt = parse_dt(value)
    if not dt:
        return False
    now_utc = datetime.now(timezone.utc).date()
    if dt.tzinfo is None:
        return dt.date() == now_utc
    return dt.astimezone(timezone.utc).date() == now_utc


def get_account() -> Account:
    global account
    if account is None:
        account = Account(
            token=PLAYEROK_TOKEN,
            user_agent=PLAYEROK_USER_AGENT,
        ).get()
    return account


def normalize_deal(deal: Any) -> Dict[str, Any]:
    item = getattr(deal, "item", None)
    buyer = getattr(deal, "buyer", None)
    seller = getattr(deal, "seller", None)

    return {
        "id": str(getattr(deal, "id", "")),
        "status": enum_to_str(getattr(deal, "status", "")),
        "previous_status": enum_to_str(getattr(deal, "previous_status", "")),
        "direction": enum_to_str(getattr(deal, "direction", "")),
        "price": getattr(deal, "price", 0) or 0,
        "created_at": to_iso(getattr(deal, "created_at", None)),
        "completed_at": to_iso(getattr(deal, "completed_at", None)),
        "status_description": getattr(deal, "status_description", None),
        "has_problem": bool(getattr(deal, "has_problem", False)),
        "item": {
            "id": str(getattr(item, "id", "")) if item else None,
            "name": getattr(item, "name", None) if item else None,
            "price": getattr(item, "price", None) if item else None,
            "raw_price": getattr(item, "raw_price", None) if item else None,
        },
        "buyer": {
            "id": str(getattr(buyer, "id", "")) if buyer else None,
            "username": getattr(buyer, "username", None) if buyer else None,
        },
        "seller": {
            "id": str(getattr(seller, "id", "")) if seller else None,
            "username": getattr(seller, "username", None) if seller else None,
        },
    }


def fetch_recent_outgoing_deals(max_pages: int = 5, page_size: int = 24) -> List[Dict[str, Any]]:
    acc = get_account()
    all_deals: List[Dict[str, Any]] = []
    after_cursor = None

    for _ in range(max_pages):
        page = acc.get_deals(
            count=page_size,
            direction=ItemDealDirections.OUT,
            after_cursor=after_cursor,
        )

        deals = getattr(page, "deals", []) or []
        for deal in deals:
            all_deals.append(normalize_deal(deal))

        page_info = getattr(page, "page_info", None)
        has_next = bool(getattr(page_info, "has_next_page", False)) if page_info else False
        end_cursor = getattr(page_info, "end_cursor", None) if page_info else None

        if not has_next or not end_cursor:
            break

        after_cursor = end_cursor

    all_deals.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return all_deals


def build_stats(deals: List[Dict[str, Any]]) -> Dict[str, Any]:
    acc = get_account().get()
    profile = getattr(acc, "profile", None)
    balance = getattr(profile, "balance", None) if profile else None

    pending_statuses = {"PAID", "PENDING", "SENT"}
    completed_statuses = {"CONFIRMED", "CONFIRMED_AUTOMATICALLY"}

    pending_orders = 0
    completed_today = 0

    for d in deals:
        status = d.get("status", "")
        if status in pending_statuses:
            pending_orders += 1
        completed_time = d.get("completed_at") or d.get("created_at")
        if status in completed_statuses and is_today(completed_time):
            completed_today += 1

    return {
        "reviews": getattr(profile, "reviews_count", 0) if profile else 0,
        "rating": getattr(profile, "rating", 0) if profile else 0,
        "balance_total": getattr(balance, "value", 0) if balance else 0,
        "balance_available": getattr(balance, "available", 0) if balance else 0,
        "balance_frozen": getattr(balance, "frozen", 0) if balance else 0,
        "balance_withdrawable": getattr(balance, "withdrawable", 0) if balance else 0,
        "pending_income": getattr(balance, "pending_income", 0) if balance else 0,
        "pending_orders": pending_orders,
        "completed_today": completed_today,
        "last_sync_at": datetime.now(timezone.utc).isoformat(),
        "sync_ok": True,
    }


def sync_once() -> None:
    global orders_cache, stats_cache, last_error

    try:
        deals = fetch_recent_outgoing_deals()
        stats = build_stats(deals)

        with lock:
            orders_cache = deals
            stats_cache = stats
            last_error = None
    except Exception as exc:
        with lock:
            last_error = str(exc)
            stats_cache = {
                **stats_cache,
                "last_sync_at": datetime.now(timezone.utc).isoformat(),
                "sync_ok": False,
            }


def background_sync_loop() -> None:
    while True:
        sync_once()
        time.sleep(SYNC_INTERVAL_SECONDS)


@app.on_event("startup")
def startup_event() -> None:
    sync_once()
    thread = threading.Thread(target=background_sync_loop, daemon=True)
    thread.start()


@app.get("/health")
def health() -> Dict[str, Any]:
    with lock:
        return {
            "ok": True,
            "sync_ok": stats_cache.get("sync_ok", False),
            "last_sync_at": stats_cache.get("last_sync_at"),
            "last_error": last_error,
        }


@app.get("/stats")
def get_stats() -> Dict[str, Any]:
    with lock:
        return stats_cache


@app.get("/orders")
def get_orders() -> Dict[str, Any]:
    with lock:
        return {
            "count": len(orders_cache),
            "orders": orders_cache,
        }


@app.post("/sync-now")
def sync_now() -> Dict[str, Any]:
    sync_once()
    with lock:
        return {
            "ok": stats_cache.get("sync_ok", False),
            "last_sync_at": stats_cache.get("last_sync_at"),
            "last_error": last_error,
            "orders_count": len(orders_cache),
        }


@app.post("/complete/{deal_id}")
def complete_order(deal_id: str) -> Dict[str, Any]:
    try:
        acc = get_account()
        updated = acc.update_deal(deal_id, ItemDealStatuses.SENT)
        sync_once()
        return {
            "ok": True,
            "deal_id": deal_id,
            "new_status": enum_to_str(getattr(updated, "status", "SENT")),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/confirm/{deal_id}")
def confirm_order(deal_id: str) -> Dict[str, Any]:
    try:
        acc = get_account()
        updated = acc.update_deal(deal_id, ItemDealStatuses.CONFIRMED)
        sync_once()
        return {
            "ok": True,
            "deal_id": deal_id,
            "new_status": enum_to_str(getattr(updated, "status", "CONFIRMED")),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
