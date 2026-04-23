import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
import importlib.util

import certifi
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------- FIX FOR PLAYEROKAPI CERT FILE ----------
# Библиотека ищет cacert.pem внутри playerokapi/, но в деплое файла может не быть.
spec = importlib.util.find_spec("playerokapi")
if spec and spec.submodule_search_locations:
    pkg_dir = Path(list(spec.submodule_search_locations)[0])
    target_cert = pkg_dir / "cacert.pem"
    if not target_cert.exists():
        target_cert.write_bytes(Path(certifi.where()).read_bytes())

from playerokapi.account import Account
from playerokapi.enums import (
    ItemDealDirections,
    ItemDealStatuses,
    ItemStatuses,
    PriorityTypes,
    TransactionProviderIds,
)

APP_TITLE = "Playerok Bridge API"
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "30"))
PLAYEROK_TOKEN = os.getenv("PLAYEROK_TOKEN", "").strip()
PLAYEROK_DDG5 = os.getenv("PLAYEROK_DDG5", "").strip()
PLAYEROK_USER_AGENT = os.getenv("PLAYEROK_USER_AGENT", "").strip()
PLAYEROK_COOKIES = os.getenv("PLAYEROK_COOKIES", "").strip()
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "*").strip()

# Прокси через env
HTTP_PROXY = os.getenv("HTTP_PROXY", "").strip()
HTTPS_PROXY = os.getenv("HTTPS_PROXY", "").strip()

if HTTP_PROXY:
    os.environ["HTTP_PROXY"] = HTTP_PROXY
    os.environ["http_proxy"] = HTTP_PROXY

if HTTPS_PROXY:
    os.environ["HTTPS_PROXY"] = HTTPS_PROXY
    os.environ["https_proxy"] = HTTPS_PROXY
elif HTTP_PROXY:
    os.environ["HTTPS_PROXY"] = HTTP_PROXY
    os.environ["https_proxy"] = HTTP_PROXY

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


class BumpRequest(BaseModel):
    productId: Optional[str] = None
    itemId: Optional[str] = None
    slug: Optional[str] = None
    lotUrl: Optional[str] = None
    priorityStatusId: Optional[str] = None


class RelistRequest(BaseModel):
    orderId: Optional[str] = None
    productId: Optional[str] = None
    itemId: Optional[str] = None
    slug: Optional[str] = None
    lotUrl: Optional[str] = None
    priorityStatusId: Optional[str] = None


lock = threading.Lock()
account: Optional[Account] = None
orders_cache: List[Dict[str, Any]] = []
items_cache: List[Dict[str, Any]] = []
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


DONE_STATUSES = {
    "CONFIRMED",
    "CONFIRMED_AUTOMATICALLY",
    "DONE",
    "COMPLETED",
    "DELIVERED",
}
PENDING_STATUSES = {"PAID", "PENDING", "SENT"}
DEFAULT_PRIORITY_STATUS_ID = "1f00f21b-7768-62a0-296f-75a31ee8ce72"


def parse_cookie_string(raw: str) -> Optional[Dict[str, str]]:
    if not raw:
        return None
    raw = raw.strip()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {str(k): str(v) for k, v in parsed.items()}
    except Exception:
        pass

    cookies: Dict[str, str] = {}
    for chunk in raw.split(";"):
        if "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            cookies[key] = value
    return cookies or None


PARSED_COOKIES = parse_cookie_string(PLAYEROK_COOKIES)


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


def playerok_item_url(slug: Optional[str]) -> Optional[str]:
    if not slug:
        return None
    return f"https://playerok.com/products/{slug}"


def extract_slug_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    text = str(url).strip()
    if not text:
        return None
    if "/" not in text and " " not in text and "?" not in text:
        return text.strip("/")
    try:
        parsed = urlparse(text)
        parts = [p for p in parsed.path.split("/") if p]
        if not parts:
            return None
        if "products" in parts:
            idx = parts.index("products")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        return parts[-1]
    except Exception:
        return text.rstrip("/").split("/")[-1].split("?")[0] or None


def get_account() -> Account:
    global account
    if account is not None:
        return account

    kwargs: Dict[str, Any] = {}
    if PARSED_COOKIES:
        kwargs["cookies"] = PARSED_COOKIES
    else:
        if not PLAYEROK_TOKEN:
            raise RuntimeError("PLAYEROK_TOKEN is not set")
        kwargs["token"] = PLAYEROK_TOKEN
        if PLAYEROK_DDG5:
            kwargs["ddg5"] = PLAYEROK_DDG5

    if PLAYEROK_USER_AGENT:
        kwargs["user_agent"] = PLAYEROK_USER_AGENT

    proxy = HTTPS_PROXY or HTTP_PROXY or ""
    if proxy:
        kwargs["proxy"] = proxy

    account = Account(**kwargs)
    return account


def normalize_item(item: Any) -> Dict[str, Any]:
    category = getattr(item, "category", None)
    game = getattr(item, "game", None)
    attachment = getattr(item, "attachment", None)

    slug = getattr(item, "slug", None)
    return {
        "id": str(getattr(item, "id", "") or ""),
        "slug": slug,
        "url": playerok_item_url(slug),
        "name": getattr(item, "name", None),
        "status": enum_to_str(getattr(item, "status", None)),
        "price": getattr(item, "price", 0) or 0,
        "raw_price": getattr(item, "raw_price", None) or getattr(item, "price", 0) or 0,
        "priority": enum_to_str(getattr(item, "priority", None)),
        "priority_position": getattr(item, "priority_position", None),
        "sequence": getattr(item, "sequence", None),
        "views_counter": getattr(item, "views_counter", None),
        "fee_multiplier": getattr(item, "fee_multiplier", None),
        "category": {
            "id": str(getattr(category, "id", "") or "") if category else None,
            "name": getattr(category, "name", None) if category else None,
            "slug": getattr(category, "slug", None) if category else None,
        },
        "game": {
            "id": str(getattr(game, "id", "") or "") if game else None,
            "name": getattr(game, "name", None) if game else None,
            "slug": getattr(game, "slug", None) if game else None,
        },
        "attachment": {
            "url": getattr(attachment, "url", None),
        } if attachment else None,
        "created_at": to_iso(getattr(item, "created_at", None)),
        "approval_date": to_iso(getattr(item, "approval_date", None)),
    }


def normalize_deal(deal: Any) -> Dict[str, Any]:
    item = getattr(deal, "item", None)
    buyer = getattr(deal, "buyer", None)
    seller = getattr(deal, "seller", None)
    chat = getattr(deal, "chat", None)

    slug = getattr(item, "slug", None) if item else None
    lot_url = playerok_item_url(slug)
    chat_id = getattr(chat, "id", None) if chat else None
    chat_url = (
        getattr(deal, "chat_url", None)
        or getattr(chat, "url", None)
        or getattr(chat, "public_url", None)
        or (f"https://playerok.com/chats/{chat_id}" if chat_id else None)
    )

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
        "lot_url": lot_url,
        "item_url": lot_url,
        "chat_url": chat_url,
        "buyer_chat_url": chat_url,
        "item": {
            "id": str(getattr(item, "id", "")) if item else None,
            "slug": slug if item else None,
            "url": lot_url,
            "public_url": lot_url,
            "link": lot_url,
            "name": getattr(item, "name", None) if item else None,
            "price": getattr(item, "price", None) if item else None,
            "raw_price": getattr(item, "raw_price", None) if item else None,
            "priority_position": getattr(item, "priority_position", None) if item else None,
            "category": getattr(getattr(item, "category", None), "name", None) if item else None,
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
    acc = get_account().get()
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


def fetch_my_items(max_pages: int = 5, page_size: int = 24) -> List[Dict[str, Any]]:
    acc = get_account().get()
    user = acc.get_user(username=acc.username)
    statuses = [
        ItemStatuses.APPROVED,
        ItemStatuses.SOLD,
        ItemStatuses.EXPIRED,
        ItemStatuses.DRAFT,
        ItemStatuses.PENDING_MODERATION,
        ItemStatuses.PENDING_APPROVAL,
        ItemStatuses.DECLINED,
        ItemStatuses.BLOCKED,
    ]

    all_items: List[Dict[str, Any]] = []
    after_cursor = None

    for _ in range(max_pages):
        page = user.get_items(count=page_size, statuses=statuses, after_cursor=after_cursor)
        page_items = getattr(page, "items", []) or []
        for item in page_items:
            all_items.append(normalize_item(item))

        page_info = getattr(page, "page_info", None)
        has_next = bool(getattr(page_info, "has_next_page", False)) if page_info else False
        end_cursor = getattr(page_info, "end_cursor", None) if page_info else None
        if not has_next or not end_cursor:
            break
        after_cursor = end_cursor

    unique: Dict[str, Dict[str, Any]] = {}
    for item in all_items:
        if item["id"]:
            unique[item["id"]] = item
    return list(unique.values())


def find_cached_item(item_id: Optional[str] = None, slug: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if item_id:
        for item in items_cache:
            if item.get("id") == str(item_id):
                return item
    if slug:
        for item in items_cache:
            if item.get("slug") == slug:
                return item
    return None


def get_item_live(item_id: Optional[str] = None, slug: Optional[str] = None) -> Any:
    acc = get_account().get()
    if item_id:
        return acc.get_item(id=str(item_id))
    if slug:
        return acc.get_item(slug=slug)
    raise RuntimeError("Item id/slug is missing")


def resolve_item(item_id: Optional[str] = None, slug: Optional[str] = None, lot_url: Optional[str] = None) -> Any:
    slug = slug or extract_slug_from_url(lot_url)

    try:
        return get_item_live(item_id=item_id, slug=slug)
    except Exception:
        cached = find_cached_item(item_id=item_id, slug=slug)
        if cached:
            return get_item_live(item_id=cached.get("id"), slug=cached.get("slug"))
        raise


def choose_priority_status(item: Any, for_relist: bool = False, requested_id: Optional[str] = None) -> str:
    acc = get_account().get()
    price = int(getattr(item, "raw_price", None) or getattr(item, "price", 0) or 0)

    try:
        statuses = acc.get_item_priority_statuses(item_id=str(getattr(item, "id")), item_price=price)
    except Exception:
        statuses = []

    statuses = list(statuses or [])
    if requested_id:
        for status in statuses:
            if str(getattr(status, "id", "")) == str(requested_id):
                return str(getattr(status, "id"))

    if statuses:
        premium = [s for s in statuses if enum_to_str(getattr(s, "type", None)) == enum_to_str(PriorityTypes.PREMIUM)]
        default = [s for s in statuses if enum_to_str(getattr(s, "type", None)) == enum_to_str(PriorityTypes.DEFAULT)]
        bucket = default if for_relist and default else premium if premium else default if default else statuses
        bucket = sorted(bucket, key=lambda s: (int(getattr(s, "price", 0) or 0), int(getattr(s, "period", 0) or 0)))
        if bucket:
            return str(getattr(bucket[0], "id"))

    return DEFAULT_PRIORITY_STATUS_ID


MINIMAL_PUBLISH_QUERY = """
mutation publishItem($input: PublishItemInput!) {
  publishItem(input: $input) {
    id
    slug
    name
    status
    price
    rawPrice
    priorityPosition
    __typename
  }
}
""".strip()

MINIMAL_BUMP_QUERY = """
mutation increaseItemPriorityStatus($input: PublishItemInput!) {
  increaseItemPriorityStatus(input: $input) {
    id
    slug
    name
    status
    price
    rawPrice
    priorityPosition
    __typename
  }
}
""".strip()


def graphql_publish(item_id: str, priority_status_id: str) -> Dict[str, Any]:
    acc = get_account()
    payload = {
        "operationName": "publishItem",
        "query": MINIMAL_PUBLISH_QUERY,
        "variables": {
            "input": {
                "itemId": item_id,
                "priorityStatuses": [priority_status_id],
                "transactionProviderId": "LOCAL",
                "transactionProviderData": {"paymentMethodId": None},
            }
        },
    }
    response = acc.request("post", f"{acc.base_url}/graphql", {"accept": "*/*"}, payload).json()
    return response.get("data", {}).get("publishItem") or {}


def graphql_bump(item_id: str, priority_status_id: str) -> Dict[str, Any]:
    acc = get_account()
    payload = {
        "operationName": "increaseItemPriorityStatus",
        "query": MINIMAL_BUMP_QUERY,
        "variables": {
            "input": {
                "itemId": item_id,
                "priorityStatuses": [priority_status_id],
                "transactionProviderId": "LOCAL",
                "transactionProviderData": {"paymentMethodId": None},
            }
        },
    }
    response = acc.request("post", f"{acc.base_url}/graphql", {"accept": "*/*"}, payload).json()
    return response.get("data", {}).get("increaseItemPriorityStatus") or {}


def perform_bump(item: Any, requested_id: Optional[str] = None) -> Dict[str, Any]:
    item_id = str(getattr(item, "id"))
    priority_status_id = choose_priority_status(item, for_relist=False, requested_id=requested_id)
    acc = get_account().get()

    try:
        updated = acc.increase_item_priority_status(
            item_id=item_id,
            priority_status_id=priority_status_id,
            transaction_provider_id=TransactionProviderIds.LOCAL,
        )
        return {
            "item_id": item_id,
            "slug": getattr(updated, "slug", getattr(item, "slug", None)),
            "lot_url": playerok_item_url(getattr(updated, "slug", getattr(item, "slug", None))),
            "priority_status_id": priority_status_id,
            "status": enum_to_str(getattr(updated, "status", None)),
            "priority_position": getattr(updated, "priority_position", None),
            "ok": True,
            "mode": "playerokapi",
        }
    except Exception:
        raw = graphql_bump(item_id=item_id, priority_status_id=priority_status_id)
        return {
            "item_id": item_id,
            "slug": raw.get("slug") or getattr(item, "slug", None),
            "lot_url": playerok_item_url(raw.get("slug") or getattr(item, "slug", None)),
            "priority_status_id": priority_status_id,
            "status": raw.get("status") or enum_to_str(getattr(item, "status", None)),
            "priority_position": raw.get("priorityPosition"),
            "ok": True,
            "mode": "graphql-fallback",
        }


def perform_relist(item: Any, requested_id: Optional[str] = None) -> Dict[str, Any]:
    item_id = str(getattr(item, "id"))
    priority_status_id = choose_priority_status(item, for_relist=True, requested_id=requested_id)
    acc = get_account().get()

    try:
        updated = acc.publish_item(
            item_id=item_id,
            priority_status_id=priority_status_id,
            transaction_provider_id=TransactionProviderIds.LOCAL,
        )
        return {
            "item_id": item_id,
            "slug": getattr(updated, "slug", getattr(item, "slug", None)),
            "lot_url": playerok_item_url(getattr(updated, "slug", getattr(item, "slug", None))),
            "priority_status_id": priority_status_id,
            "status": enum_to_str(getattr(updated, "status", None)),
            "priority_position": getattr(updated, "priority_position", None),
            "ok": True,
            "mode": "playerokapi",
        }
    except Exception:
        raw = graphql_publish(item_id=item_id, priority_status_id=priority_status_id)
        return {
            "item_id": item_id,
            "slug": raw.get("slug") or getattr(item, "slug", None),
            "lot_url": playerok_item_url(raw.get("slug") or getattr(item, "slug", None)),
            "priority_status_id": priority_status_id,
            "status": raw.get("status") or enum_to_str(getattr(item, "status", None)),
            "priority_position": raw.get("priorityPosition"),
            "ok": True,
            "mode": "graphql-fallback",
        }


def build_stats(deals: List[Dict[str, Any]]) -> Dict[str, Any]:
    acc = get_account().get()
    profile = getattr(acc, "profile", None)
    balance = getattr(profile, "balance", None) if profile else None

    pending_orders = 0
    completed_today = 0

    for d in deals:
        status = d.get("status", "")
        if status in PENDING_STATUSES:
            pending_orders += 1
        completed_time = d.get("completed_at") or d.get("created_at")
        if status in DONE_STATUSES and is_today(completed_time):
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
    global orders_cache, items_cache, stats_cache, last_error
    try:
        deals = fetch_recent_outgoing_deals()
        items = fetch_my_items()
        stats = build_stats(deals)

        with lock:
            orders_cache = deals
            items_cache = items
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
            "proxy_enabled": bool(HTTP_PROXY or HTTPS_PROXY),
            "auth_mode": "cookies" if PARSED_COOKIES else "token",
            "has_ddg5": bool(PLAYEROK_DDG5 or (PARSED_COOKIES and PARSED_COOKIES.get("__ddg5_"))),
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


@app.get("/items")
def get_items_endpoint() -> Dict[str, Any]:
    with lock:
        return {
            "count": len(items_cache),
            "items": items_cache,
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
            "items_count": len(items_cache),
        }


@app.post("/complete/{deal_id}")
def complete_order(deal_id: str) -> Dict[str, Any]:
    try:
        acc = get_account().get()
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
        acc = get_account().get()
        updated = acc.update_deal(deal_id, ItemDealStatuses.CONFIRMED)
        sync_once()
        return {
            "ok": True,
            "deal_id": deal_id,
            "new_status": enum_to_str(getattr(updated, "status", "CONFIRMED")),
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/bump")
def bump_item(payload: BumpRequest) -> Dict[str, Any]:
    try:
        item = resolve_item(item_id=payload.itemId, slug=payload.slug, lot_url=payload.lotUrl)
        result = perform_bump(item=item, requested_id=payload.priorityStatusId)
        sync_once()
        return result
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/relist")
def relist_item(payload: RelistRequest) -> Dict[str, Any]:
    try:
        item = resolve_item(item_id=payload.itemId, slug=payload.slug, lot_url=payload.lotUrl)
        result = perform_relist(item=item, requested_id=payload.priorityStatusId)
        sync_once()
        return {
            **result,
            "order_id": payload.orderId,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
