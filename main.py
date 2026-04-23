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
AUTO_RELIST_ENABLED = os.getenv("AUTO_RELIST_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
AUTO_RELIST_INTERVAL_SECONDS = int(os.getenv("AUTO_RELIST_INTERVAL_SECONDS", "90"))
BINDINGS_PATH = Path(os.getenv("BINDINGS_PATH", "product_bindings.json")).resolve()

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


class BindingUpsertRequest(BaseModel):
    productId: str
    title: Optional[str] = None
    itemId: Optional[str] = None
    slug: Optional[str] = None
    lotUrl: Optional[str] = None
    priorityStatusId: Optional[str] = None
    autoRelist: Optional[bool] = None
    matchText: Optional[str] = None
    notes: Optional[str] = None


lock = threading.Lock()
account: Optional[Account] = None
orders_cache: List[Dict[str, Any]] = []
items_cache: List[Dict[str, Any]] = []
product_bindings: Dict[str, Dict[str, Any]] = {}
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
RELISTABLE_ITEM_STATUSES = {
    "SOLD",
    "EXPIRED",
    "DECLINED",
    "BLOCKED",
    "DRAFT",
}
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


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_bindings() -> Dict[str, Dict[str, Any]]:
    ensure_parent_dir(BINDINGS_PATH)
    if not BINDINGS_PATH.exists():
        BINDINGS_PATH.write_text("{}", encoding="utf-8")
        return {}
    try:
        raw = json.loads(BINDINGS_PATH.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return {str(k): v for k, v in raw.items() if isinstance(v, dict)}
    except Exception:
        pass
    return {}


def save_bindings() -> None:
    ensure_parent_dir(BINDINGS_PATH)
    BINDINGS_PATH.write_text(
        json.dumps(product_bindings, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_binding(product_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    lot_url = (data.get("lotUrl") or data.get("lot_url") or "").strip() or None
    slug = (data.get("slug") or extract_slug_from_url(lot_url) or "").strip() or None
    item_id = data.get("itemId") or data.get("item_id")
    priority_status_id = data.get("priorityStatusId") or data.get("priority_status_id")
    return {
        "productId": str(product_id),
        "title": data.get("title") or None,
        "itemId": str(item_id) if item_id not in (None, "") else None,
        "slug": slug,
        "lotUrl": lot_url or (playerok_item_url(slug) if slug else None),
        "priorityStatusId": str(priority_status_id) if priority_status_id not in (None, "") else None,
        "autoRelist": bool(data.get("autoRelist", data.get("auto_relist", False))),
        "matchText": data.get("matchText") or data.get("match_text") or None,
        "notes": data.get("notes") or None,
        "updatedAt": data.get("updatedAt") or data.get("updated_at") or datetime.now(timezone.utc).isoformat(),
    }


def upsert_binding(product_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    current = product_bindings.get(str(product_id), {})
    merged = {**current, **payload}
    normalized = normalize_binding(str(product_id), merged)
    product_bindings[str(product_id)] = normalized
    save_bindings()
    return normalized


def binding_for_product(product_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not product_id:
        return None
    return product_bindings.get(str(product_id))


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

    slug = getattr(item, "slug", None) if item else None
    lot_url = playerok_item_url(slug)

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


def find_cached_order(order_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not order_id:
        return None
    for order in orders_cache:
        if order.get("id") == str(order_id):
            return order
    return None


def get_item_live(item_id: Optional[str] = None, slug: Optional[str] = None) -> Any:
    acc = get_account().get()
    if item_id:
        return acc.get_item(id=str(item_id))
    if slug:
        return acc.get_item(slug=slug)
    raise RuntimeError("Item id/slug is missing")


def resolve_item(
    item_id: Optional[str] = None,
    slug: Optional[str] = None,
    lot_url: Optional[str] = None,
    product_id: Optional[str] = None,
    order_id: Optional[str] = None,
) -> Any:
    binding = binding_for_product(product_id)
    if binding:
        item_id = item_id or binding.get("itemId")
        slug = slug or binding.get("slug")
        lot_url = lot_url or binding.get("lotUrl")

    order = find_cached_order(order_id)
    if order:
        order_item = order.get("item") or {}
        item_id = item_id or order_item.get("id")
        slug = slug or order_item.get("slug")
        lot_url = lot_url or order.get("lot_url") or order_item.get("url")

    slug = slug or extract_slug_from_url(lot_url)

    try:
        item = get_item_live(item_id=item_id, slug=slug)
    except Exception:
        cached = find_cached_item(item_id=item_id, slug=slug)
        if cached:
            item = get_item_live(item_id=cached.get("id"), slug=cached.get("slug"))
        else:
            raise

    if product_id:
        upsert_payload = {
            "itemId": str(getattr(item, "id", "") or "") or None,
            "slug": getattr(item, "slug", None),
            "lotUrl": playerok_item_url(getattr(item, "slug", None)),
        }
        if binding:
            if binding.get("title"):
                upsert_payload["title"] = binding.get("title")
            if binding.get("priorityStatusId"):
                upsert_payload["priorityStatusId"] = binding.get("priorityStatusId")
            upsert_payload["autoRelist"] = bool(binding.get("autoRelist", False))
            if binding.get("matchText"):
                upsert_payload["matchText"] = binding.get("matchText")
        upsert_binding(str(product_id), upsert_payload)

    return item


def choose_priority_status(
    item: Any,
    for_relist: bool = False,
    requested_id: Optional[str] = None,
    product_id: Optional[str] = None,
) -> str:
    binding = binding_for_product(product_id)
    requested_id = requested_id or (binding.get("priorityStatusId") if binding else None)

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

    return requested_id or DEFAULT_PRIORITY_STATUS_ID


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


def perform_bump(item: Any, requested_id: Optional[str] = None, product_id: Optional[str] = None) -> Dict[str, Any]:
    item_id = str(getattr(item, "id"))
    priority_status_id = choose_priority_status(
        item,
        for_relist=False,
        requested_id=requested_id,
        product_id=product_id,
    )
    acc = get_account().get()

    try:
        updated = acc.increase_item_priority_status(
            item_id=item_id,
            priority_status_id=priority_status_id,
            transaction_provider_id=TransactionProviderIds.LOCAL,
        )
        result = {
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
        result = {
            "item_id": item_id,
            "slug": raw.get("slug") or getattr(item, "slug", None),
            "lot_url": playerok_item_url(raw.get("slug") or getattr(item, "slug", None)),
            "priority_status_id": priority_status_id,
            "status": raw.get("status") or enum_to_str(getattr(item, "status", None)),
            "priority_position": raw.get("priorityPosition"),
            "ok": True,
            "mode": "graphql-fallback",
        }

    if product_id:
        upsert_binding(str(product_id), {
            "itemId": result.get("item_id"),
            "slug": result.get("slug"),
            "lotUrl": result.get("lot_url"),
            "priorityStatusId": result.get("priority_status_id"),
        })
    return result


def perform_relist(item: Any, requested_id: Optional[str] = None, product_id: Optional[str] = None) -> Dict[str, Any]:
    item_id = str(getattr(item, "id"))
    priority_status_id = choose_priority_status(
        item,
        for_relist=True,
        requested_id=requested_id,
        product_id=product_id,
    )
    acc = get_account().get()

    try:
        updated = acc.publish_item(
            item_id=item_id,
            priority_status_id=priority_status_id,
            transaction_provider_id=TransactionProviderIds.LOCAL,
        )
        result = {
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
        result = {
            "item_id": item_id,
            "slug": raw.get("slug") or getattr(item, "slug", None),
            "lot_url": playerok_item_url(raw.get("slug") or getattr(item, "slug", None)),
            "priority_status_id": priority_status_id,
            "status": raw.get("status") or enum_to_str(getattr(item, "status", None)),
            "priority_position": raw.get("priorityPosition"),
            "ok": True,
            "mode": "graphql-fallback",
        }

    if product_id:
        upsert_binding(str(product_id), {
            "itemId": result.get("item_id"),
            "slug": result.get("slug"),
            "lotUrl": result.get("lot_url"),
            "priorityStatusId": result.get("priority_status_id"),
        })
    return result


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


def maybe_auto_relist_once() -> None:
    if not AUTO_RELIST_ENABLED:
        return

    for product_id, binding in list(product_bindings.items()):
        if not binding.get("autoRelist"):
            continue
        try:
            item = resolve_item(
                item_id=binding.get("itemId"),
                slug=binding.get("slug"),
                lot_url=binding.get("lotUrl"),
                product_id=product_id,
            )
            status = enum_to_str(getattr(item, "status", None))
            if status in RELISTABLE_ITEM_STATUSES:
                perform_relist(item, requested_id=binding.get("priorityStatusId"), product_id=product_id)
        except Exception:
            continue


def background_sync_loop() -> None:
    while True:
        sync_once()
        try:
            maybe_auto_relist_once()
        except Exception:
            pass
        time.sleep(SYNC_INTERVAL_SECONDS)


@app.on_event("startup")
def startup_event() -> None:
    global product_bindings
    product_bindings = load_bindings()
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
            "bindings_count": len(product_bindings),
            "auto_relist_enabled": AUTO_RELIST_ENABLED,
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


@app.get("/lots")
def get_lots_endpoint() -> Dict[str, Any]:
    with lock:
        mapped: List[Dict[str, Any]] = []
        for item in items_cache:
            item_id = str(item.get("id") or "")
            attached_bindings = [b for b in product_bindings.values() if (b.get("itemId") and str(b.get("itemId")) == item_id) or (b.get("slug") and b.get("slug") == item.get("slug"))]
            mapped.append({
                **item,
                "bindings": attached_bindings,
                "bound_product_ids": [b.get("productId") for b in attached_bindings],
            })
        return {
            "count": len(mapped),
            "lots": mapped,
            "bindings_count": len(product_bindings),
        }


@app.get("/bindings")
def get_bindings() -> Dict[str, Any]:
    return {
        "count": len(product_bindings),
        "bindings": list(product_bindings.values()),
    }


@app.post("/bindings/upsert")
def bindings_upsert(payload: BindingUpsertRequest) -> Dict[str, Any]:
    binding = upsert_binding(payload.productId, payload.model_dump(exclude_none=True))
    return {
        "ok": True,
        "binding": binding,
    }


@app.delete("/bindings/{product_id}")
def bindings_delete(product_id: str) -> Dict[str, Any]:
    existed = product_bindings.pop(str(product_id), None)
    save_bindings()
    return {
        "ok": True,
        "deleted": bool(existed),
        "productId": str(product_id),
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
            "bindings_count": len(product_bindings),
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
        item = resolve_item(
            item_id=payload.itemId,
            slug=payload.slug,
            lot_url=payload.lotUrl,
            product_id=payload.productId,
        )
        result = perform_bump(item=item, requested_id=payload.priorityStatusId, product_id=payload.productId)
        sync_once()
        return result
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/relist")
def relist_item(payload: RelistRequest) -> Dict[str, Any]:
    try:
        item = resolve_item(
            item_id=payload.itemId,
            slug=payload.slug,
            lot_url=payload.lotUrl,
            product_id=payload.productId,
            order_id=payload.orderId,
        )
        result = perform_relist(item=item, requested_id=payload.priorityStatusId, product_id=payload.productId)
        sync_once()
        return {
            **result,
            "order_id": payload.orderId,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/auto-relist/run")
def auto_relist_run() -> Dict[str, Any]:
    try:
        maybe_auto_relist_once()
        sync_once()
        return {
            "ok": True,
            "bindings_count": len(product_bindings),
            "auto_relist_enabled": AUTO_RELIST_ENABLED,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
