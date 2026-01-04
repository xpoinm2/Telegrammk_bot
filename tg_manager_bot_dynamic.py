from __future__ import annotations  # Updated

import asyncio
import base64
import contextlib
import os
import json
import logging
import sys
import random
import secrets
import html
import re
import shutil
import socket
import mimetypes
from dataclasses import dataclass
from datetime import datetime
from collections import OrderedDict, defaultdict, deque
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional, Any, List, Tuple, Set, TYPE_CHECKING, Callable, cast
from io import BytesIO
from telethon import TelegramClient, events, Button, functions, helpers, types
from OpenAi_helper import generate_dating_ai_variants, recommend_dating_ai_variant
from telethon.utils import get_display_name
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    PeerIdInvalidError,
)

try:  # Telethon <= 1.33.1
    from telethon.errors import QueryIdInvalidError  # type: ignore[attr-defined]
except ImportError:  # Telethon >= 1.34 moved/renamed the error
    from telethon.errors.rpcerrorlist import QueryIdInvalidError  # type: ignore[attr-defined]
from telethon.tl.types import ReactionEmoji, User

if TYPE_CHECKING:
    from telethon.tl.custom.inlinebuilder import InlineBuilder
from telethon.tl.custom import Message
try:  # Telethon <= 1.33.1
    from telethon.errors import AuthKeyDuplicatedError  # type: ignore[attr-defined]
except ImportError:  # Telethon >= 1.34
    try:
        from telethon.errors.rpcerrorlist import AuthKeyDuplicatedError  # type: ignore[attr-defined]
    except ImportError:  # pragma: no cover - fallback safety
        class AuthKeyDuplicatedError(RuntimeError):  # type: ignore[override]
            """Fallback placeholder when Telethon is missing the error."""
            pass

try:  # Telethon <= 1.33.1
    from telethon.errors import PhoneCodeFloodError  # type: ignore[attr-defined]
except ImportError:  # Telethon >= 1.34 moved/renamed the error
    try:
        from telethon.errors.rpcerrorlist import PhoneCodeFloodError  # type: ignore[attr-defined]
    except ImportError:
        from telethon.errors.rpcerrorlist import (
            PhoneNumberFloodError as PhoneCodeFloodError,  # type: ignore[attr-defined]
        )
try:  # Telethon <= 1.33.1
    from telethon.errors import (  # type: ignore[attr-defined]
        UserDeactivatedError,
        UserDeactivatedBanError,
        PhoneNumberBannedError,
    )
except ImportError:  # Telethon >= 1.34 moved/renamed the errors
    from telethon.errors.rpcerrorlist import (  # type: ignore[attr-defined]
        UserDeactivatedError,
        UserDeactivatedBanError,
        PhoneNumberBannedError,
    )

import socks  # PySocks

from telethon.network.connection.connection import python_socks
from telethon.network.connection.tcpfull import ConnectionTcpFull as _ConnectionTcpFull

# On Windows, prefer the selector-based event loop for compatibility with
# libraries that expect the pre-3.8 default behaviour.
if sys.platform.startswith("win"):
    selector_policy_factory = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
    if selector_policy_factory is not None:
        with contextlib.suppress(Exception):
            asyncio.set_event_loop_policy(selector_policy_factory())

# ================== LOGGING (console + file) ==================
LOG_FILE = "bot.log"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.addHandler(ch)

fh = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)

log = logging.getLogger("mgrbot")

# ================== CONFIG ==================

# До 5 API ключей — новые аккаунты распределяются по кругу
# ВСТАВЬ СВОИ ДАННЫЕ НИЖЕ:
API_KEYS = [
    {"api_id": 29762521, "api_hash": "23b2fbb113e33642cd669159afc51f54"},
    {"api_id": 24266525, "api_hash": "8499988b06e7991e900bce3178eabbb8"},
    {"api_id": 20149796, "api_hash": "ece55838826c41f32c4ccf4cbe74eee4"},
]

BOT_TOKEN = "8263496850:AAFks1scz-bIMTInNJ3wyirNoZXbWR7hkHU"   # токен бота от @BotFather
# Имя пользователя бота устанавливается во время запуска через get_me().
BOT_USERNAME: Optional[str] = None
# Изначальные супер-администраторы, которые могут выдавать доступ другим пользователям
ROOT_ADMIN_IDS = {5760263106, 7519364639, 6587523771, 8412294171}

# Статичный приватный SOCKS5-прокси (используется ботом и как дефолт для аккаунтов)
# Боту больше не требуется прокси, поэтому он выключен. Чтобы вернуть подключение
# через прокси, установите enabled=True и задайте параметры вручную.
PRIVATE_PROXY = {
    "enabled": False,
    "dynamic": False,
}

# Необязательный периодический авто-reconnect для обновления IP (минуты).
# 0 — выключено.
AUTO_RECONNECT_MINUTES = 0

# Небольшая рандомная задержка перед критичными запросами (код, логин и т.п.)
LOGIN_DELAY_SECONDS = (5, 15)

# Базовый интервал keepalive с шумом, чтобы избежать синхронных запросов
KEEPALIVE_INTERVAL_SECONDS = 90
KEEPALIVE_JITTER = (20, 60)

# Расширенные профили устройств/версий
DEVICE_PROFILES: List[Dict[str, str]] = [
    {"device_model":"iPhone 12", "system_version":"16.4", "app_version":"10.9.0",  "lang_code":"en"},
    {"device_model":"iPhone 13 Pro", "system_version":"17.1", "app_version":"10.10.2","lang_code":"ru"},
    {"device_model":"iPhone SE (2nd)", "system_version":"15.7","app_version":"10.6.1","lang_code":"en"},
    {"device_model":"iPad Pro 11", "system_version":"17.0","app_version":"9.6.0",   "lang_code":"en"},
    {"device_model":"iPhone XR", "system_version":"14.8", "app_version":"8.9.1",    "lang_code":"en"},
    {"device_model":"Pixel 7", "system_version":"13", "app_version":"9.5.1",        "lang_code":"en"},
    {"device_model":"Pixel 8 Pro", "system_version":"14", "app_version":"10.7.1",   "lang_code":"en"},
    {"device_model":"Samsung S22", "system_version":"14", "app_version":"10.8.1",   "lang_code":"en"},
    {"device_model":"Xiaomi 13", "system_version":"13", "app_version":"9.6.3",      "lang_code":"ru"},
    {"device_model":"OnePlus 11", "system_version":"14", "app_version":"10.7.0",    "lang_code":"en"},
    {"device_model":"Huawei P30", "system_version":"12", "app_version":"9.3.5",     "lang_code":"en"},
    {"device_model":"Honor 90", "system_version":"14", "app_version":"10.5.3",      "lang_code":"ru"},
    {"device_model":"Windows 10 PC", "system_version":"10","app_version":"4.16",    "lang_code":"en"},
    {"device_model":"Windows 11", "system_version":"11",  "app_version":"4.12",     "lang_code":"ru"},
    {"device_model":"MacBook Pro", "system_version":"14.5","app_version":"4.16",    "lang_code":"en"},
    {"device_model":"Linux Desktop","system_version":"6.9", "app_version":"4.8",    "lang_code":"en"},
    {"device_model":"Nexus 5X", "system_version":"8.1", "app_version":"7.9.2",      "lang_code":"en"},
    {"device_model":"Galaxy S9", "system_version":"10", "app_version":"8.4.4",      "lang_code":"ru"},
    {"device_model":"iPhone 8", "system_version":"14.8","app_version":"8.7.1",      "lang_code":"en"},
    {"device_model":"Xiaomi Pad 6","system_version":"14","app_version":"10.9.1",    "lang_code":"en"},
]

SESSIONS_DIR = "sessions"; os.makedirs(SESSIONS_DIR, exist_ok=True)
LIBRARY_DIR = "library"
# Директории будут создаваться внутри каталога пользователя
PASTES_DIR = os.path.join(LIBRARY_DIR, "pastes")
VOICES_DIR = os.path.join(LIBRARY_DIR, "voices")
VIDEO_DIR = os.path.join(LIBRARY_DIR, "video")
STICKERS_DIR = os.path.join(LIBRARY_DIR, "stickers")
PROXIES_DIR = os.path.join(LIBRARY_DIR, "proxies")
TEXT_EXTENSIONS = {".txt", ".md"}
VOICE_EXTENSIONS = {".ogg", ".oga", ".mp3"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm", ".jpg", ".jpeg", ".png"}
STICKER_EXTENSIONS = {".webp", ".tgs"}
for _dir in (LIBRARY_DIR, PASTES_DIR, VOICES_DIR, VIDEO_DIR, STICKERS_DIR, PROXIES_DIR):
    os.makedirs(_dir, exist_ok=True)
ARCHIVE_DIR = "Archive"
os.makedirs(ARCHIVE_DIR, exist_ok=True)
ASSET_TITLE_MAX = 32
ITEMS_PER_PAGE = 10
ACCOUNTS_META = "accounts.json"
ROTATION_STATE = ".rotation_state.json"
TENANTS_DB = "tenants.json"
MAX_MEDIA_FORWARD_SIZE = 20 * 1024 * 1024  # 20 MB

REACTION_CHOICES: List[Tuple[str, str]] = [
    ("😂 Смех", "😂"),
    ("🔥 Огонь", "🔥"),
    ("❤️ Сердечко", "❤️"),
    ("😮 Удивление", "😮"),
]
REACTION_EMOJI_SET: Set[str] = {emoji for _, emoji in REACTION_CHOICES}


def _ensure_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _save(d, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)


def _load(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


tenants: Dict[str, Dict[str, Any]] = _ensure_dict(_load(TENANTS_DB, {}))
_tenants_initially_empty = not tenants


def persist_tenants() -> None:
    _save(tenants, TENANTS_DB)


def _normalize_peer_id(user_id: Any) -> int:
    """Extract the integer ID from Telethon peer objects or raw identifiers."""

    if isinstance(user_id, bool):  # guard against bool being a subclass of int
        raise TypeError("Boolean is not a valid Telegram identifier")

    if isinstance(user_id, int):
        return user_id

    if isinstance(user_id, str):
        cleaned = user_id.strip()
        if not cleaned:
            raise ValueError("Empty string cannot represent a Telegram identifier")
        return int(cleaned)

    for attr in ("user_id", "channel_id", "chat_id"):
        value = getattr(user_id, attr, None)
        if value is not None:
            return int(value)

    raise TypeError(f"Unsupported Telegram identifier type: {type(user_id)!r}")


def tenant_key(user_id: Any) -> str:
    return str(_normalize_peer_id(user_id))


def ensure_user_dirs(user_id: int) -> None:
    base = os.path.join(LIBRARY_DIR, str(user_id))
    os.makedirs(base, exist_ok=True)
    for sub in ("pastes", "voices", "video", "stickers"):
        os.makedirs(os.path.join(base, sub), exist_ok=True)
    os.makedirs(os.path.join(PROXIES_DIR, str(user_id)), exist_ok=True)
    os.makedirs(os.path.join(SESSIONS_DIR, str(user_id)), exist_ok=True)


def user_library_dir(user_id: int, kind: str) -> str:
    ensure_user_dirs(user_id)
    return os.path.join(LIBRARY_DIR, str(user_id), kind)


def user_sessions_dir(user_id: int) -> str:
    ensure_user_dirs(user_id)
    return os.path.join(SESSIONS_DIR, str(user_id))


def user_session_path(user_id: int, phone: str) -> str:
    return os.path.join(user_sessions_dir(user_id), f"{phone}.session")


def user_proxy_dir(user_id: int) -> str:
    ensure_user_dirs(user_id)
    return os.path.join(PROXIES_DIR, str(user_id))


def _get_media_metadata_path(file_path: str) -> str:
    """Возвращает путь к файлу метаданных для медиа файла."""
    return f"{file_path}.meta.json"


def _save_media_metadata(file_path: str, media_type: str) -> None:
    """Сохраняет метаданные о типе медиа файла."""
    meta_path = _get_media_metadata_path(file_path)
    try:
        metadata = {"media_type": media_type}
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False)
    except Exception as e:
        logging.getLogger("mgrbot").warning(f"Не удалось сохранить метаданные для {file_path}: {e}")


def _load_media_metadata(file_path: str) -> Optional[str]:
    """Загружает метаданные о типе медиа файла. Возвращает тип или None."""
    meta_path = _get_media_metadata_path(file_path)
    try:
        if os.path.exists(meta_path):
            with open(meta_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
                return metadata.get("media_type")
    except Exception as e:
        logging.getLogger("mgrbot").warning(f"Не удалось загрузить метаданные для {file_path}: {e}")
    return None


def store_user_proxy_config(user_id: int, config: Dict[str, Any]) -> str:
    directory = user_proxy_dir(user_id)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    host = str(config.get("host", "proxy"))
    safe_host = re.sub(r"[^A-Za-z0-9_.-]", "_", host) or "proxy"
    port = config.get("port", "")
    filename = f"{timestamp}_{safe_host}_{port}.json" if port else f"{timestamp}_{safe_host}.json"
    path = os.path.join(directory, filename)
    to_dump = dict(config)
    to_dump["saved_at"] = datetime.now().isoformat()
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(to_dump, fh, ensure_ascii=False, indent=2)
    return path


def ensure_tenant(user_id: int, *, role: str = "user") -> Dict[str, Any]:
    key = tenant_key(user_id)
    data = tenants.setdefault(key, {})
    if data.get("role") not in {"root", "user"}:
        data["role"] = role
    elif role == "root" and data.get("role") != "root":
        data["role"] = "root"
    data.setdefault("accounts", {})
    data.setdefault("rotation_state", {})
    data.setdefault("proxy", {})
    ensure_user_dirs(user_id)
    persist_tenants()
    return data


def get_tenant(user_id: int) -> Dict[str, Any]:
    key = tenant_key(user_id)
    if key not in tenants:
        return ensure_tenant(user_id)
    ensure_user_dirs(user_id)
    data = tenants[key]
    data.setdefault("accounts", {})
    data.setdefault("rotation_state", {})
    if not isinstance(data.get("proxy"), dict):
        data["proxy"] = {}
        persist_tenants()
    else:
        data.setdefault("proxy", {})
    return data


def get_accounts_meta(owner_id: int) -> Dict[str, Dict[str, Any]]:
    return get_tenant(owner_id).setdefault("accounts", {})


def get_rotation_state(owner_id: int) -> Dict[str, int]:
    tenant = get_tenant(owner_id)
    rotation = tenant.setdefault("rotation_state", {})
    if not isinstance(rotation, dict):
        rotation = {}
        tenant["rotation_state"] = rotation
        persist_tenants()
    return rotation


def get_account_meta(owner_id: int, phone: str) -> Optional[Dict[str, Any]]:
    accounts = get_accounts_meta(owner_id)
    return accounts.get(phone)


def ensure_account_meta(owner_id: int, phone: str) -> Dict[str, Any]:
    accounts = get_accounts_meta(owner_id)
    meta = accounts.setdefault(phone, {"phone": phone})
    return meta


def get_tenant_proxy_config(owner_id: int) -> Dict[str, Any]:
    tenant = get_tenant(owner_id)
    proxy_cfg = tenant.get("proxy")
    if not isinstance(proxy_cfg, dict):
        proxy_cfg = {}
        tenant["proxy"] = proxy_cfg
        persist_tenants()
    return proxy_cfg


def set_tenant_proxy_config(owner_id: int, config: Dict[str, Any]) -> None:
    tenant = get_tenant(owner_id)
    tenant["proxy"] = dict(config)
    persist_tenants()


def clear_tenant_proxy_config(owner_id: int) -> None:
    tenant = get_tenant(owner_id)
    tenant["proxy"] = {}
    persist_tenants()


def get_active_tenant_proxy(owner_id: int) -> Optional[Dict[str, Any]]:
    cfg = get_tenant_proxy_config(owner_id)
    if not cfg:
        return None
    if not bool(cfg.get("enabled", True)):
        return None
    if not cfg.get("host") or cfg.get("port") is None:
        return None
    return cfg


def owner_has_account_proxy_overrides(owner_id: int) -> bool:
    """Return True if the user has at least one account with a custom proxy."""

    accounts = get_accounts_meta(owner_id)
    for meta in accounts.values():
        override = meta.get("proxy_override")
        if isinstance(override, dict) and override.get("enabled", True):
            return True
    return False


def clear_account_proxy_overrides(
    owner_id: int, *, include_disabled: bool = False
) -> Tuple[int, List[str]]:
    """Remove stored per-account proxy overrides.

    Parameters
    ----------
    owner_id: int
        Tenant identifier whose accounts should be processed.
    include_disabled: bool
        When True the function also removes overrides that explicitly
        disabled proxies (``{"enabled": False}``).  By default those
        overrides are preserved so admins do not accidentally switch
        such accounts back to using the tenant/global proxy.

    Returns
    -------
    Tuple[int, List[str]]
        ``(removed_count, phones)`` where ``phones`` is the list of
        affected phone numbers.
    """

    accounts = get_accounts_meta(owner_id)
    removed = 0
    phones: List[str] = []

    for phone, meta in accounts.items():
        override = meta.get("proxy_override")
        if not isinstance(override, dict):
            continue
        if not include_disabled and not override.get("enabled", True):
            continue
        meta.pop("proxy_override", None)
        removed += 1
        phones.append(phone)

    if removed:
        persist_tenants()

    return removed, phones


async def clear_owner_runtime(owner_id: int) -> None:
    owner_workers = WORKERS.pop(owner_id, {})
    for worker in owner_workers.values():
        with contextlib.suppress(Exception):
            await worker.logout()
    pending.pop(owner_id, None)
    reply_waiting.pop(owner_id, None)
    menu_button_reset.discard(owner_id)
    for ctx_id, ctx in list(reply_contexts.items()):
        if ctx.get("owner_id") == owner_id:
            reply_contexts.pop(ctx_id, None)
            for admin_key, waiting_ctx in list(reply_waiting.items()):
                if waiting_ctx.get("ctx") == ctx_id:
                    reply_waiting.pop(admin_key, None)


def remove_tenant(owner_id: int) -> bool:
    key = tenant_key(owner_id)
    data = tenants.get(key)
    if not data:
        return False
    if data.get("role") == "root" and owner_id in ROOT_ADMIN_IDS:
        return False
    tenants.pop(key, None)
    persist_tenants()
    return True


def archive_user_data(user_id: int) -> None:
    """Перемещает пользовательские каталоги в архив."""
    dest_base = os.path.join(ARCHIVE_DIR, str(user_id))
    os.makedirs(dest_base, exist_ok=True)
    sources = [
        os.path.join(LIBRARY_DIR, str(user_id)),
        os.path.join(SESSIONS_DIR, str(user_id)),
    ]
    for src in sources:
        if not os.path.exists(src):
            continue
        dst = os.path.join(dest_base, os.path.basename(src))
        if os.path.exists(dst):
            shutil.rmtree(dst, ignore_errors=True)
        try:
            shutil.move(src, dst)
        except Exception as exc:  # pragma: no cover - best effort archival
            log.warning("Не удалось переместить %s в архив: %s", src, exc)
    # Удаляем пустую папку пользователя в library/sessions, если она осталась
    for parent in (LIBRARY_DIR, SESSIONS_DIR):
        path = os.path.join(parent, str(user_id))
        if os.path.isdir(path) and not os.listdir(path):
            with contextlib.suppress(OSError):
                os.rmdir(path)


def list_regular_tenants() -> List[Tuple[int, Dict[str, Any]]]:
    entries: List[Tuple[int, Dict[str, Any]]] = []
    for key, data in tenants.items():
        try:
            user_id = int(key)
        except (TypeError, ValueError):
            continue
        info = data if isinstance(data, dict) else {}
        role = info.get("role") or "user"
        if role == "root":
            continue
        entries.append((user_id, info))
    entries.sort(key=lambda item: item[0])
    return entries


def build_user_access_view() -> Tuple[str, Optional[List[List[Button]]]]:
    tenants_list = list_regular_tenants()
    if not tenants_list:
        return (
            "Нет пользователей с доступом (кроме супер-администраторов).",
            [[Button.inline("⬅️ Закрыть", b"userlist_close")]],
        )
    lines = ["Пользователи с доступом (кроме супер-админов):"]
    buttons: List[List[Button]] = []
    for user_id, info in tenants_list:
        accounts = info.get("accounts")
        count = len(accounts) if isinstance(accounts, dict) else 0
        lines.append(f"• {user_id} — {count} аккаунтов")
        buttons.append(
            [
                Button.inline(str(user_id), f"usernoop:{user_id}".encode()),
                Button.inline("🚫 Блокировать", f"userblock:{user_id}".encode()),
            ]
        )
    buttons.append([Button.inline("⬅️ Закрыть", b"userlist_close")])
    return "\n".join(lines), buttons


async def send_user_access_list(admin_id: int, *, event=None) -> None:
    text, buttons = build_user_access_view()
    markup = buttons if buttons else None
    if event is not None:
        try:
            await event.edit(text, buttons=markup)
            return
        except Exception as exc:
            log.warning("Не удалось обновить сообщение списка пользователей: %s", exc)
    await bot_client.send_message(admin_id, text, buttons=markup)


def all_admin_ids() -> Set[int]:
    ids: Set[int] = set()
    for key in tenants.keys():
        try:
            ids.add(int(key))
        except (TypeError, ValueError):
            continue
    return ids


def is_root_admin(user_id: Any) -> bool:
    try:
        key = tenant_key(user_id)
    except (TypeError, ValueError) as exc:
        log.warning("Cannot normalise ID %r for root admin check: %s", user_id, exc)
        return False
    data = tenants.get(key)
    if not data:
        return False
    return data.get("role") == "root"


for root_id in ROOT_ADMIN_IDS:
    ensure_tenant(root_id, role="root")


# переносим данные из старых файлов, если это первый запуск новой схемы
if _tenants_initially_empty:
    legacy_accounts = _ensure_dict(_load(ACCOUNTS_META, {}))
    legacy_rotation = _ensure_dict(_load(ROTATION_STATE, {}))
    if legacy_accounts and ROOT_ADMIN_IDS:
        fallback_owner = next(iter(ROOT_ADMIN_IDS))
        tenant = ensure_tenant(fallback_owner, role="root")
        tenant["accounts"] = legacy_accounts
        tenant["rotation_state"] = legacy_rotation
        persist_tenants()

# Параметры имитации активности перед отправкой
# Реалистичная скорость печати для зумеров: 50-80 WPM = 4-7 символов/сек
TYPING_CHAR_SPEED = (4.0, 7.0)  # символов в секунду (реалистично для зумеров)
TYPING_WORD_DELAY = (0.15, 0.25)  # сек. на слово (реалистично)
TYPING_BASE_DELAY = (0.3, 0.7)  # небольшое постоянное смещение
TYPING_NEWLINE_DELAY = (0.2, 0.5)  # штраф за перевод строки
TYPING_DURATION_LIMITS = (0.5, 60.0)  # минимальная и максимальная продолжительность «печати»
TYPING_DURATION_VARIANCE = (0.85, 1.15)  # небольшая вариативность
VOICE_RECORD_LIMITS = (2.0, 45.0)
VOICE_RECORD_BYTES_PER_SECOND = (15000.0, 26000.0)
VOICE_RECORD_FALLBACK = (5.0, 10.0)
VOICE_RECORD_EXTRA_DELAY = (0.6, 1.4)
VOICE_RECORD_VARIANCE = (0.9, 1.05)
VIDEO_NOTE_RECORD_LIMITS = (3.0, 55.0)
VIDEO_NOTE_BYTES_PER_SECOND = (60000.0, 110000.0)
VIDEO_NOTE_FALLBACK = (9.0, 18.0)
VIDEO_NOTE_EXTRA_DELAY = (1.4, 2.8)
VIDEO_NOTE_VARIANCE = (0.92, 1.18)
# Средняя скорость загрузки фото: 1-3 МБ/сек для хорошего интернета
PHOTO_UPLOAD_LIMITS = (0.3, 10.0)
PHOTO_UPLOAD_BYTES_PER_SECOND = (800000.0, 3000000.0)  # 0.8-3 МБ/сек
PHOTO_UPLOAD_FALLBACK = (0.5, 2.0)
PHOTO_UPLOAD_EXTRA_DELAY = (0.1, 0.4)
PHOTO_UPLOAD_VARIANCE = (0.9, 1.1)
# Средняя скорость загрузки видео: 0.5-2 МБ/сек
VIDEO_UPLOAD_LIMITS = (0.5, 30.0)
VIDEO_UPLOAD_BYTES_PER_SECOND = (500000.0, 2000000.0)  # 0.5-2 МБ/сек
VIDEO_UPLOAD_FALLBACK = (1.0, 4.0)
VIDEO_UPLOAD_EXTRA_DELAY = (0.2, 0.6)
VIDEO_UPLOAD_VARIANCE = (0.9, 1.1)
CHAT_ACTION_REFRESH = 4.5  # секунды между повторными действиями, если требуется
# ============================================

def _rand_delay(span: Tuple[int, int]) -> float:
    low, high = span
    if low >= high:
        return float(low)
    return random.uniform(low, high)


def _describe_sent_code_type(code_type: Optional[Any]) -> Optional[str]:
    if code_type is None:
        return None

    auth_types = getattr(types, "auth", None)
    mapping: List[Tuple[Optional[type], str]] = []
    if auth_types is not None:
        mapping = [
            (getattr(auth_types, "SentCodeTypeApp", None), "app"),
            (getattr(auth_types, "SentCodeTypeSms", None), "sms"),
            (getattr(auth_types, "SentCodeTypeCall", None), "call"),
            (getattr(auth_types, "SentCodeTypeFlashCall", None), "flash_call"),
            (getattr(auth_types, "SentCodeTypeMissedCall", None), "missed_call"),
            (getattr(auth_types, "SentCodeTypeEmailCode", None), "email"),
            (getattr(auth_types, "SentCodeTypeFirebaseSms", None), "sms"),
            (getattr(auth_types, "SentCodeTypeSmsWord", None), "sms"),
            (getattr(auth_types, "SentCodeTypeFragmentSms", None), "sms"),
        ]
    for cls, label in mapping:
        if cls is not None and isinstance(code_type, cls):
            return label
    # Fallback to class name string for logging/diagnostics
    name = getattr(getattr(code_type, "__class__", type(code_type)), "__name__", None)
    if isinstance(name, str):
        return name.lower()
    return "unknown"

def _typing_duration(message: str) -> float:
    message = message or ""
    stripped = message.strip()
    variance = random.uniform(*TYPING_DURATION_VARIANCE)
    low_limit, high_limit = TYPING_DURATION_LIMITS

    if not stripped:
        base = random.uniform(*TYPING_BASE_DELAY)
        return max(low_limit, min(base * variance, high_limit))

    char_count = len(message)
    word_count = len(stripped.split())
    line_breaks = message.count("\n")

    speed = random.uniform(*TYPING_CHAR_SPEED)
    char_component = char_count / max(speed, 1e-3)

    word_delay = random.uniform(*TYPING_WORD_DELAY)
    word_component = word_count * word_delay if word_count else 0.0

    base_delay = random.uniform(*TYPING_BASE_DELAY)
    newline_penalty = line_breaks * random.uniform(*TYPING_NEWLINE_DELAY) if line_breaks else 0.0

    duration = max(char_component, word_component) + base_delay + newline_penalty
    duration *= variance

    return max(low_limit, min(duration, high_limit))


def _media_recording_duration(
    file_path: Optional[str],
    bytes_per_second_range: Tuple[float, float],
    fallback_range: Tuple[float, float],
    limits: Tuple[float, float],
    extra_delay_range: Tuple[float, float],
    variance_range: Tuple[float, float],
) -> float:
    low_limit, high_limit = limits
    variance = random.uniform(*variance_range)
    extra = random.uniform(*extra_delay_range)

    size = 0
    if file_path:
        with contextlib.suppress(OSError):
            size = os.path.getsize(file_path)

    bps_low, bps_high = bytes_per_second_range
    duration: float
    if size > 0 and bps_low > 0 and bps_high > 0:
        bps_high = max(bps_high, bps_low)
        bps = random.uniform(bps_low, bps_high)
        duration = size / max(bps, 1.0)
    else:
        duration = random.uniform(*fallback_range)

    duration = (duration + extra) * variance
    return max(low_limit, min(duration, high_limit))


def _voice_record_duration(file_path: Optional[str]) -> float:
    return _media_recording_duration(
        file_path,
        VOICE_RECORD_BYTES_PER_SECOND,
        VOICE_RECORD_FALLBACK,
        VOICE_RECORD_LIMITS,
        VOICE_RECORD_EXTRA_DELAY,
        VOICE_RECORD_VARIANCE,
    )


def _get_video_duration(file_path: Optional[str]) -> Optional[float]:
    """Получает реальную длительность видео из файла. Возвращает None если не удалось."""
    if not file_path or not os.path.exists(file_path):
        return None
    
    # Пробуем использовать moviepy (если установлен)
    try:
        from moviepy.editor import VideoFileClip
        with VideoFileClip(file_path) as clip:
            return clip.duration
    except ImportError:
        pass
    except Exception:
        pass
    
    # Пробуем использовать ffprobe через subprocess (если доступен)
    try:
        import subprocess
        result = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path
            ],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            duration = float(result.stdout.strip())
            if duration > 0:
                return duration
    except (subprocess.TimeoutExpired, ValueError, FileNotFoundError, Exception):
        pass
    
    return None


def _video_note_record_duration(file_path: Optional[str]) -> float:
    """Получает длительность анимации записи кружка. 
    Если возможно, использует реальную длительность видео, иначе рассчитывает по размеру файла."""
    # Пробуем получить реальную длительность видео
    real_duration = _get_video_duration(file_path)
    if real_duration is not None and real_duration > 0:
        # Добавляем небольшую вариативность для реалистичности
        variance = random.uniform(0.95, 1.05)
        return real_duration * variance
    
    # Fallback: рассчитываем по размеру файла
    return _media_recording_duration(
        file_path,
        VIDEO_NOTE_BYTES_PER_SECOND,
        VIDEO_NOTE_FALLBACK,
        VIDEO_NOTE_RECORD_LIMITS,
        VIDEO_NOTE_EXTRA_DELAY,
        VIDEO_NOTE_VARIANCE,
    )


def _photo_upload_duration(file_path: Optional[str]) -> float:
    return _media_recording_duration(
        file_path,
        PHOTO_UPLOAD_BYTES_PER_SECOND,
        PHOTO_UPLOAD_FALLBACK,
        PHOTO_UPLOAD_LIMITS,
        PHOTO_UPLOAD_EXTRA_DELAY,
        PHOTO_UPLOAD_VARIANCE,
    )


def _video_upload_duration(file_path: Optional[str]) -> float:
    """Рассчитывает время загрузки видео на основе размера файла.
    Если возможно, использует реальную длительность видео для более точного расчета."""
    # Пробуем получить реальную длительность видео
    real_duration = _get_video_duration(file_path)
    if real_duration is not None and real_duration > 0:
        # Для видео загрузка обычно занимает примерно 0.3-0.8 от длительности видео
        # (зависит от размера файла и скорости интернета)
        file_size = 0
        if file_path:
            with contextlib.suppress(OSError):
                file_size = os.path.getsize(file_path)
        
        if file_size > 0:
            # Рассчитываем примерную скорость загрузки на основе размера и длительности
            # Средняя скорость: 0.5-2 МБ/сек
            upload_speed = random.uniform(500000.0, 2000000.0)  # байт/сек
            upload_time = file_size / upload_speed
            # Добавляем небольшую вариативность
            variance = random.uniform(0.9, 1.1)
            return max(VIDEO_UPLOAD_LIMITS[0], min(upload_time * variance, VIDEO_UPLOAD_LIMITS[1]))
    
    # Fallback: рассчитываем только по размеру файла
    return _media_recording_duration(
        file_path,
        VIDEO_UPLOAD_BYTES_PER_SECOND,
        VIDEO_UPLOAD_FALLBACK,
        VIDEO_UPLOAD_LIMITS,
        VIDEO_UPLOAD_EXTRA_DELAY,
        VIDEO_UPLOAD_VARIANCE,
    )


def _list_files(directory: str, allowed_ext: Set[str]) -> List[str]:
    if not os.path.isdir(directory):
        return []

    entries: List[Tuple[float, str, str]] = []
    for name in os.listdir(directory):
        full = os.path.join(directory, name)
        if not os.path.isfile(full):
            continue
        ext = os.path.splitext(name)[1].lower()
        if allowed_ext and ext not in allowed_ext:
            continue
        try:
            mtime = os.path.getmtime(full)
        except OSError:
            mtime = 0.0
        entries.append((mtime, name, full))

    entries.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [full for _, _, full in entries]


def _file_sort_key(path: str) -> Tuple[float, str]:
    try:
        return os.path.getmtime(path), os.path.basename(path)
    except OSError:
        return 0.0, os.path.basename(path)


PAYLOAD_CACHE_LIMIT = 512
_payload_cache: "OrderedDict[str, str]" = OrderedDict()


def _register_payload(value: str) -> str:
    """Register *value* in the inline payload cache and return its token."""

    for _ in range(8):
        token = secrets.token_urlsafe(8)
        if token not in _payload_cache:
            break
    else:  # pragma: no cover - extremely unlikely
        token = secrets.token_urlsafe(12)
    _payload_cache[token] = value
    _payload_cache.move_to_end(token)
    while len(_payload_cache) > PAYLOAD_CACHE_LIMIT:
        _payload_cache.popitem(last=False)
    return token


def _resolve_payload(token: str) -> Optional[str]:
    value = _payload_cache.get(token)
    if value is not None:
        _payload_cache.move_to_end(token)
    return value


def _encode_payload(text: str) -> str:
    raw = base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii")
    return raw.rstrip("=")


def _decode_payload(data: str) -> str:
    padding = "=" * (-len(data) % 4)
    raw = base64.urlsafe_b64decode((data + padding).encode("ascii"))
    return raw.decode("utf-8")


def paginate_list(items: List[Any], page: int, per_page: int = ITEMS_PER_PAGE) -> Tuple[List[Any], int, int, int]:
    total = len(items)
    if total == 0:
        return [], 0, 0, 0
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    end = start + per_page
    return items[start:end], page, total_pages, total


def format_page_caption(base: str, page: int, total_pages: int) -> str:
    if total_pages > 1:
        return f"{base} (страница {page + 1}/{total_pages}):"
    return f"{base}:"


def sanitize_filename(name: str, default: str = "file") -> str:
    """Convert arbitrary text to a safe filename."""
    cleaned = re.sub(r"[^\w\s.-]", "", name, flags=re.UNICODE).strip()
    cleaned = re.sub(r"\s+", "_", cleaned)
    cleaned = cleaned[:64]
    return cleaned or default


def _list_user_and_shared_files(owner_id: int, kind: str, allowed_ext: Set[str]) -> List[str]:
    """Return files from the personal library with a fallback to the shared one.

    Earlier versions of the bot stored assets directly in ``library/<kind>``.
    Newer builds keep user-specific copies in ``library/<user>/<kind>`` to avoid
    name collisions.  Some users still place their files in the legacy shared
    folders manually, so we merge both locations to ensure compatibility.  The
    combined result is deduplicated and sorted by the modification time so that
    the most recently added items appear first.
    """

    personal_dir = user_library_dir(owner_id, kind)
    shared_dir = os.path.join(LIBRARY_DIR, kind)

    files: List[str] = []
    seen: Set[str] = set()

    def add_files(paths: List[str]) -> None:
        for path in paths:
            if path not in seen:
                seen.add(path)
                files.append(path)

    add_files(_list_files(personal_dir, allowed_ext))

    # Avoid duplicates if personal_dir and shared_dir accidentally point to the
    # same location or contain identical files.  ``os.path.normpath`` is used to
    # compare directories on platforms with different path separators.
    if os.path.normpath(personal_dir) != os.path.normpath(shared_dir):
        add_files(_list_files(shared_dir, allowed_ext))

    files.sort(key=_file_sort_key, reverse=True)
    return files


def list_text_templates(owner_id: int) -> List[str]:
    return _list_user_and_shared_files(owner_id, "pastes", TEXT_EXTENSIONS)


def list_voice_templates(owner_id: int) -> List[str]:
    return _list_user_and_shared_files(owner_id, "voices", VOICE_EXTENSIONS)


def list_video_templates(owner_id: int) -> List[str]:
    return _list_user_and_shared_files(owner_id, "video", VIDEO_EXTENSIONS)


def list_sticker_templates(owner_id: int) -> List[str]:
    return _list_user_and_shared_files(owner_id, "stickers", STICKER_EXTENSIONS)


FILE_TYPE_LABELS = {
    "paste": "Пасты",
    "voice": "Голосовые",
    "video": "Медиа",
    "sticker": "Стикеры",
}

FILE_TYPE_ADD_CALLBACK = {
    "paste": "files_paste",
    "voice": "files_voice",
    "video": "files_video",
    "sticker": "files_sticker",
}

FILE_TYPE_ADD_PROMPTS = {
    "paste": "Введите название пасты:",
    "voice": "Введите название голосового:",
    "video": "Введите название кружка:",
    "sticker": "Введите название стикера:",
}


REPLY_TEMPLATE_META: Dict[str, Dict[str, Any]] = {
    "paste": {
        "emoji": "📄",
        "label": FILE_TYPE_LABELS["paste"],
        "title": "📄 Выбери пасту для отправки:",
        "empty": "Папка с пастами пуста",
        "prefix": "paste_send",
        "loader": list_text_templates,
    },
    "voice": {
        "emoji": "🎙",
        "label": FILE_TYPE_LABELS["voice"],
        "title": "🎙 Выбери голосовое сообщение:",
        "empty": "Папка с голосовыми пуста",
        "prefix": "voice_send",
        "loader": list_voice_templates,
    },
    "video": {
        "emoji": "📹",
        "label": FILE_TYPE_LABELS["video"],
        "title": "📹 Выбери медиа для отправки:",
        "empty": "Папка с кружками пуста",
        "prefix": "video_send",
        "loader": list_video_templates,
    },
    "sticker": {
        "emoji": "💟",
        "label": FILE_TYPE_LABELS["sticker"],
        "title": "💟 Выбери стикер для отправки:",
        "empty": "Папка со стикерами пуста",
        "prefix": "sticker_send",
        "loader": list_sticker_templates,
    },
}



LIBRARY_INLINE_QUERY_PREFIXES = {"library", "lib", "files"}
LIBRARY_INLINE_RESULT_LIMIT = 50


INLINE_REPLY_SENTINEL = "\u2063INLINE_REPLY:"
INLINE_REPLY_RESULT_PREFIX = "reply:"
INLINE_REPLY_TOKEN_TRACK = 512
_inline_reply_token_queue: "deque[str]" = deque()
_inline_reply_token_seen: Set[str] = set()





@dataclass
class InlineArticle:
    """Minimal representation of an inline article result."""

    id: str
    title: str
    description: str
    text: str
    buttons: Optional[List[List[Button]]] = None


def library_inline_button(file_type: str, label: str) -> Button:
    """Create an inline switch button for library previews."""

    query = " ".join(("library", file_type)).strip()
    # ``Button.switch_inline_current`` was removed in recent Telethon releases.
    # ``Button.switch_inline`` with ``same_peer=True`` replicates the previous
    # behaviour by opening the inline query in the current chat instead of
    # redirecting the user to a different dialog.
    return Button.switch_inline(label, query=query, same_peer=True)


def _build_add_account_inline_results() -> List[InlineArticle]:
    return [
        InlineArticle(
            id="add_account_with_proxy",
            title="Ввести прокси",
            description="Сначала указать прокси, потом номер",
            text="START_ADD_WITH_PROXY",
        ),
        InlineArticle(
            id="add_account_without_proxy",
            title="Без прокси",
            description="Сразу ввести номер телефона",
            text="START_ADD_WITHOUT_PROXY",
        ),
    ]


def _reply_inline_help_article(mode: str, reason: str) -> InlineArticle:
    mode_label = "ответа" if mode == "normal" else "реплая"
    return InlineArticle(
        id=f"reply_help_{mode}",
        title=f"Нет контекста для {mode_label}",
        description=reason,
        text=f"ℹ️ {reason}",
    )


def _build_reply_inline_results(
    admin_id: int, ctx_id: str, mode: str
) -> List[InlineArticle]:
    ctx_info = get_reply_context_for_admin(ctx_id, admin_id)
    if not ctx_info:
        return [
            _reply_inline_help_article(
                mode,
                "Контекст устарел. Закрой уведомление и дождись нового.",
            )
        ]
    description = f"Аккаунт {ctx_info['phone']} • чат {ctx_info['chat_id']}"
    base_payload = {"ctx": ctx_id, "mode": mode}
    articles: List[InlineArticle] = []

    token = _register_payload(json.dumps({**base_payload, "variant": "text"}, ensure_ascii=False))
    articles.append(
        InlineArticle(
            id=f"{INLINE_REPLY_RESULT_PREFIX}{token}",
            title="✍️ Написать сообщение",
            description=description,
            text=f"{INLINE_REPLY_SENTINEL}{token}",
        )
    )

    for file_type, meta in REPLY_TEMPLATE_META.items():
        emoji = meta.get("emoji", "")
        label = meta.get("label", file_type)
        inline_label = f"{emoji} {label}".strip()
        picker_payload = {
            **base_payload,
            "variant": "picker",
            "file_type": file_type,
        }
        picker_token = _register_payload(json.dumps(picker_payload, ensure_ascii=False))
        articles.append(
            InlineArticle(
                id=f"{INLINE_REPLY_RESULT_PREFIX}{picker_token}",
                title=inline_label,
                description=description,
                text=f"{INLINE_REPLY_SENTINEL}{picker_token}",
            )
        )

    return articles


def _prune_inline_reply_tokens() -> None:
    while len(_inline_reply_token_queue) > INLINE_REPLY_TOKEN_TRACK:
        oldest = _inline_reply_token_queue.popleft()
        _inline_reply_token_seen.discard(oldest)


def _claim_inline_reply_token(token: str) -> bool:
    if not token:
        return False
    if token in _inline_reply_token_seen:
        return False
    _inline_reply_token_seen.add(token)
    _inline_reply_token_queue.append(token)
    _prune_inline_reply_tokens()
    return True


def _resolve_inline_reply_payload(token: str) -> Optional[Dict[str, Any]]:
    raw = _resolve_payload(token)
    if not raw:
        return None
    with contextlib.suppress(json.JSONDecodeError):
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    return None


async def _execute_inline_reply_payload(admin_id: int, payload: Dict[str, Any]) -> None:
    ctx = payload.get("ctx")
    if not ctx:
        await send_temporary_message(admin_id, "❌ Контекст недоступен.")
        return
    mode = payload.get("mode", "normal")
    mode_value = "reply" if mode == "reply" else "normal"
    error = await _activate_reply_session(admin_id, ctx, mode_value)
    if error:
        await send_temporary_message(admin_id, f"❌ {error}")
        return
    if payload.get("variant") == "picker":
        file_type = payload.get("file_type")
        if not file_type:
            return
        picker_error = await _open_reply_asset_menu(admin_id, ctx, mode_value, file_type)
        if picker_error:
            await send_temporary_message(admin_id, f"❌ {picker_error}")


async def _process_inline_reply_token(admin_id: int, token: str) -> bool:
    payload = _resolve_inline_reply_payload(token)
    if not payload:
        return False
    if not _claim_inline_reply_token(token):
        return True
    await _execute_inline_reply_payload(admin_id, payload)
    return True


def _parse_reply_inline_query(query: str) -> Optional[Tuple[str, str]]:
    parts = query.split(None, 1)
    if not parts:
        return None
    token = parts[0].lower()
    if token not in {"reply", "reply_to"}:
        return None
    ctx = parts[1] if len(parts) > 1 else ""
    mode = "reply" if token == "reply_to" else "normal"
    return ctx, mode


def _prepare_reply_asset_menu(owner_id: int, file_type: str) -> Optional[Tuple[List[str], str, str, str]]:
    meta = REPLY_TEMPLATE_META.get(file_type)
    if not meta:
        return None
    loader = cast(Callable[[int], List[str]], meta["loader"])
    files = loader(owner_id)
    return files, meta["empty"], meta["title"], meta["prefix"]


async def _open_reply_asset_menu(
    admin_id: int, ctx: str, mode: Optional[str], file_type: str, page: int = 0
) -> Optional[str]:
    ctx_info = get_reply_context_for_admin(ctx, admin_id)
    if not ctx_info:
        return "Контекст истёк"
    await mark_dialog_read_for_context(ctx_info)
    menu = _prepare_reply_asset_menu(ctx_info["owner_id"], file_type)
    if not menu:
        return "Неизвестный тип файла"
    files, empty_text, title, prefix = menu
    if not files:
        return empty_text
    buttons = build_asset_keyboard(files, file_type, prefix, ctx, mode, page=page)
    await show_interactive_message(
        admin_id,
        title,
        buttons=buttons,
        replace=True,
    )
    return None


def library_manage_buttons(file_type: str) -> Optional[List[List[Button]]]:
    """Inline keyboard for managing templates of the given type."""

    if file_type not in FILE_TYPE_LABELS:
        return None

    rows: List[List[Button]] = []
    add_payload = FILE_TYPE_ADD_CALLBACK.get(file_type)
    if add_payload:
        rows.append([Button.inline("➕ Добавить", add_payload.encode())])
    rows.append([Button.inline("🗑 Удалить", f"files_delete_{file_type}".encode())])
    return rows


async def _render_inline_articles(
    builder: "InlineBuilder", articles: List[InlineArticle]
) -> List[Any]:
    results = []
    for article in articles:
        kwargs = {
            "title": article.title,
            "text": article.text,
            "id": article.id,
            "link_preview": False,
        }
        if article.description:
            kwargs["description"] = article.description
        if article.buttons:
            kwargs["buttons"] = article.buttons
        results.append(await builder.article(**kwargs))
    return results


def _inline_file_metadata(path: str) -> Tuple[str, str]:
    try:
        stat = os.stat(path)
    except (FileNotFoundError, OSError):
        return "", ""

    size_label = _format_filesize(int(getattr(stat, "st_size", 0)))
    try:
        modified = datetime.fromtimestamp(getattr(stat, "st_mtime", 0))
        modified_label = modified.strftime("%d.%m.%Y %H:%M")
    except Exception:
        modified_label = ""
    return size_label, modified_label


def _library_command_instructions() -> str:
    if BOT_USERNAME:
        return (
            "ℹ️ Для расширенного поиска нажмите кнопку «Файлы ↗» или наберите "
            f"@{BOT_USERNAME} library <категория> в строке сообщения."
        )
    return "ℹ️ Для расширенного поиска воспользуйтесь кнопкой «Файлы ↗»."


def _build_library_overview_text(owner_id: int) -> str:
    files_by_type = {ft: list_templates_by_type(owner_id, ft) for ft in FILE_TYPE_LABELS}
    total = sum(len(items) for items in files_by_type.values())

    lines = ["📁 Доступные файлы:"]
    for ft, label in FILE_TYPE_LABELS.items():
        lines.append(f"• {label}: {len(files_by_type[ft])}")
    lines.append("")
    lines.append(f"Всего файлов: {total}")
    lines.append("")
    lines.append(_library_command_instructions())
    return "\n".join(lines)


def _build_library_category_text(
    owner_id: int, file_type: str, search_term: str
) -> str:
    all_files = list_templates_by_type(owner_id, file_type)
    normalized_term = " ".join(search_term.split()) if search_term else ""
    if normalized_term:
        lowered = normalized_term.lower()
        files = [path for path in all_files if lowered in os.path.basename(path).lower()]
    else:
        files = all_files

    label = FILE_TYPE_LABELS.get(file_type, file_type.title())
    lines: List[str] = []
    header = f"{label}: {len(files)}"
    if normalized_term:
        header += f" (фильтр \"{normalized_term}\")"
    lines.append(header)
    lines.append(f"Всего в категории: {len(all_files)}")
    if not files:
        lines.append("Нет файлов, подходящих под условия.")
    else:
        limit = 10
        for path in files[:limit]:
            name = os.path.basename(path)
            size_label, modified_label = _inline_file_metadata(path)
            meta_parts = [part for part in (size_label, modified_label) if part]
            meta = f" ({', '.join(meta_parts)})" if meta_parts else ""
            rel_path = os.path.relpath(path, start=LIBRARY_DIR)
            lines.append(f"• {name}{meta}")
            lines.append(f"  {rel_path}")
        if len(files) > limit:
            lines.append(f"… и ещё {len(files) - limit}")
    lines.append("")
    lines.append(
        "Чтобы отправить файл собеседнику, используйте меню «Файлы» или шаблоны ответа."
    )
    lines.append(_library_command_instructions())
    return "\n".join(lines)


def _build_library_unknown_text(query: str) -> str:
    lines = [f"Категория \"{query}\" не найдена."]
    lines.append("Доступные категории:")
    for key, label in FILE_TYPE_LABELS.items():
        lines.append(f"• {label} ({key})")
    lines.append("")
    lines.append("Попробуйте, например, запрос `library paste`.")
    lines.append(_library_command_instructions())
    return "\n".join(lines)


def _extract_library_command_query(text: str) -> Optional[str]:
    stripped = text.strip()
    if not stripped:
        return None
    tokens = stripped.split()
    if not tokens:
        return None
    first = tokens[0]
    if first.startswith("@") and len(tokens) > 1:
        tokens = tokens[1:]
        first = tokens[0]
    lowered = first.lower()
    if (
        lowered in LIBRARY_INLINE_QUERY_PREFIXES
        or lowered in FILE_TYPE_LABELS
        or lowered in {"overview", "all"}
    ):
        return " ".join(tokens)
    return None


def _render_library_command(owner_id: int, query: str) -> str:
    parts = query.split()
    if parts and parts[0].lower() in LIBRARY_INLINE_QUERY_PREFIXES:
        parts = parts[1:]

    if not parts:
        return _build_library_overview_text(owner_id)

    category = parts[0].lower()
    remainder = " ".join(parts[1:]) if len(parts) > 1 else ""

    if category in FILE_TYPE_LABELS:
        return _build_library_category_text(owner_id, category, remainder)
    if category in {"overview", "all"}:
        return _build_library_overview_text(owner_id)
    return _build_library_unknown_text(category)


def _build_library_file_results(
    owner_id: int,
    file_type: str,
    search_term: str,
    *,
    preloaded: Optional[List[str]] = None,
    mode: Optional[str] = None,
) -> List[InlineArticle]:
    """Формирует inline-результаты для файлов конкретного типа.

    summary-карточка сверху убрана — возвращаем только сами файлы.
    Если mode == "delete", выбор результата приводит к удалению файла.
    """
    all_files = list(preloaded) if preloaded is not None else list_templates_by_type(owner_id, file_type)

    normalized_term = " ".join(search_term.split()) if search_term else ""
    if normalized_term:
        lowered = normalized_term.lower()
        files = [path for path in all_files if lowered in os.path.basename(path).lower()]
    else:
        files = all_files

    total_count = len(files)
    label = FILE_TYPE_LABELS.get(file_type, file_type.title())

    adding = mode == "add"
    deleting = mode == "delete"
    results: List[InlineArticle] = []

    if adding:
        results.append(
            InlineArticle(
                id=f"{file_type}:add",
                title=f"➕ Добавить {label.lower()}",
                description="Начать добавление нового файла",
                text=f"INLINE_ADD:{file_type}",
            )
        )

    # Если файлов нет — возвращаем информационную карточку (и кнопку добавления, если есть)
    if not total_count:
        if results:
            return results
        if deleting:
            return []
        msg_lines = [
            f"{label}: файлов нет.",
            "",
            "Добавьте файлы через меню бота.",
        ]
        empty_article = InlineArticle(
            id=f"{file_type}:empty",
            title=f"{label}: нет файлов",
            description="В этой категории пока нет файлов",
            text="\n".join(msg_lines),
        )
        results.append(empty_article)
        return results

    # Обрезаем по лимиту
    limited = files[:LIBRARY_INLINE_RESULT_LIMIT]

    for idx, path in enumerate(limited):
        name = os.path.basename(path)
        size_label, modified_label = _inline_file_metadata(path)
        desc_parts = [part for part in (size_label, modified_label) if part]
        description_text = " • ".join(desc_parts) if desc_parts else "Файл из библиотеки"
        rel_path = os.path.relpath(path, start=LIBRARY_DIR)

        if deleting:
            # Режим удаления: при выборе инлайна отправляем служебную команду,
            # которую перехватит on_text и удалит файл.
            token = _register_payload(path)
            command = f"INLINE_DEL:{file_type}:{token}"
            article_text = command
            article_title = f"🗑 {label} — {name}"
        else:
            message_lines = [f"{label} — {name}"]
            if size_label:
                message_lines.append(f"Размер: {size_label}")
            if modified_label:
                message_lines.append(f"Обновлён: {modified_label}")
            message_lines.append(f"Путь: {rel_path}")
            if normalized_term:
                message_lines.append(f'Фильтр: "{normalized_term}"')
            message_lines.append("")
            message_lines.append(
                "Чтобы отправить этот файл пользователю, откройте меню файлов или используйте шаблоны ответа."
            )
            article_text = "\n".join(message_lines)
            article_title = name

        results.append(
            InlineArticle(
                id=f"{file_type}:{idx}",
                title=article_title,
                description=description_text,
                text=article_text,
            )
        )

    return results

def _inline_command_text(command: str) -> str:
    username = BOT_USERNAME
    if username:
        prefix = f"@{username} "
    else:
        prefix = ""
    return f"{prefix}{command}".strip()


def _build_files_main_menu() -> List[InlineArticle]:
    """Инлайн-экран главного меню файлов: Добавить/Удалить.
    
    Использует ТОЛЬКО Button.switch_inline для seamless переходов в текущем чате.
    """
    results = []

    # Плашка "Добавить" с кнопкой для открытия новых инлайн-плашек
    results.append(
        InlineArticle(
            id="files_add",
            title="➕ Добавить",
            description="Добавить пасту, голосовое, медиа, стикер",
            text="Открываю меню добавления...",
            buttons=[
                [Button.switch_inline(
                    text="📂 Открыть типы файлов",
                    query="files_add",
                    same_peer=True
                )]
            ],
        )
    )

    # Плашка "Удалить" с кнопкой для открытия новых инлайн-плашек
    results.append(
        InlineArticle(
            id="files_delete",
            title="🗑 Удалить",
            description="Удалить из библиотеки",
            text="🔹 Нажмите кнопку ниже для удаления файлов из библиотеки.",
            buttons=[
                [Button.switch_pm(
                    text="📂 Выбрать тип файлов",
                    start_parameter="files_del"
                )]
            ],
        )
    )

    return results


def _build_files_add_menu() -> List[InlineArticle]:
    """Меню выбора типа файла для добавления - через switch_pm плашки."""
    results = []
    
    file_types = [
        ("paste", "📄 Пасты", "Добавить текстовую пасту"),
        ("voice", "🎙 Голосовые", "Добавить голосовое сообщение"),
        ("video", "📹 Медиа", "Добавить медиа"),
        ("sticker", "💟 Стикеры", "Добавить стикер"),
    ]
    
    for file_type, title, desc in file_types:
        results.append(
            InlineArticle(
                id=f"add_{file_type}",
                title=title,
                description=desc,
                text=f"🔹 Нажмите кнопку ниже для добавления",
                buttons=[
                    [Button.switch_pm(
                        text=f"➕ Добавить {title.lower()}",
                        start_parameter=f"add_{file_type}"
                    )]
                ],
            )
        )
    
    return results


def _build_files_del_menu() -> List[InlineArticle]:
    """Меню выбора типа файла для удаления - через switch_pm плашки."""
    results = []
    
    file_types = [
        ("paste", "📄 Пасты", "Удалить текстовую пасту"),
        ("voice", "🎙 Голосовые", "Удалить голосовое сообщение"),
        ("video", "📹 Медиа", "Удалить медиа"),
        ("sticker", "💟 Стикеры", "Удалить стикер"),
    ]
    
    for file_type, title, desc in file_types:
        results.append(
            InlineArticle(
                id=f"del_{file_type}",
                title=title,
                description=desc,
                text=f"🔹 Нажмите кнопку ниже для удаления",
                buttons=[
                    [Button.switch_pm(
                        text=f"🗑 Удалить {title.lower()}",
                        start_parameter=f"del_{file_type}"
                    )]
                ],
            )
        )
    
    return results


def _build_files_delete_list(user_id: int, file_type: str) -> List[InlineArticle]:
    """Список конкретных файлов для удаления - каждый файл как отдельная плашка."""
    files = list_templates_by_type(user_id, file_type)
    results = []
    
    if not files:
        label = FILE_TYPE_LABELS.get(file_type, file_type.title())
        results.append(
            InlineArticle(
                id=f"del_{file_type}_empty",
                title=f"❌ {label} отсутствуют",
                description="Нет файлов для удаления",
                text="📭 В этой категории пока нет файлов",
                buttons=[
                    [Button.switch_pm(
                        text="🔙 Назад к меню",
                        start_parameter="files_del"
                    )]
                ],
            )
        )
        return results
    
    # Проверяем количество файлов
    total_files = len(files)
    if total_files <= 50:
        # Показываем файлы inline (до 50 файлов)
        limited_files = files
    else:
        # Файлов больше 50 - показываем меню с пагинацией вместо inline
        results.append(
            InlineArticle(
                id=f"del_{file_type}_menu_large",
                title=f"🗂 {label} ({total_files} файлов)",
                description="Слишком много файлов для быстрого просмотра",
                text=f"🔹 В категории '{label}' найдено {total_files} файлов.\nНажмите кнопку ниже для просмотра с пагинацией.",
                buttons=[
                    [Button.switch_pm(
                        text="📋 Показать все файлы",
                        start_parameter=f"del_files_{file_type}"
                    )]
                ],
            )
        )
        return results

    for idx, file_path in enumerate(limited_files):
        file_name = os.path.basename(file_path)
        # Убираем расширение для красоты
        display_name = os.path.splitext(file_name)[0]
        
        # Создаем уникальный ID для каждого файла
        file_id = f"{file_type}_{idx}_{hash(file_path) % 10000}"
        
        results.append(
            InlineArticle(
                id=f"confirm_del_{file_id}",
                title=f"🗑 {display_name}",
                description=f"Нажмите для удаления: {file_name}",
                text=f"🔹 Подтвердите удаление файла",
                buttons=[
                    [Button.switch_pm(
                        text=f"❌ Удалить «{display_name[:20]}»",
                        start_parameter=f"confirm_del_{file_type}_{idx}"
                    )]
                ],
            )
        )
    
    if len(files) > 25:
        results.append(
            InlineArticle(
                id=f"del_{file_type}_more",
                title=f"📋 ... ещё {len(files) - 25} файлов",
                description="Используйте меню в ЛС для полного списка",
                text="📋 Слишком много файлов для отображения в inline-режиме",
            )
        )
    
    return results


def _build_add_file_results(user_id: int, file_type: str) -> List[InlineArticle]:
    """Создаёт плашки для добавления файлов конкретного типа.
    
    При выборе плашки запускается процесс добавления файла в ЛС бота.
    """
    label = FILE_TYPE_LABELS.get(file_type, file_type.title())
    
    # Создаем плашку, которая при выборе запустит процесс добавления
    result = InlineArticle(
        id=f"add_start:{file_type}",
        title=f"➕ Добавить {label.lower()}",
        description=f"Начать процесс добавления {label.lower()} в библиотеку",
        text=f"🚀 Запускаю процесс добавления {label.lower()}...\n\n"
             f"Следуйте инструкциям в личных сообщениях бота.",
    )
    
    return [result]


async def _handle_inline_file_action(user_id: int, action: str, file_type: str):
    """Имитирует нажатие на кнопку добавления/удаления файла из inline-режима."""
    from telethon.tl.custom import Message

    # Создаем фиктивный callback event для имитации нажатия кнопки
    class FakeCallbackEvent:
        def __init__(self, user_id, data):
            self.sender_id = user_id
            self.data = data

    if action == "add":
        # Имитируем добавление файла
        callback_data = FILE_TYPE_ADD_CALLBACK[file_type]
        fake_ev = FakeCallbackEvent(user_id, callback_data.encode())

        # Вызываем логику добавления файла
        pending[user_id] = {"flow": "file", "file_type": file_type, "step": "name"}
        prompt = FILE_TYPE_ADD_PROMPTS[file_type]

        try:
            await bot_client.send_message(user_id, prompt)
        except Exception as e:
            logger.error(f"Failed to send file add prompt: {e}")

    elif action == "delete":
        # Имитируем удаление файла
        callback_data = f"files_delete_{file_type}"
        fake_ev = FakeCallbackEvent(user_id, callback_data.encode())

        # Получаем файлы для удаления
        files = list_templates_by_type(user_id, file_type)
        if not files:
            try:
                await bot_client.send_message(
                    user_id,
                    f"{FILE_TYPE_LABELS[file_type]} отсутствуют.",
                )
            except Exception as e:
                logger.error(f"Failed to send file delete message: {e}")
            return

        # Показываем меню удаления файлов
        buttons, page, total_pages, _ = build_file_delete_keyboard(files, file_type)
        caption = format_page_caption(
            f"{FILE_TYPE_LABELS[file_type]} для удаления", page, total_pages
        )

        try:
            await bot_client.send_message(user_id, caption, buttons=buttons)
        except Exception as e:
            logger.error(f"Failed to send file delete menu: {e}")


def _build_inline_type_results(owner_id: int, mode: str) -> List[InlineArticle]:
    """Инлайн-экран выбора типа файлов для добавления/удаления."""

    normalized_mode = "add" if mode == "add" else "delete"

    if normalized_mode == "add":
        results: List[InlineArticle] = []
        for file_type, label in FILE_TYPE_LABELS.items():
            results.append(
                InlineArticle(
                    id=f"mode:add:{file_type}",
                    title=f"➕ {label}",
                    description="Начать добавление в эту категорию",
                    text=f"INLINE_ADD:{file_type}",
                )
            )
        return results

    return _build_delete_search_results(owner_id, "")


def _build_delete_search_results(owner_id: int, search_term: str) -> List[InlineArticle]:
    aggregated: List[InlineArticle] = []
    normalized_term = " ".join(search_term.split()).strip()

    for file_type in FILE_TYPE_LABELS:
        aggregated.extend(
            _build_library_file_results(
                owner_id, file_type, normalized_term, mode="delete"
            )
        )
        if len(aggregated) >= LIBRARY_INLINE_RESULT_LIMIT:
            break

    if not aggregated:
        if normalized_term:
            description = f"Нет совпадений для \"{normalized_term}\""
        else:
            description = "В библиотеке пока нечего удалить"
        text = (
            f"Удаляемых файлов по запросу \"{normalized_term}\" не найдено."
            if normalized_term
            else "Файлы для удаления не найдены."
        )
        return [
            InlineArticle(
                id="mode:delete:empty",
                title="Файлы не найдены",
                description=description,
                text=text,
            )
        ]

    return aggregated[:LIBRARY_INLINE_RESULT_LIMIT]

def _build_library_overview_results(owner_id: int) -> List[InlineArticle]:
    """Стартовый экран инлайна для файлов.

    При пустом запросе показываем две карточки: "Добавить" и "Удалить".
    """

    add_article = InlineArticle(
        id="overview:add",
        title="➕ Добавить",
        description="Перейти к добавлению файлов",
        text=_inline_command_text("library add"),
    )

    delete_article = InlineArticle(
        id="overview:delete",
        title="🗑 Удалить",
        description="Перейти к удалению файлов",
        text=_inline_command_text("library delete"),
    )

    return [add_article, delete_article]


def _build_library_unknown_results(query: str) -> List[InlineArticle]:
    available = ", ".join(f"{key}" for key in FILE_TYPE_LABELS.keys())
    text_lines = [
        f"Категория \"{query}\" не найдена.",
        "Доступные категории:",
    ]
    text_lines.extend(f"• {label} ({key})" for key, label in FILE_TYPE_LABELS.items())
    text_lines.append("")
    text_lines.append("Используйте, например, запрос `library paste`.")

    return [
        InlineArticle(
            id="unknown",
            title="Категория не найдена",
            description=f"Доступные: {available}",
            text="\n".join(text_lines),
        )
    ]


def list_templates_by_type(owner_id: int, file_type: str) -> List[str]:
    if file_type == "paste":
        return list_text_templates(owner_id)
    if file_type == "voice":
        return list_voice_templates(owner_id)
    if file_type == "video":
        return list_video_templates(owner_id)
    if file_type == "sticker":
        return list_sticker_templates(owner_id)
    return []


def user_library_subdir(owner_id: int, file_type: str) -> Optional[str]:
    if file_type == "paste":
        return user_library_dir(owner_id, "pastes")
    if file_type == "voice":
        return user_library_dir(owner_id, "voices")
    if file_type == "video":
        return user_library_dir(owner_id, "video")
    if file_type == "sticker":
        return user_library_dir(owner_id, "stickers")
    return None


def _is_path_within(path: str, base: str) -> bool:
    if not base:
        return False
    abs_base = os.path.abspath(base)
    abs_path = os.path.abspath(path)
    try:
        return os.path.commonpath([abs_base, abs_path]) == abs_base
    except ValueError:
        return False


def _allowed_template_directories(owner_id: int, file_type: str) -> List[str]:
    directories: List[str] = []
    personal = user_library_subdir(owner_id, file_type)
    if personal:
        directories.append(personal)
    if file_type == "paste":
        directories.append(PASTES_DIR)
    elif file_type == "voice":
        directories.append(VOICES_DIR)
    elif file_type == "video":
        directories.append(VIDEO_DIR)
    elif file_type == "sticker":
        directories.append(STICKERS_DIR)
    return directories


def build_file_delete_keyboard(
    files: List[str], file_type: str, page: int = 0
) -> Tuple[List[List[Button]], int, int, int]:
    page_items, current_page, total_pages, total_count = paginate_list(list(files), page)
    rows: List[List[Button]] = []
    for path in page_items:
        display = os.path.basename(path)
        token = _register_payload(path)
        payload = f"file_del_do:{file_type}:{current_page}:{token}"
        rows.append([Button.inline(f"🗑 {display}", payload.encode())])
    if total_count > ITEMS_PER_PAGE:
        nav: List[Button] = []
        if current_page > 0:
            nav.append(Button.inline("◀️", f"file_del_page:{file_type}:{current_page - 1}".encode()))
        nav.append(Button.inline(f"{current_page + 1}/{total_pages}", b"noop"))
        if current_page < total_pages - 1:
            nav.append(Button.inline("▶️", f"file_del_page:{file_type}:{current_page + 1}".encode()))
        rows.append(nav)
    rows.append([Button.inline("⬅️ Назад", b"files_delete")])
    return rows, current_page, total_pages, total_count


def build_asset_keyboard(
    files: List[str],
    file_type: str,
    prefix: str,
    ctx: str,
    mode: Optional[str] = None,
    page: int = 0,
) -> List[List[Button]]:
    page_items, current_page, total_pages, _ = paginate_list(files, page)
    rows: List[List[Button]] = []
    base_index = current_page * ITEMS_PER_PAGE
    for offset, path in enumerate(page_items):
        base = os.path.splitext(os.path.basename(path))[0]
        title = base if len(base) <= ASSET_TITLE_MAX else base[: ASSET_TITLE_MAX - 1] + "…"
        idx = base_index + offset
        payload = f"{prefix}:{ctx}:{idx}" if mode is None else f"{prefix}:{ctx}:{mode}:{idx}"
        rows.append([Button.inline(title, payload.encode())])
    if total_pages > 1:
        mode_token = mode or ""
        nav: List[Button] = []
        if current_page > 0:
            nav.append(
                Button.inline(
                    "◀️",
                    f"asset_page:{file_type}:{ctx}:{mode_token}:{current_page - 1}".encode(),
                )
            )
        nav.append(Button.inline(f"{current_page + 1}/{total_pages}", b"noop"))
        if current_page < total_pages - 1:
            nav.append(
                Button.inline(
                    "▶️",
                    f"asset_page:{file_type}:{ctx}:{mode_token}:{current_page + 1}".encode(),
                )
            )
        rows.append(nav)
    rows.append([Button.inline("⬅️ Закрыть", b"asset_close")])
    return rows


def build_reply_prompt(ctx_info: Dict[str, Any], mode: str) -> str:
    if mode == "reply" and ctx_info.get("msg_id"):
        hint_suffix = " (будет отправлено как reply)."
    else:
        hint_suffix = " (будет отправлено как обычное сообщение)."
    return (
        f"Ответ для {ctx_info['phone']} (chat_id {ctx_info['chat_id']}): "
        f"пришли текст сообщения{hint_suffix}\n"
        "Или выбери шаблон ниже."
    )


def _reply_mode_button(label: str, ctx: str, mode: str) -> Button:
    return Button.inline(label, f"reply_mode:{ctx}:{mode}".encode())


def build_reply_options_keyboard(ctx: str, mode: str) -> List[List[Button]]:
    current_mode = "reply" if mode == "reply" else "normal"
    normal_label = ("✅ " if current_mode == "normal" else "") + "✉️ Ответить"
    reply_label = ("✅ " if current_mode == "reply" else "") + "↩️ Реплай"

    rows: List[List[Button]] = []
    if current_mode == "normal":
        rows.append([_reply_mode_button(normal_label, ctx, "normal")])
    else:
        rows.append(
            [
                _reply_mode_button(normal_label, ctx, "normal"),
                _reply_mode_button(reply_label, ctx, "reply"),
            ]
        )

    rows.extend(_library_inline_rows())
    if mode == "reply":
        rows.append([Button.inline("💬 Реакция", f"reply_reaction_menu:{ctx}:{mode}".encode())])
    rows.append([Button.inline("❌ Отмена", f"reply_cancel:{ctx}".encode())])
    return rows


async def _activate_reply_session(admin_id: int, ctx: str, mode: str) -> Optional[str]:
    """Prepare reply workflow for the given admin/context."""

    ctx_info = get_reply_context_for_admin(ctx, admin_id)
    if not ctx_info:
        return "Контекст истёк"

    await mark_dialog_read_for_context(ctx_info)

    if reply_waiting.get(admin_id):
        return "Уже жду сообщение"

    reply_waiting[admin_id] = {"ctx": ctx, "mode": mode}
    await show_interactive_message(
        admin_id,
        build_reply_prompt(ctx_info, mode),
        buttons=build_reply_options_keyboard(ctx, mode),
    )
    return None


def build_reaction_keyboard(ctx: str, mode: str) -> List[List[Button]]:
    rows: List[List[Button]] = []
    for title, emoji in REACTION_CHOICES:
        encoded = _encode_payload(emoji)
        rows.append(
            [Button.inline(f"{emoji} {title}", f"reply_reaction:{ctx}:{mode}:{encoded}".encode())]
        )
    rows.append([Button.inline("⬅️ Назад", f"reply_reaction_back:{ctx}:{mode}".encode())])
    return rows

def next_index(owner_id: int, key: str, length: int) -> int:
    rotation_state = get_rotation_state(owner_id)
    cur = rotation_state.get(key, -1)
    cur = (cur + 1) % max(1, length)
    rotation_state[key] = cur
    persist_tenants()
    return cur

# ---- connection helpers ----


class _ThreadedPySocksConnection(_ConnectionTcpFull):
    """Connection variant that avoids non-blocking PySocks issues on Windows."""

    async def _proxy_connect(self, timeout=None, local_addr=None):  # type: ignore[override]
        if python_socks:
            return await super()._proxy_connect(timeout=timeout, local_addr=local_addr)

        if isinstance(self._proxy, (tuple, list)):
            parsed = self._parse_proxy(*self._proxy)
        elif isinstance(self._proxy, dict):
            parsed = self._parse_proxy(**self._proxy)
        else:
            raise TypeError(f"Proxy of unknown format: {type(self._proxy)}")

        if ":" in parsed[1]:
            mode, address = socket.AF_INET6, (self._ip, self._port, 0, 0)
        else:
            mode, address = socket.AF_INET, (self._ip, self._port)

        sock = socks.socksocket(mode, socket.SOCK_STREAM)
        sock.set_proxy(*parsed)
        sock.settimeout(timeout)

        if local_addr is not None:
            sock.bind(local_addr)

        loop = helpers.get_running_loop()
        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, sock.connect, address),
                timeout=timeout,
            )
        except Exception:
            with contextlib.suppress(Exception):
                sock.close()
            raise

        sock.setblocking(False)
        return sock



def _proxy_tuple_from_config(config: Optional[Dict[str, Any]], *, context: str = "dynamic") -> Optional[Tuple]:
    if not config:
        return None
    if not isinstance(config, dict):
        log.warning("[%s] proxy config must be a mapping, got %s", context, type(config).__name__)
        return None
    if not config.get("enabled", True):
        return None
    
    host = config.get("host")
    port = config.get("port")
    if not host or port is None:
        log.warning("[%s] proxy config missing host/port", context)
        return None
    
    try:
        port_int = int(port)
    except (TypeError, ValueError):
        log.warning("[%s] proxy port must be integer, got %r", context, port)
        return None

    username = config.get("username") or None
    password = config.get("password") or None
    rdns = bool(config.get("rdns", True))

    proxy_type = str(config.get("type", "HTTP")).upper()
    if proxy_type in {"SOCKS", "SOCKS5"}:
        proxy_const = socks.SOCKS5
    elif proxy_type == "SOCKS4":
        proxy_const = socks.SOCKS4
    else:
        proxy_const = socks.HTTP

    return (proxy_const, host, port_int, rdns, username, password)


def build_private_proxy_tuple() -> Optional[Tuple]:
    return _proxy_tuple_from_config(PRIVATE_PROXY, context="default")


def proxy_desc(p: Optional[Tuple]) -> str:
    if not p:
        return "None"
    proxy_type, host, port, *_ = p
    proto = {
        socks.SOCKS5: "SOCKS5",
        socks.SOCKS4: "SOCKS4",
        socks.HTTP: "HTTP",
    }.get(proxy_type, str(proxy_type))
    return f"{proto}://{host}:{port}"


def parse_proxy_input(text: str) -> Dict[str, Any]:
    raw = text.strip()
    if not raw:
        raise ValueError("строка пуста")

    scheme_split = raw.split("://", 1)
    if len(scheme_split) == 2:
        proxy_type, remainder = scheme_split[0].upper(), scheme_split[1]
    else:
        proxy_type, remainder = "SOCKS5", raw

    username: Optional[str] = None
    password: Optional[str] = None
    host_part = remainder

    if "@" in remainder:
        creds, host_part = remainder.split("@", 1)
        cred_parts = creds.split(":", 1)
        if cred_parts:
            username = cred_parts[0] or None
        if len(cred_parts) == 2:
            password = cred_parts[1] or None

    host_sections = host_part.split(":")
    if len(host_sections) < 2:
        raise ValueError("ожидается формат host:port")

    host = host_sections[0].strip()
    if not host:
        raise ValueError("адрес прокси не может быть пустым")

    port_str = host_sections[1].strip()
    if not port_str:
        raise ValueError("порт обязателен")
    try:
        port = int(port_str)
    except (TypeError, ValueError):
        raise ValueError("порт должен быть числом")
    if not (1 <= port <= 65535):
        raise ValueError("порт вне диапазона 1-65535")

    if len(host_sections) >= 3 and username is None:
        username = host_sections[2] or None
    if len(host_sections) >= 4 and password is None:
        password = host_sections[3] or None

    cfg: Dict[str, Any] = {
        "enabled": True,
        "type": proxy_type,
        "host": host,
        "port": port,
        "rdns": True,
    }
    if username:
        cfg["username"] = username
    if password:
        cfg["password"] = password
    return cfg


# ---- bot client ----


def build_bot_proxy_config() -> Dict[str, Any]:
    """Derive proxy settings for the service bot itself.

    Historically the bot proxy configuration duplicated ``DYNAMIC_PROXY`` but
    without the optional credentials.  When the upstream provider requires
    authentication this resulted in ``GeneralProxyError`` during start-up,
    because the proxy rejected the unauthenticated SOCKS5 handshake.  Reuse the
    ``PRIVATE_PROXY`` values instead so the bot client benefits from the same
    credentials (while still allowing manual overrides by editing the returned
    mapping).
    """

    base: Dict[str, Any] = {}
    if isinstance(PRIVATE_PROXY, dict):
        base.update(PRIVATE_PROXY)

    # If the proxy config was disabled entirely fall back to the
    # default behaviour of running without a proxy (``enabled`` evaluates to
    # False downstream and ``_proxy_tuple_from_config`` will return ``None``).
    base.setdefault("enabled", True)
    return base


BOT_PROXY_CONFIG = build_bot_proxy_config()
BOT_PROXY_TUPLE = _proxy_tuple_from_config(BOT_PROXY_CONFIG, context="bot")

# Используем первую пару API_KEYS для бота
bot_client = TelegramClient(
    StringSession(),
    API_KEYS[0]["api_id"],
    API_KEYS[0]["api_hash"],
    proxy=BOT_PROXY_TUPLE,
    connection=_ThreadedPySocksConnection,
)

# безопасная отправка админу (не падаем, если админ ещё не нажал /start)
def is_admin(user_id: Any) -> bool:
    try:
        key = tenant_key(user_id)
    except (TypeError, ValueError) as exc:
        log.warning("Cannot normalise ID %r for admin check: %s", user_id, exc)
        return False
    return key in tenants


def _extract_event_user_id(ev: Any) -> Optional[int]:
    """Best-effort extraction of the Telegram user id from an event."""

    candidates = [getattr(ev, "sender_id", None), getattr(ev, "chat_id", None)]
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            return _normalize_peer_id(candidate)
        except (TypeError, ValueError):
            continue

    peer = getattr(ev, "peer_id", None)
    if peer is not None:
        try:
            return _normalize_peer_id(peer)
        except (TypeError, ValueError):
            return None
    return None


def _format_filesize(size: Optional[int]) -> str:
    if not size:
        return ""
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if size < 1024 or unit == "ГБ":
            return f"{size:.1f} {unit}" if unit != "Б" else f"{size} {unit}"
        size /= 1024
    return f"{size:.1f} ГБ"


def _describe_media(event: Any) -> Tuple[str, str]:
    checks: List[Tuple[str, str, str]] = [
        ("voice", "voice", "Голосовое сообщение"),
        ("video_note", "video_note", "Видеосообщение (кружок/медиа)"),
        ("video", "video", "Видео"),
        ("audio", "audio", "Аудио"),
        ("photo", "photo", "Фотография"),
        ("gif", "gif", "GIF"),
        ("sticker", "sticker", "Стикер"),
        ("document", "document", "Документ"),
    ]
    for attr, code, description in checks:
        if getattr(event, attr, None):
            return code, description
    if getattr(event, "media", None):
        return "media", "Медиа"
    return "", ""


def _resolve_media_filename(event: Any, media_code: str) -> str:
    file_obj = getattr(event, "file", None)
    if file_obj is not None:
        name = getattr(file_obj, "name", None)
        if name:
            return name
        ext = getattr(file_obj, "ext", None) or ""
        if not ext:
            mime_type = getattr(file_obj, "mime_type", None)
            if mime_type:
                ext = mimetypes.guess_extension(mime_type) or ""
        if not ext:
            fallback_map = {
                "voice": ".ogg",
                "video_note": ".mp4",
                "video": ".mp4",
                "photo": ".jpg",
                "audio": ".mp3",
                "gif": ".gif",
                "sticker": ".webp",
            }
            ext = fallback_map.get(media_code, "")
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"{media_code or 'media'}_{timestamp}{ext}"
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return f"media_{timestamp}"


def _collapse_whitespace(text: str) -> str:
    return " ".join(text.split())


def _extract_reply_to_msg_id(ev: Any) -> Optional[int]:
    message = getattr(ev, "message", ev)
    reply_header = getattr(message, "reply_to", None) or getattr(ev, "reply_to", None)
    if reply_header is not None:
        reply_msg_id = getattr(reply_header, "reply_to_msg_id", None)
        if isinstance(reply_msg_id, int):
            return reply_msg_id
    reply_msg_id = getattr(message, "reply_to_msg_id", None)
    if isinstance(reply_msg_id, int):
        return reply_msg_id
    reply_msg_id = getattr(ev, "reply_to_msg_id", None)
    if isinstance(reply_msg_id, int):
        return reply_msg_id
    return None


_admin_reply_threads: Dict[int, Dict[Any, int]] = defaultdict(dict)


@dataclass
class _NotificationThreadState:
    message_id: int
    thread_id: str
    ctx_id: str
    bullets: List[str]
    header_lines: List[str]
    history_html: str
    history_collapsed: bool = True


notification_threads: Dict[int, Dict[str, _NotificationThreadState]] = defaultdict(dict)

MAX_NOTIFICATION_BULLETS = 20
MAX_HISTORY_MESSAGES = 10


def _make_thread_id(phone: str, chat_id: int) -> str:
    return f"{phone}:{chat_id}"


def _format_multiline_html(text: str) -> str:
    escaped = html.escape(text.strip())
    return escaped.replace("\n", "<br>")


def _format_incoming_bullet(text: Optional[str], media_description: Optional[str]) -> str:
    parts: List[str] = []
    if text:
        parts.append(_format_multiline_html(text))
    if media_description:
        parts.append(f"<i>{html.escape(media_description)}</i>")
    if not parts:
        parts.append("<i>Без текста</i>")
    first = parts[0]
    extras = parts[1:]
    bullet = f"- {first}"
    for extra in extras:
        bullet += f"<br>&nbsp;&nbsp;{extra}"
    return bullet


def _format_history_entry(message: Message) -> str:
    sender_label = "🧑‍💼 Вы" if message.out else "👥 Собеседник"
    raw_text = (message.raw_text or "").strip()
    if raw_text:
        text_html = _format_multiline_html(raw_text)
    else:
        text_html = "<i>Без текста</i>"
    entry = f"<b>{sender_label}:</b> {text_html}"
    media_code, media_desc = _describe_media(message)
    file_info: Optional[str] = None
    if media_code:
        file_obj = getattr(message, "file", None)
        if message.out:
            name = getattr(file_obj, "name", None)
            if not name and media_code:
                with contextlib.suppress(Exception):
                    name = _resolve_media_filename(message, media_code)
            mime_type = getattr(file_obj, "mime_type", None) or media_desc or media_code
            if name:
                file_info = f"📎 Файл: {html.escape(name)} ({html.escape(mime_type)})"
            else:
                file_info = f"📎 Файл: {html.escape(mime_type)}"
        else:
            label = media_desc or media_code
            file_info = f"📎 Файл: {html.escape(label)}"
    if file_info:
        entry += f"<br>&nbsp;&nbsp;{file_info}"
    return entry


async def _build_history_html(client: TelegramClient, peer: Any, limit: int = MAX_HISTORY_MESSAGES) -> str:
    try:
        messages = await client.get_messages(peer, limit=limit)
    except Exception as exc:
        log.warning("Не удалось загрузить историю диалога: %s", exc)
        return "<i>Не удалось загрузить историю</i>"
    if not messages:
        return "<i>История пуста</i>"
    entries = [_format_history_entry(msg) for msg in reversed(messages)]
    return "<br>".join(entries)


def _library_inline_rows() -> List[List[Button]]:
    """Shortcut for commonly used inline query buttons."""

    return [
        [
            library_inline_button("paste", "📄 Пасты ↗"),
            library_inline_button("voice", "🎙 Голосовые ↗"),
        ],
        [
            library_inline_button("video", "📹 Медиа ↗"),
            library_inline_button("sticker", "💟 Стикеры ↗"),
        ],
    ]


def _build_notification_text(
    header_lines: List[str],
    bullet_lines: List[str],
    history_html: str,
    collapsed: bool,
) -> str:
    lines = list(header_lines)
    lines.append("")
    lines.append("Собеседник пишет:")
    lines.extend(bullet_lines)
    lines.append("")
    if collapsed:
        lines.append("⏵ История диалога (последние 10 сообщений)")
    else:
        lines.append("⏷ История диалога (последние 10 сообщений)")
        if history_html:
            lines.append(history_html)
    return "\n".join(lines)


def _build_notification_buttons(
    ctx_id: str,
    thread_id: str,
    collapsed: bool,
) -> List[List[Button]]:
    rows: List[List[Button]] = [
        [
            Button.switch_inline("✉️ Ответить", query=f"reply {ctx_id}", same_peer=True),
            Button.switch_inline("↩️ Реплай", query=f"reply_to {ctx_id}", same_peer=True),
        ],
        [Button.inline("👀 Прочитать", f"mark_read:{ctx_id}".encode())],
        [Button.inline("🚫 Заблокировать", f"block_contact:{ctx_id}".encode())],
    ]
    toggle_label = "⏵ История" if collapsed else "⏷ История"
    toggle_state = "open" if collapsed else "close"
    rows.append(
        [
            Button.inline(
                toggle_label,
                f"history_toggle:{thread_id}:{toggle_state}".encode(),
            )
        ]
    )
    return rows


def clear_notification_thread(admin_id: int, thread_id: str) -> None:
    threads = notification_threads.get(admin_id)
    if not threads:
        return
    threads.pop(thread_id, None)
    if not threads:
        notification_threads.pop(admin_id, None)


async def safe_send_admin(
    text: str,
    *,
    owner_id: Optional[int] = None,
    reply_context: Optional[Any] = None,
    **kwargs,
):
    targets = {owner_id} if owner_id is not None else all_admin_ids()
    for admin_id in targets:
        send_kwargs = dict(kwargs)
        if (
            reply_context is not None
            and "reply_to" not in send_kwargs
            and "reply_to_msg_id" not in send_kwargs
        ):
            thread_map = _admin_reply_threads[admin_id]
            reply_msg_id = thread_map.get(reply_context)
            if reply_msg_id is not None:
                send_kwargs["reply_to"] = reply_msg_id
        try:
            msg = await bot_client.send_message(admin_id, text, **send_kwargs)
        except Exception as e:
            logging.getLogger("mgrbot").warning(
                "Cannot DM admin %s yet (probably admin hasn't started the bot): %s",
                admin_id,
                e,
            )
            continue
        if reply_context is not None:
            _admin_reply_threads[admin_id][reply_context] = msg.id


async def safe_send_admin_file(
    file_data: bytes,
    filename: str,
    *,
    owner_id: Optional[int] = None,
    reply_context: Optional[Any] = None,
    **kwargs,
) -> None:
    if not file_data:
        return
    targets = {owner_id} if owner_id is not None else all_admin_ids()
    for admin_id in targets:
        try:
            bio = BytesIO(file_data)
            bio.name = filename
            send_kwargs = dict(kwargs)
            if (
                reply_context is not None
                and "reply_to" not in send_kwargs
                and "reply_to_msg_id" not in send_kwargs
            ):
                thread_map = _admin_reply_threads[admin_id]
                reply_msg_id = thread_map.get(reply_context)
                if reply_msg_id is not None:
                    send_kwargs["reply_to"] = reply_msg_id

            result = await bot_client.send_file(admin_id, bio, **send_kwargs)
        except Exception as e:
            logging.getLogger("mgrbot").warning(
                "Cannot send file to admin %s yet (probably admin hasn't started the bot): %s",
                admin_id,
                e,
            )
            continue
        if reply_context is not None:
            if isinstance(result, (list, tuple)):
                last_msg = result[-1] if result else None
            else:
                last_msg = result
            if last_msg is not None and hasattr(last_msg, "id"):
                _admin_reply_threads[admin_id][reply_context] = last_msg.id


async def answer_callback(event: events.CallbackQuery.Event, *args, **kwargs):
    try:
        return await event.answer(*args, **kwargs)
    except QueryIdInvalidError:
        raw = getattr(event, "data", None)
        if isinstance(raw, bytes):
            data_repr = raw.decode("utf-8", errors="replace")
        else:
            data_repr = str(raw)
        log.warning(
            "Callback query already handled or expired for %s (data=%s)",
            getattr(event, "sender_id", None),
            data_repr,
        )
    return None


def resolve_proxy_for_account(owner_id: int, phone: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    raw_override = meta.get("proxy_override")
    override_signature = "__none__"
    override_cfg: Optional[Dict[str, Any]] = None
    override_enabled = True
    warnings: List[Tuple[str, Optional[str]]] = []

    if raw_override is None:
        override_signature = "__none__"
    elif isinstance(raw_override, dict):
        try:
            override_signature = json.dumps(raw_override, sort_keys=True, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            override_signature = str(raw_override)
        override_cfg = raw_override
        override_enabled = bool(override_cfg.get("enabled", True))
    else:
        override_signature = "__invalid_type__"
        override_enabled = True
        warnings.append(("invalid_type", type(raw_override).__name__))

    proxy_tuple: Optional[Tuple] = None
    is_dynamic = False

    if override_cfg is not None:
        if override_enabled:
            proxy_tuple = _proxy_tuple_from_config(override_cfg, context=f"account:{phone}")
            if proxy_tuple is None:
                warnings.append(("override_invalid", None))
            else:
                is_dynamic = bool(override_cfg.get("dynamic"))
        else:
            proxy_tuple = None

    if proxy_tuple is None and override_enabled:
        tenant_cfg = get_active_tenant_proxy(owner_id)
        if tenant_cfg:
            tenant_tuple = _proxy_tuple_from_config(tenant_cfg, context=f"tenant:{owner_id}")
            if tenant_tuple is not None:
                proxy_tuple = tenant_tuple
                is_dynamic = bool(tenant_cfg.get("dynamic"))
            else:
                warnings.append(("tenant_invalid", None))

    if proxy_tuple is None and override_enabled:
        default_tuple = build_private_proxy_tuple()
        if default_tuple is not None:
            proxy_tuple = default_tuple
            is_dynamic = bool(PRIVATE_PROXY.get("dynamic", False))

    return {
        "proxy_tuple": proxy_tuple,
        "dynamic": is_dynamic,
        "override_signature": override_signature,
        "override_cfg": override_cfg,
        "override_enabled": override_enabled,
        "warnings": warnings,
    }


def recompute_account_proxy_meta(owner_id: int, phone: str, meta: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    resolution = resolve_proxy_for_account(owner_id, phone, meta)
    desc = proxy_desc(resolution["proxy_tuple"])
    dynamic = resolution["dynamic"]
    changed = False
    if meta.get("proxy_desc") != desc:
        meta["proxy_desc"] = desc
        changed = True
    if meta.get("proxy_dynamic") != dynamic:
        meta["proxy_dynamic"] = dynamic
        changed = True
    return changed, resolution

# ---- worker ----


@dataclass
class _SendQueueItem:
    future: asyncio.Future
    chat_id: int
    message: str
    peer: Optional[Any]
    reply_to_msg_id: Optional[int]
    mark_read_msg_id: Optional[int]


class AccountWorker:
    def __init__(self, owner_id: int, phone: str, api_id: int, api_hash: str, device: Dict[str,str], session_str: Optional[str]):
        self.owner_id = owner_id
        self.phone = phone
        self.api_id = api_id
        self.api_hash = api_hash
        self.device = device
        self.session_file = user_session_path(owner_id, phone)
        self.session = StringSession(session_str) if session_str else StringSession()
        self.client: Optional[TelegramClient] = None
        self.started = False
        self._keepalive_task: Optional[asyncio.Task] = None
        self.account_name: Optional[str] = None
        self._proxy_tuple: Optional[Tuple] = None
        self._proxy_desc: str = proxy_desc(None)
        self._proxy_dynamic: bool = False
        self._proxy_override_signature: Optional[str] = None
        self._proxy_forced_off: bool = False
        self._proxy_force_reason: Optional[str] = None
        self._send_queue: asyncio.Queue = asyncio.Queue()
        self._send_worker_task: Optional[asyncio.Task] = None
        self._last_code_delivery: Optional[str] = None

    def _reset_session_state(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.session_file)
        self.session = StringSession()

    def _set_session_invalid_flag(self, invalid: bool) -> None:
        meta = get_account_meta(self.owner_id, self.phone)
        if not meta:
            return
        changed = False
        if invalid:
            if not meta.get("session_invalid"):
                meta["session_invalid"] = True
                changed = True
        else:
            if meta.pop("session_invalid", None) is not None:
                changed = True
        if changed:
            persist_tenants()

    def _set_account_state(self, state: Optional[str], details: Optional[str] = None) -> None:
        meta = get_account_meta(self.owner_id, self.phone)
        if not meta:
            return
        changed = False
        if state:
            if meta.get("state") != state:
                meta["state"] = state
                changed = True
        else:
            if meta.pop("state", None) is not None:
                changed = True
        if details:
            if meta.get("state_note") != details:
                meta["state_note"] = details
                changed = True
        else:
            if meta.pop("state_note", None) is not None:
                changed = True
        if changed:
            persist_tenants()

    async def _handle_account_disabled(self, state: str, error: Exception) -> None:
        human = "заморожен" if state == "frozen" else "заблокирован"
        log.warning("[%s] account %s by Telegram: %s", self.phone, human, error)
        meta = get_account_meta(self.owner_id, self.phone) or {}
        prev_state = meta.get("state")
        prev_note = meta.get("state_note")
        self._set_account_state(state, str(error))
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
        self._keepalive_task = None
        if self.client:
            with contextlib.suppress(Exception):
                await self.client.disconnect()
        self.client = None
        self.started = False
        if prev_state != state or prev_note != str(error):
            await safe_send_admin(
                (
                    f"⛔️ <b>{self.phone}</b>: аккаунт {human} Telegram.\n"
                    f"Ответ: <code>{error}</code>"
                ),
                owner_id=self.owner_id,
                parse_mode="html",
            )

    async def _handle_authkey_duplication(self, error: Exception) -> None:
        log.warning(
            "[%s] session revoked by Telegram due to IP mismatch: %s",
            self.phone,
            error,
        )
        if self._keepalive_task and not self._keepalive_task.done():
            self._keepalive_task.cancel()
        self._keepalive_task = None
        if self.client:
            with contextlib.suppress(Exception):
                await self.client.disconnect()
        self.client = None
        self.started = False
        self._reset_session_state()
        self._set_session_invalid_flag(True)
        unregister_worker(self.owner_id, self.phone)
        await safe_send_admin(
            (
                f"⚠️ <b>{self.phone}</b>: Telegram аннулировал сессию из-за одновременного"
                " входа с разных IP. Добавь аккаунт заново, чтобы получить новую сессию."
            ),
            owner_id=self.owner_id,
            parse_mode="html",
        )

    def _update_proxy_meta(self) -> None:
        meta = ensure_account_meta(self.owner_id, self.phone)
        changed = False
        if meta.get("proxy_desc") != self._proxy_desc:
            meta["proxy_desc"] = self._proxy_desc
            changed = True
        if meta.get("proxy_dynamic") != self._proxy_dynamic:
            meta["proxy_dynamic"] = self._proxy_dynamic
            changed = True
        if changed:
            persist_tenants()

    def _disable_proxy_for_session(self, reason: str) -> None:
        if self._proxy_forced_off:
            return
        self._proxy_forced_off = True
        self._proxy_force_reason = reason
        self._proxy_tuple = None
        self._proxy_dynamic = False
        self._proxy_desc = proxy_desc(None)
        self._update_proxy_meta()
        log.warning(
            "[%s] proxy disabled for this session due to error: %s. Подключение продолжится без прокси.",
            self.phone,
            reason,
        )

    def _enable_proxy_for_session(self) -> None:
        if not self._proxy_forced_off:
            return
        self._proxy_forced_off = False
        self._proxy_force_reason = None

    async def _disconnect_client(self) -> None:
        if not self.client:
            return
        with contextlib.suppress(Exception):
            await self.client.disconnect()
        self.client = None

    def _ensure_send_worker(self) -> None:
        if self._send_worker_task is None or self._send_worker_task.done():
            self._send_worker_task = asyncio.create_task(self._send_queue_worker())

    async def _send_queue_worker(self) -> None:
        while True:
            item = await self._send_queue.get()
            if item is None:
                self._send_queue.task_done()
                break
            if item.future.cancelled():
                self._send_queue.task_done()
                continue
            try:
                result = await self._send_outgoing_impl(
                    chat_id=item.chat_id,
                    message=item.message,
                    peer=item.peer,
                    reply_to_msg_id=item.reply_to_msg_id,
                    mark_read_msg_id=item.mark_read_msg_id,
                )
            except Exception as exc:
                if not item.future.done():
                    item.future.set_exception(exc)
            else:
                if not item.future.done():
                    item.future.set_result(result)
            finally:
                self._send_queue.task_done()

    async def _shutdown_send_worker(self) -> None:
        if self._send_worker_task is not None:
            await self._send_queue.put(None)
            try:
                await self._send_worker_task
            except asyncio.CancelledError:
                pass
            self._send_worker_task = None
        while True:
            try:
                leftover = self._send_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if leftover is None:
                self._send_queue.task_done()
                continue
            if not leftover.future.done():
                leftover.future.set_exception(
                    RuntimeError("Отправка отменена: аккаунт остановлен")
                )
            self._send_queue.task_done()
        self._send_queue = asyncio.Queue()

    async def _send_outgoing_impl(
        self,
        *,
        chat_id: int,
        message: str,
        peer: Optional[Any],
        reply_to_msg_id: Optional[int],
        mark_read_msg_id: Optional[int],
    ):
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        await self._simulate_typing(client, peer, message)
        try:
            sent = await client.send_message(peer, message, reply_to=reply_to_msg_id)
            if mark_read_msg_id is not None:
                with contextlib.suppress(Exception):
                    await client.send_read_acknowledge(peer, max_id=mark_read_msg_id)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")
        return sent

    def _select_proxy(self, *, force_new: bool = False) -> Optional[Tuple]:
        if self._proxy_forced_off:
            if self._proxy_tuple is not None or self._proxy_desc != proxy_desc(None) or self._proxy_dynamic:
                self._proxy_tuple = None
                self._proxy_dynamic = False
                self._proxy_desc = proxy_desc(None)
                self._update_proxy_meta()
            return None

        meta = get_account_meta(self.owner_id, self.phone) or {}
        resolution = resolve_proxy_for_account(self.owner_id, self.phone, meta)
        override_signature = resolution["override_signature"]

        need_refresh = force_new or self._proxy_tuple is None or override_signature != self._proxy_override_signature
        if not need_refresh:
            return self._proxy_tuple

        if self._proxy_override_signature != override_signature:
            for code, detail in resolution.get("warnings", []):
                if code == "invalid_type":
                    log.warning(
                        "[%s] proxy_override must be a mapping, got %s. Игнорирую переопределение.",
                        self.phone,
                        detail or "unknown",
                    )
                elif code == "override_invalid":
                    log.warning(
                        "[%s] proxy_override указано, но конфигурация некорректна. Пытаюсь использовать пользовательский или динамический прокси.",
                        self.phone,
                    )
                elif code == "tenant_invalid":
                    log.warning(
                        "[%s] пользовательский прокси для пользователя настроен, но параметры некорректны. Подключение пойдёт без него или с глобальным прокси.",
                        self.phone,
                    )

        proxy_tuple: Optional[Tuple] = resolution["proxy_tuple"]
        is_dynamic = resolution["dynamic"]
        override_cfg = resolution.get("override_cfg")
        override_enabled = resolution.get("override_enabled", True)

        if (
            proxy_tuple is None
            and override_cfg is not None
            and override_enabled
            and self._proxy_override_signature != override_signature
        ):
            log.warning(
                "[%s] proxy_override не удалось применить. Подключение пойдёт без прокси или с системным по умолчанию.",
                self.phone,
            )

        self._proxy_tuple = proxy_tuple
        self._proxy_dynamic = is_dynamic
        self._proxy_desc = proxy_desc(proxy_tuple)
        self._proxy_override_signature = override_signature
        self._update_proxy_meta()
        return self._proxy_tuple

    @property
    def proxy_description(self) -> str:
        if self._proxy_tuple is None:
            self._select_proxy()
        return self._proxy_desc

    @property
    def using_dynamic_proxy(self) -> bool:
        if self._proxy_tuple is None:
            self._select_proxy()
        return self._proxy_dynamic

    async def refresh_proxy(self, restart: bool = True) -> None:
        self._proxy_forced_off = False
        self._proxy_force_reason = None
        self._proxy_tuple = None
        self._proxy_dynamic = False
        self._proxy_desc = proxy_desc(None)
        self._proxy_override_signature = None
        self._select_proxy(force_new=True)
        if restart and self.started:
            was_started = self.started
            await self.stop()
            self.client = None
            if was_started:
                try:
                    await self.start()
                except Exception as exc:
                    log.warning(
                        "[%s] не удалось перезапустить аккаунт после обновления прокси: %s",
                        self.phone,
                        exc,
                    )
                    raise

    def _make_client(self) -> TelegramClient:
        proxy_cfg = self._select_proxy()
        return TelegramClient(
            self.session, self.api_id, self.api_hash,
            proxy=proxy_cfg,
            connection=_ThreadedPySocksConnection,
            device_model=self.device.get("device_model"),
            system_version=self.device.get("system_version"),
            app_version=self.device.get("app_version"),
            lang_code=self.device.get("lang_code"),
        )
    
    async def _simulate_chat_action(
        self,
        client: TelegramClient,
        peer: Any,
        action: str,
        duration: float,
    ) -> None:
        if duration <= 0:
            return
        try:
            async with client.action(peer, action):
                await asyncio.sleep(duration)
        except Exception as e:
            log.debug("[%s] unable to send chat action %s: %s", self.phone, action, e)
            await asyncio.sleep(duration)
            return

    async def _simulate_typing(self, client: TelegramClient, peer: Any, message: str) -> None:
        duration = _typing_duration(message)
        await self._simulate_chat_action(client, peer, "typing", duration)

    async def _simulate_voice_recording(
        self, client: TelegramClient, peer: Any, file_path: Optional[str] = None
    ) -> None:
        duration = _voice_record_duration(file_path)
        await self._simulate_chat_action(client, peer, "record-audio", duration)

    async def _simulate_round_recording(
        self, client: TelegramClient, peer: Any, file_path: Optional[str] = None
    ) -> None:
        duration = _video_note_record_duration(file_path)
        await self._simulate_chat_action(client, peer, "record-round", duration)

    async def _simulate_photo_upload(
        self, client: TelegramClient, peer: Any, file_path: Optional[str] = None
    ) -> None:
        duration = _photo_upload_duration(file_path)
        await self._simulate_chat_action(client, peer, "upload-photo", duration)

    async def _simulate_video_upload(
        self, client: TelegramClient, peer: Any, file_path: Optional[str] = None
    ) -> None:
        duration = _video_upload_duration(file_path)
        await self._simulate_chat_action(client, peer, "upload-video", duration)

    async def _ensure_client(self) -> TelegramClient:
        if not self.client:
            self.client = self._make_client()
        if not self.client.is_connected():
            try:
                await self.client.connect()
            except ValueError as e:
                if "non-blocking" in str(e).lower() and not self._proxy_forced_off:
                    await self._disconnect_client()
                    self._disable_proxy_for_session(str(e))
                    self.client = self._make_client()
                    await self.client.connect()
                else:
                    raise
        return self.client

    async def start(self):
        try:
            self.client = await self._ensure_client()
            if not await self.client.is_user_authorized():
                return
            
            try:
                me = await self.client.get_me()
                self.account_name = get_display_name(me)
            except Exception:
                self.account_name = None

            meta = ensure_account_meta(self.owner_id, self.phone)
            changed = False
            if self.account_name:
                if meta.get("full_name") != self.account_name:
                    meta["full_name"] = self.account_name
                    changed = True
            else:
                if meta.pop("full_name", None) is not None:
                    changed = True
            if changed:
                persist_tenants()

            @self.client.on(events.NewMessage(incoming=True))
            async def on_new(ev):
                # Фильтр: принимаем только личные чаты
                if not ev.is_private:
                    return

                # Получаем информацию об отправителе для проверки на бота
                sender_entity = None
                with contextlib.suppress(Exception):
                    sender_entity = await ev.get_sender()

                # Фильтр: игнорируем сообщения от ботов
                if getattr(sender_entity, "bot", False):
                    return

                txt = ev.raw_text or ""
                ctx_id = secrets.token_hex(4)
                peer = None
                try:
                    peer = await ev.get_input_chat()
                except Exception:
                    try:
                        peer = await ev.get_input_sender()
                    except Exception:
                        peer = None
                # AI автоответчик (шаблоны + GPT-подсказки)
                try:
                    await handle_ai_autoreply(self, ev, peer)
                except Exception as ai_err:
                    log.warning("[%s] ошибка AI-автоответа: %s", self.phone, ai_err)
                account_meta = get_account_meta(self.owner_id, self.phone) or {}
                account_display = self.account_name or account_meta.get("full_name")
                if not account_display:
                    account_display = self.phone

                if not isinstance(sender_entity, User):
                    return
                
                sender_name = get_display_name(sender_entity) if sender_entity else None
                sender_username = getattr(sender_entity, "username", None) if sender_entity else None
                tag_value = f"@{sender_username}" if sender_username else "hidden"
                sender_id_display = str(ev.sender_id) if ev.sender_id is not None else "unknown"

                header_lines = [
                    f"👤 Аккаунт: <b>{html.escape(account_display)}</b>",
                    f"👥 Собеседник: <b>{html.escape(sender_name) if sender_name else '—'}</b>",
                    f"🔗 {html.escape(tag_value)}",
                    f"ID Собеседника: {html.escape(sender_id_display)}",
                ]
                reply_to_msg_id = _extract_reply_to_msg_id(ev)
                reply_preview_html: Optional[str] = None
                if reply_to_msg_id is not None:
                    reply_msg: Optional[Message] = None
                    with contextlib.suppress(Exception):
                        reply_msg = await ev.get_reply_message()
                    if reply_msg and getattr(reply_msg, "out", False):
                        preview_source = (reply_msg.raw_text or "").strip()
                        if preview_source:
                            preview = _collapse_whitespace(preview_source)
                            if len(preview) > 160:
                                preview = preview[:157].rstrip() + "…"
                            reply_preview_html = html.escape(preview)
                        header_lines.extend(
                            [
                                "",
                                f"↩️ Собеседник ответил на ваше сообщение (ID {reply_to_msg_id}).",
                            ]
                        )
                        if reply_preview_html:
                            header_lines.append(f"📝 Цитата: {reply_preview_html}")
                media_bytes: Optional[bytes] = None
                media_filename: Optional[str] = None
                media_description: Optional[str] = None
                media_notice: Optional[str] = None
                media_code, media_description_raw = _describe_media(ev)
                file_obj = getattr(ev, "file", None)
                media_size = getattr(file_obj, "size", None)
                has_media = bool(getattr(ev, "media", None))
                if media_description_raw:
                    media_description = media_description_raw
                elif has_media:
                    media_description = "Медиа"
                if media_description:
                    size_display = _format_filesize(media_size)
                    description_line = f"🗂 Вложение: <b>{html.escape(media_description)}</b>"
                    if size_display:
                        description_line += f" ({size_display})"
                    header_lines.append(description_line)
                if has_media:
                    if media_size and media_size > MAX_MEDIA_FORWARD_SIZE:
                        formatted_limit = _format_filesize(MAX_MEDIA_FORWARD_SIZE)
                        formatted_size = _format_filesize(media_size)
                        media_notice = (
                            f"⚠️ Файл {html.escape(media_description or 'медиа')} "
                            f"({formatted_size}) превышает лимит авто-перенаправления"
                            f" ({formatted_limit})."
                        )
                    else:
                        buffer = BytesIO()
                        try:
                            downloaded = await ev.download_media(file=buffer)
                            if downloaded is not None:
                                media_bytes = buffer.getvalue()
                        except Exception as download_error:
                            media_notice = (
                                "⚠️ Не удалось скачать вложение: "
                                f"{html.escape(str(download_error))}"
                            )
                        finally:
                            buffer.close()
                        if media_bytes:
                            media_filename = _resolve_media_filename(ev, media_code)
                        elif media_notice is None:
                            media_notice = "⚠️ Вложение не удалось скачать."
                if media_notice:
                    header_lines.extend(["", media_notice])

                reply_contexts[ctx_id] = {
                    "owner_id": self.owner_id,
                    "phone": self.phone,
                    "chat_id": ev.chat_id,
                    "sender_id": ev.sender_id,
                    "peer": peer,
                    "msg_id": ev.id,
                }
                reply_context_key = (self.phone, ev.chat_id)
                thread_id = _make_thread_id(self.phone, ev.chat_id)
                bullet_entry = _format_incoming_bullet(txt, media_description)
                history_html = await _build_history_html(
                    self.client, peer or ev.chat_id, limit=MAX_HISTORY_MESSAGES
                )
                header_snapshot = list(header_lines)
                state_map = notification_threads.setdefault(self.owner_id, {})
                state = state_map.get(thread_id)
                if state:
                    state.bullets.append(bullet_entry)
                    if len(state.bullets) > MAX_NOTIFICATION_BULLETS:
                        state.bullets = state.bullets[-MAX_NOTIFICATION_BULLETS:]
                    state.ctx_id = ctx_id
                    state.header_lines = header_snapshot
                    state.history_html = history_html
                    buttons = _build_notification_buttons(
                        ctx_id, thread_id, state.history_collapsed
                    )
                    notification_text = _build_notification_text(
                        state.header_lines,
                        state.bullets,
                        state.history_html,
                        state.history_collapsed,
                    )
                    try:
                        await bot_client.edit_message(
                            self.owner_id,
                            state.message_id,
                            notification_text,
                            buttons=buttons,
                            parse_mode="html",
                            link_preview=False,
                        )
                    except Exception as edit_error:
                        log.warning(
                            "[%s] не удалось обновить уведомление: %s",
                            self.phone,
                            edit_error,
                        )
                        state_map.pop(thread_id, None)
                        await safe_send_admin(
                            notification_text,
                            buttons=buttons,
                            parse_mode="html",
                            link_preview=False,
                            owner_id=self.owner_id,
                            reply_context=reply_context_key,
                        )
                        msg_id = _admin_reply_threads[self.owner_id].get(reply_context_key)
                        if msg_id is not None:
                            state_map[thread_id] = _NotificationThreadState(
                                message_id=msg_id,
                                thread_id=thread_id,
                                ctx_id=ctx_id,
                                bullets=list(state.bullets),
                                header_lines=header_snapshot,
                                history_html=history_html,
                                history_collapsed=state.history_collapsed,
                            )
                    else:
                        state.header_lines = header_snapshot
                        state.history_html = history_html
                else:
                    bullets = [bullet_entry]
                    buttons = _build_notification_buttons(ctx_id, thread_id, True)
                    notification_text = _build_notification_text(
                        header_snapshot, bullets, history_html, True
                    )
                    await safe_send_admin(
                        notification_text,
                        buttons=buttons,
                        parse_mode="html",
                        link_preview=False,
                        owner_id=self.owner_id,
                        reply_context=reply_context_key,
                    )
                    msg_id = _admin_reply_threads[self.owner_id].get(reply_context_key)
                    if msg_id is not None:
                        state_map[thread_id] = _NotificationThreadState(
                            message_id=msg_id,
                            thread_id=thread_id,
                            ctx_id=ctx_id,
                            bullets=bullets,
                            header_lines=header_snapshot,
                            history_html=history_html,
                            history_collapsed=True,
                        )
                if media_bytes and media_filename:
                    media_caption_lines = [
                        f"👤 Аккаунт: <b>{html.escape(account_display)}</b>",
                        f"👥 Собеседник: <b>{html.escape(sender_name) if sender_name else '—'}</b>",
                    ]
                    if media_description:
                        media_caption_lines.append(
                            f"📎 {html.escape(media_description)}"
                        )
                    await safe_send_admin_file(
                        media_bytes,
                        media_filename,
                        owner_id=self.owner_id,
                        reply_context=reply_context_key,
                        caption="\n".join(media_caption_lines),
                        parse_mode="html",
                    )

            await self.client.start()
        except AuthKeyDuplicatedError as e:
            await self._handle_authkey_duplication(e)
            raise
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise

        self.started = True
        with open(self.session_file, "w", encoding="utf-8") as f:
            f.write(self.client.session.save())
        self._set_session_invalid_flag(False)
        self._set_account_state(None)
        log.info(
            "[%s] started on %s; device=%s",
            self.phone,
            self.proxy_description,
            self.device.get("device_model"),
        )

        # keepalive/reconnect supervisor
        if self._keepalive_task is None:
            self._keepalive_task = asyncio.create_task(self._keepalive())

    async def stop(self):
        if self._keepalive_task:
            self._keepalive_task.cancel()
            self._keepalive_task = None
        await self._shutdown_send_worker()
        if self.client:
            try: await self.client.disconnect()
            except: pass
        self.started = False

    async def send_code(self):
        await self._ensure_client()
        await asyncio.sleep(_rand_delay(LOGIN_DELAY_SECONDS))
        self._last_code_delivery = None
        try:
            result = await self.client.send_code_request(self.phone)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise
        except PhoneCodeFloodError as e:
            wait = getattr(e, "seconds", getattr(e, "value", 60))
            log.warning("[%s] phone code flood wait %ss", self.phone, wait)
            await asyncio.sleep(wait + 5)
            raise
        except FloodWaitError as e:
            wait = getattr(e, "seconds", getattr(e, "value", 60))
            log.warning("[%s] flood wait %ss on send_code", self.phone, wait)
            await asyncio.sleep(wait + 5)
            raise
        if isinstance(result, types.auth.SentCode):
            code_type = getattr(result, "type", None)
            self._last_code_delivery = _describe_sent_code_type(code_type)
            if isinstance(code_type, types.auth.SentCodeTypeApp):
                try:
                    forced = await self.client.send_code_request(self.phone, force_sms=True)
                    log.info(
                        "[%s] initial login code sent via app; retrying with SMS delivery",
                        self.phone,
                    )
                except PhoneCodeFloodError as e:
                    wait = getattr(e, "seconds", getattr(e, "value", 60))
                    log.warning("[%s] phone code flood wait %ss", self.phone, wait)
                    await asyncio.sleep(wait + 5)
                    raise
                except FloodWaitError as e:
                    wait = getattr(e, "seconds", getattr(e, "value", 60))
                    log.warning("[%s] flood wait %ss on send_code", self.phone, wait)
                    await asyncio.sleep(wait + 5)
                    raise
                except Exception as force_err:
                    log.warning(
                        "[%s] unable to force SMS code delivery: %s",
                        self.phone,
                        force_err,
                    )
                else:
                    result = forced
                    forced_type = getattr(forced, "type", None)
                    self._last_code_delivery = (
                        "sms_forced"
                        if isinstance(forced_type, types.auth.SentCodeTypeSms)
                        else _describe_sent_code_type(forced_type)
                    )
        return result

    @property
    def code_delivery_hint(self) -> Optional[str]:
        return self._last_code_delivery

    async def sign_in_code(self, code: str):
        await asyncio.sleep(_rand_delay(LOGIN_DELAY_SECONDS))
        try:
            await self.client.sign_in(self.phone, code)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise
        except FloodWaitError as e:
            wait = getattr(e, "seconds", getattr(e, "value", 60))
            log.warning("[%s] flood wait %ss on sign_in", self.phone, wait)
            await asyncio.sleep(wait + 5)
            raise
        with open(self.session_file, "w", encoding="utf-8") as f:
            f.write(self.client.session.save())
        self._set_session_invalid_flag(False)
        self._set_account_state(None)

    async def sign_in_2fa(self, password: str):
        await asyncio.sleep(_rand_delay(LOGIN_DELAY_SECONDS))
        try:
            await self.client.sign_in(password=password)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise
        except FloodWaitError as e:
            wait = getattr(e, "seconds", getattr(e, "value", 60))
            log.warning("[%s] flood wait %ss on 2FA", self.phone, wait)
            await asyncio.sleep(wait + 5)
            raise
        with open(self.session_file, "w", encoding="utf-8") as f:
            f.write(self.client.session.save())
        self._set_session_invalid_flag(False)
        self._set_account_state(None)

    async def _reconnect(self):
        # Разорвать соединение и создать новое — получим новый IP от динамического прокси
        try:
            if self.client:
                await self.client.disconnect()
        except Exception:
            pass
        self._enable_proxy_for_session()
        self._select_proxy(force_new=True)
        self.client = self._make_client()
        await self.start()

    async def validate(self) -> bool:
        try:
            client = await self._ensure_client()
            if not await client.is_user_authorized():
                return False
            await client.get_me()
            self._set_account_state(None)
            return True
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            return False
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            return False
        except Exception as e:
            log.warning("[%s] validation error: %s", self.phone, e)
            return False

    async def logout(self):
        try:
            client = await self._ensure_client()
            if await client.is_user_authorized():
                await client.log_out()
        except Exception as e:
            log.warning("[%s] logout error: %s", self.phone, e)
        finally:
            await self.stop()

    async def send_outgoing(
        self,
        chat_id: int,
        message: str,
        peer: Optional[Any] = None,
        reply_to_msg_id: Optional[int] = None,
        mark_read_msg_id: Optional[int] = None,
    ):
        self._ensure_send_worker()
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        await self._send_queue.put(
            _SendQueueItem(
                future=future,
                chat_id=chat_id,
                message=message,
                peer=peer,
                reply_to_msg_id=reply_to_msg_id,
                mark_read_msg_id=mark_read_msg_id,
            )
        )
        return await future

    async def send_voice(
        self,
        chat_id: int,
        file_path: str,
        peer: Optional[Any] = None,
        reply_to_msg_id: Optional[int] = None,
        mark_read_msg_id: Optional[int] = None,
    ):
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        await self._simulate_voice_recording(client, peer, file_path)
        try:
            sent = await client.send_file(
                peer,
                file_path,
                voice_note=True,
                reply_to=reply_to_msg_id,
            )
            if mark_read_msg_id is not None:
                with contextlib.suppress(Exception):
                    await client.send_read_acknowledge(peer, max_id=mark_read_msg_id)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")
        return sent
        
    async def send_video_note(
        self,
        chat_id: int,
        file_path: str,
        peer: Optional[Any] = None,
        reply_to_msg_id: Optional[int] = None,
        mark_read_msg_id: Optional[int] = None,
    ):
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        await self._simulate_round_recording(client, peer, file_path)
        try:
            sent = await client.send_file(
                peer,
                file_path,
                video_note=True,
                reply_to=reply_to_msg_id,
            )
            if mark_read_msg_id is not None:
                with contextlib.suppress(Exception):
                    await client.send_read_acknowledge(peer, max_id=mark_read_msg_id)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")
        return sent

    async def send_sticker(
        self,
        chat_id: int,
        file_path: str,
        peer: Optional[Any] = None,
        reply_to_msg_id: Optional[int] = None,
        mark_read_msg_id: Optional[int] = None,
    ):
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        try:
            sent = await client.send_file(
                peer,
                file_path,
                reply_to=reply_to_msg_id,
                supports_streaming=False,
            )
            if mark_read_msg_id is not None:
                with contextlib.suppress(Exception):
                    await client.send_read_acknowledge(peer, max_id=mark_read_msg_id)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")
        return sent

    async def send_media(
        self,
        chat_id: int,
        file_path: str,
        peer: Optional[Any] = None,
        reply_to_msg_id: Optional[int] = None,
        mark_read_msg_id: Optional[int] = None,
    ):
        import os
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id

        # Пытаемся загрузить метаданные о типе медиа
        media_type = _load_media_metadata(file_path)
        
        # Если метаданных нет, определяем тип по расширению (для обратной совместимости)
        if not media_type:
            _, ext = os.path.splitext(file_path.lower())
            if ext in {".jpg", ".jpeg", ".png"}:
                media_type = "photo"
            elif ext in {".mp4", ".mov", ".webm"}:
                media_type = "video"  # По умолчанию обычное видео, если нет метаданных
            else:
                media_type = "video_note"  # Для неизвестных расширений предполагаем кружок

        try:
            if media_type == "photo":
                # Отправляем как фото с анимацией загрузки
                await self._simulate_photo_upload(client, peer, file_path)
                sent = await client.send_file(
                    peer,
                    file_path,
                    reply_to=reply_to_msg_id,
                )
            elif media_type == "video_note":
                # Отправляем как кружок (video note) с анимацией записи
                await self._simulate_round_recording(client, peer, file_path)
                sent = await client.send_file(
                    peer,
                    file_path,
                    video_note=True,
                    reply_to=reply_to_msg_id,
                )
            else:  # media_type == "video" или по умолчанию
                # Отправляем как обычное видео с анимацией загрузки
                await self._simulate_video_upload(client, peer, file_path)
                sent = await client.send_file(
                    peer,
                    file_path,
                    reply_to=reply_to_msg_id,
                    supports_streaming=True,
                )

            if mark_read_msg_id is not None:
                with contextlib.suppress(Exception):
                    await client.send_read_acknowledge(peer, max_id=mark_read_msg_id)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")
        return sent

    async def edit_message(
        self,
        chat_id: int,
        msg_id: int,
        new_text: str,
        peer: Optional[Any] = None,
    ):
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        try:
            await client.edit_message(peer, msg_id, new_text)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")

    async def delete_message(
        self,
        chat_id: int,
        msg_id: int,
        peer: Optional[Any] = None,
    ):
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        try:
            await client.delete_messages(peer, [msg_id], revoke=True)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")

    async def mark_dialog_read(
        self,
        chat_id: int,
        peer: Optional[Any] = None,
        msg_id: Optional[int] = None,
    ) -> None:
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        try:
            await client.send_read_acknowledge(peer, max_id=msg_id)
        except Exception as e:
            log.debug("[%s] failed to mark dialog read: %s", self.phone, e)

    async def send_reaction(
        self,
        chat_id: int,
        reaction: str,
        peer: Optional[Any] = None,
        msg_id: Optional[int] = None,
    ) -> None:
        if msg_id is None:
            raise RuntimeError("Неизвестно, к какому сообщению добавить реакцию")
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        try:
            await client(
                functions.messages.SendReactionRequest(
                    peer=peer,
                    msg_id=msg_id,
                    reaction=[ReactionEmoji(reaction)],
                    add_to_recent=True,
                )
            )
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")
    
    async def block_contact(
        self,
        chat_id: int,
        peer: Optional[Any] = None,
    ) -> None:
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        try:
            input_peer = await client.get_input_entity(peer)
        except Exception:
            input_peer = peer
        try:
            await client(functions.contacts.BlockRequest(id=input_peer))
        except FloodWaitError as e:
            wait = getattr(e, "seconds", getattr(e, "value", 0))
            raise RuntimeError(f"Flood wait {wait}s при блокировке") from e
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram") from e
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram") from e
        except Exception as e:
            log.warning("[%s] не удалось заблокировать контакт %s: %s", self.phone, chat_id, e)
        try:
            await client(
                functions.messages.DeleteHistoryRequest(
                    peer=input_peer,
                    max_id=0,
                    just_clear=False,
                    revoke=True,
                )
            )
        except FloodWaitError as e:
            wait = getattr(e, "seconds", getattr(e, "value", 0))
            raise RuntimeError(f"Flood wait {wait}s при удалении диалога") from e
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram") from e
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram") from e
        except (PeerIdInvalidError, ValueError, TypeError) as e:
            log.warning(
                "[%s] не удалось удалить диалог %s из-за некорректного peer: %s",
                self.phone,
                chat_id,
                e,
            )
        except Exception as e:
            raise RuntimeError(f"Не удалось удалить диалог: {e}") from e
        with contextlib.suppress(Exception):
            await client.delete_dialog(input_peer)

    async def _keepalive(self):
        """Поддержание соединения: по ошибкам — reconnect; по таймеру (если включён) — тоже."""
        while True:
            try:
                await self.client.get_me()
            except AuthKeyDuplicatedError as e:
                await self._handle_authkey_duplication(e)
                return
            except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
                await self._handle_account_disabled("banned", e)
                return
            except UserDeactivatedError as e:
                await self._handle_account_disabled("frozen", e)
                return
            except FloodWaitError as e:
                wait = getattr(e, "seconds", getattr(e, "value", 60))
                log.warning("[%s] flood wait %ss on keepalive", self.phone, wait)
                await asyncio.sleep(wait + 5)
                continue
            except Exception as e:
                log.warning("[%s] connection issue -> reconnect: %s", self.phone, e)
                try:
                    await self._reconnect()
                except Exception as ex:
                    log.error("[%s] reconnect failed: %s", self.phone, ex)
            # периодический reconnect по таймеру (если включён)
            if AUTO_RECONNECT_MINUTES and AUTO_RECONNECT_MINUTES > 0:
                await asyncio.sleep(AUTO_RECONNECT_MINUTES * 60)
                try:
                    await self._reconnect()
                except Exception as ex:
                    log.error("[%s] scheduled reconnect failed: %s", self.phone, ex)
            else:
                interval = KEEPALIVE_INTERVAL_SECONDS + _rand_delay(KEEPALIVE_JITTER)
                await asyncio.sleep(interval)

# ---- runtime ----
pending: Dict[int, Dict[str, Any]] = {}
WORKERS: Dict[int, Dict[str, AccountWorker]] = {}
reply_contexts: Dict[str, Dict[str, Any]] = {}
reply_waiting: Dict[int, Dict[str, Any]] = {}
edit_waiting: Dict[int, Dict[str, Any]] = {}
outgoing_actions: Dict[str, Dict[str, Any]] = {}

# ---- AI автоответы (шаблоны + GPT-подсказки) ----

@dataclass
class PendingAIReply:
    owner_id: int
    phone: str
    peer_id: int
    msg_id: int
    incoming_text: str
    suggested_variants: List[str]
    # -1 = админ ещё не выбрал вариант
    chosen_index: int = -1
    recommended_index: Optional[int] = None
    recommendation_text: Optional[str] = None
    reply_to_source: bool = True


pending_ai_replies: Dict[str, PendingAIReply] = {}
# admin_id -> task_id
editing_ai_reply: Dict[int, str] = {}


def _format_ai_variants_for_admin(task_id: str, pr: PendingAIReply):
    """
    Текст + кнопки с вариантами ответа ИИ.
    """
    variants = pr.suggested_variants or []
    lines = [
        "🧠 Новое входящее сообщение",
        f"Аккаунт: {pr.phone}",
        f"Чат ID: {pr.peer_id}",
        "",
        "💬 Сообщение пользователя:",
        pr.incoming_text,
        "",
        "🤖 Варианты ответа:",
    ]

    for i, v in enumerate(variants, start=1):
        suffix = ""
        if pr.recommended_index is not None and pr.recommended_index == i - 1:
            suffix = "  ⭐️"
        lines.append(f"{i}) {v}{suffix}")
        lines.append("")

    if pr.recommendation_text:
        lines.extend(
            [
                "🤖 Рекомендация ИИ:",
                pr.recommendation_text,
                "",
            ]
        )
    elif pr.recommended_index is not None:
        lines.extend(
            [
                "🤖 Рекомендация ИИ:",
                f"Рекомендован вариант №{pr.recommended_index + 1}.",
                "",
            ]
        )

    lines.append(
        "Выбери один из вариантов кнопками ниже.\n"
        "После выбора можно будет при необходимости отредактировать текст перед отправкой."
    )
    mode_line = (
        "📩 Текущий режим отправки: ответ с реплаем на сообщение собеседника."
        if pr.reply_to_source
        else "📩 Текущий режим отправки: обычное сообщение без реплая."
    )
    lines.extend(
        [
            "",
            mode_line,
            "Нажми кнопку «Режим отправки» ниже, чтобы переключить поведение.",
        ]
    )
    text = "\n".join(lines).strip()

    digit_emoji = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣"]
    buttons: List[List[Button]] = []
    for idx, _ in enumerate(variants):
        emoji = digit_emoji[idx] if idx < len(digit_emoji) else f"{idx+1}."
        buttons.append(
            [Button.inline(f"{emoji} Вариант {idx+1}", f"ai_pick:{task_id}:{idx}")]
        )

    mode_label = (
        "🔁 Режим отправки: с реплаем"
        if pr.reply_to_source
        else "🔁 Режим отправки: без реплая"
    )
    buttons.append([Button.inline(mode_label, f"ai_toggle_reply:{task_id}")])
    buttons.append([Button.inline("❌ Отменить", f"ai_cancel:{task_id}")])
    return text, buttons


def _format_ai_chosen_for_admin(task_id: str, pr: PendingAIReply):
    """
    Текст + кнопки после выбора конкретного варианта.
    """
    variants = pr.suggested_variants or []
    idx = pr.chosen_index
    if not variants or idx < 0 or idx >= len(variants):
        chosen_text = ""
        num = 0
    else:
        chosen_text = variants[idx]
        num = idx + 1

    lines = [
        "🧠 Новое входящее сообщение",
        f"Аккаунт: {pr.phone}",
        f"Чат ID: {pr.peer_id}",
        "",
        "💬 Сообщение пользователя:",
        pr.incoming_text,
        "",
    ]

    if pr.recommendation_text:
        lines.extend(
            [
                "🤖 Рекомендация ИИ:",
                pr.recommendation_text,
                "",
            ]
        )
    elif pr.recommended_index is not None:
        lines.extend(
            [
                "🤖 Рекомендация ИИ:",
                f"Рекомендован вариант №{pr.recommended_index + 1}.",
                "",
            ]
        )

    if num > 0:
        lines.extend(
            [
                f"✅ Выбран вариант №{num}:",
                chosen_text,
                "",
                "Можешь отправить его как есть или нажать «Изменить текст» и прислать свою версию.",
            ]
        )
    else:
        lines.append("🤖 Вариант не выбран.")

    mode_line = (
        "📩 Текущий режим отправки: ответ с реплаем на сообщение собеседника."
        if pr.reply_to_source
        else "📩 Текущий режим отправки: обычное сообщение без реплая."
    )
    lines.extend(
        [
            "",
            mode_line,
            "Используй кнопку «Режим отправки» ниже, чтобы переключить поведение.",
        ]
    )

    text = "\n".join(lines).strip()

    mode_label = (
        "🔁 Режим отправки: с реплаем"
        if pr.reply_to_source
        else "🔁 Режим отправки: без реплая"
    )

    buttons: List[List[Button]] = [
        [Button.inline("📤 Отправить", f"ai_send_final:{task_id}")],
        [Button.inline("✏️ Изменить текст", f"ai_edit_final:{task_id}")],
        [
            Button.inline("🔁 Выбрать другой", f"ai_repick:{task_id}"),
            Button.inline("❌ Отменить", f"ai_cancel:{task_id}"),
        ],
        [Button.inline(mode_label, f"ai_toggle_reply:{task_id}")],
    ]
    return text, buttons


async def handle_ai_autoreply(worker: "AccountWorker", ev, peer) -> None:
    # Не отвечаем на исходящие и не-личные чаты
    try:
        if getattr(ev, "out", False):
            return
        if not getattr(ev, "is_private", False):
            return
    except Exception:
        return

    user_text = (getattr(ev, "raw_text", None) or "").strip()
    if not user_text:
        return

    # Подготовка профиля и истории для промпта
    account_meta = get_account_meta(worker.owner_id, worker.phone) or {}
    profile_description: Optional[str] = None
    for key in (
        "profile_description",
        "profile_text",
        "bio",
        "about",
        "full_name",
    ):
        value = account_meta.get(key)
        if isinstance(value, str) and value.strip():
            profile_description = value.strip()
            break

    history_lines: List[str] = []
    history_texts: List[str] = []
    client = getattr(worker, "client", None)
    if client is not None:
        try:
            history_messages = await client.get_messages(
                peer or ev.chat_id, limit=MAX_HISTORY_MESSAGES
            )
        except Exception as history_err:
            log.debug(
                "[%s] не удалось загрузить историю для AI-промпта: %s",
                worker.phone,
                history_err,
            )
        else:
            for message in reversed(history_messages):
                if getattr(message, "id", None) == getattr(ev, "id", None):
                    continue
                raw = (message.raw_text or "").strip()
                if not raw:
                    continue
                label = "Я:" if getattr(message, "out", False) else "Он:"
                history_lines.append(f"{label} {raw}")
                history_texts.append(raw)

    # 2) GPT — генерим несколько вариантов и отправляем админу на выбор
    # Получаем API ключ из переменной окружения
    api_key = os.getenv("OPENAI_API_KEY")
    try:
        variants = await generate_dating_ai_variants(
            user_text,
            history_lines=history_lines,
            history_texts=[*history_texts, user_text],
            profile=profile_description,
            api_key=api_key,
            model="gpt-4o",
            temperature=0.7,
            n=3,
        )
    except Exception as e:
        log.warning("[%s] ошибка GPT-подсказки: %s", worker.phone, e)
        return

    # Чистим, удаляем пустые и дубликаты
    cleaned: List[str] = []
    for v in variants:
        v = (v or "").strip()
        if not v:
            continue
        if v in cleaned:
            continue
        cleaned.append(v)

    if not cleaned:
        return

    # Если вдруг меньше 3, дублируем последние, чтобы всегда было 3 кнопки
    while len(cleaned) < 3:
        cleaned.append(cleaned[-1])

    recommended_index: Optional[int] = None
    recommendation_text: Optional[str] = None

    try:
        rec_idx, rec_text = await recommend_dating_ai_variant(
            incoming_text=user_text,
            variants=cleaned,
            history_lines=history_lines,
            profile=profile_description,
            api_key=api_key,
            model="gpt-4o",
            temperature=0.4,
        )
    except Exception as rec_err:
        log.debug(
            "[%s] не удалось получить рекомендацию варианта: %s",
            worker.phone,
            rec_err,
        )
    else:
        recommended_index = rec_idx
        recommendation_text = rec_text

    task_id = f"{worker.owner_id}:{worker.phone}:{ev.chat_id}:{ev.id}"
    pr = PendingAIReply(
        owner_id=worker.owner_id,
        phone=worker.phone,
        peer_id=ev.chat_id,
        msg_id=ev.id,
        incoming_text=user_text,
        suggested_variants=cleaned,
        recommended_index=recommended_index,
        recommendation_text=recommendation_text,
    )
    pending_ai_replies[task_id] = pr

    text_for_admin, buttons = _format_ai_variants_for_admin(task_id, pr)

    try:
        await safe_send_admin(
            text_for_admin,
            owner_id=worker.owner_id,
            buttons=buttons,
        )
    except Exception as send_err:
        log.warning(
            "[%s] не удалось отправить AI-подсказку админу: %s",
            worker.phone,
            send_err,
        )


def _extract_message_id(sent: Any) -> Optional[int]:
    if sent is None:
        return None
    if isinstance(sent, Message):
        return sent.id
    if isinstance(sent, (list, tuple, set)):
        for item in sent:
            msg_id = _extract_message_id(item)
            if msg_id is not None:
                return msg_id
    msg_id = getattr(sent, "id", None)
    if isinstance(msg_id, int):
        return msg_id
    return None


def register_outgoing_action(
    admin_id: int,
    *,
    phone: str,
    chat_id: int,
    peer: Optional[Any],
    msg_id: Optional[int],
    message_type: str,
) -> Optional[str]:
    if msg_id is None:
        return None
    token = secrets.token_urlsafe(8)
    outgoing_actions[token] = {
        "admin_id": admin_id,
        "phone": phone,
        "chat_id": chat_id,
        "peer": peer,
        "msg_id": msg_id,
        "type": message_type,
    }
    return token


def build_outgoing_control_buttons(token: str, *, allow_edit: bool) -> List[List[Button]]:
    buttons: List[Button] = []
    if allow_edit:
        buttons.append(Button.inline("✏️ Исправить", f"out_edit:{token}".encode()))
    buttons.append(Button.inline("🗑 Стереть", f"out_delete:{token}".encode()))
    return [buttons]


async def apply_proxy_config_to_owner(owner_id: int, *, restart_active: bool = True) -> Tuple[int, List[str]]:
    owner_workers = WORKERS.get(owner_id, {})
    restarted = 0
    errors: List[str] = []

    for phone, worker in owner_workers.items():
        try:
            await worker.refresh_proxy(restart=restart_active)
            restarted += 1
        except Exception as exc:
            err_text = str(exc)
            errors.append(f"{phone}: {err_text}")
            log.warning("[%s] не удалось обновить прокси: %s", phone, exc)

    accounts = get_accounts_meta(owner_id)
    changed = False
    for phone, meta in accounts.items():
        if phone in owner_workers:
            continue
        meta_changed, resolution = recompute_account_proxy_meta(owner_id, phone, meta)
        if meta_changed:
            changed = True
        for code, detail in resolution.get("warnings", []):
            if code == "invalid_type":
                log.warning(
                    "[%s] proxy_override must be a mapping, got %s. Игнорирую переопределение.",
                    phone,
                    detail or "unknown",
                )
            elif code == "override_invalid":
                log.warning(
                    "[%s] proxy_override указано, но конфигурация некорректна. Пытаюсь использовать пользовательский или динамический прокси.",
                    phone,
                )
            elif code == "tenant_invalid":
                log.warning(
                    "[%s] пользовательский прокси для пользователя настроен, но параметры некорректны. Подключение пойдёт без него или с глобальным прокси.",
                    phone,
                )

    if changed:
        persist_tenants()

    return restarted, errors


def _clone_buttons(buttons: Optional[List[List[Button]]]) -> Optional[List[List[Button]]]:
    if buttons is None:
        return None
    return [list(row) for row in buttons]


def _clean_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in kwargs.items() if v is not None}


def _ensure_back_button(
    buttons: Optional[List[List[Button]]],
    session_id: str,
) -> List[List[Button]]:
    rows = _clone_buttons(buttons) or []
    payload = f"ui_back:{session_id}".encode()
    if any(getattr(btn, "data", None) == payload for row in rows for btn in row):
        return rows
    rows.append([Button.inline("⬅️ Назад", payload)])
    return rows


InteractiveViewState = Dict[str, Any]
interactive_views: Dict[int, Dict[str, Any]] = {}


async def show_interactive_message(
    admin_id: int,
    text: str,
    *,
    buttons: Optional[List[List[Button]]] = None,
    replace: bool = False,
    **kwargs: Any,
) -> None:
    kwargs_clean = _clean_kwargs(kwargs)
    current = interactive_views.get(admin_id)
    if replace and (not current or not current.get("message_id")):
        replace = False

    if replace and current:
        session_id = current["session_id"]
        history: List[InteractiveViewState] = current.setdefault("states", [])
        rows = _ensure_back_button(buttons, session_id)
        try:
            await bot_client.edit_message(
                admin_id,
                current["message_id"],
                text,
                buttons=rows,
                **kwargs_clean,
            )
        except Exception as e:
            log.warning("Failed to edit interactive message for %s: %s", admin_id, e)
            msg = await bot_client.send_message(
                admin_id,
                text,
                buttons=rows,
                **kwargs_clean,
            )
            history.append(
                {
                    "text": text,
                    "buttons": _clone_buttons(rows),
                    "kwargs": dict(kwargs_clean),
                }
            )
            interactive_views[admin_id] = {
                "session_id": session_id,
                "message_id": msg.id,
                "states": history,
            }
        else:
            history.append(
                {
                    "text": text,
                    "buttons": _clone_buttons(rows),
                    "kwargs": dict(kwargs_clean),
                }
            )
        return

    session_id = secrets.token_hex(4)
    rows = _clone_buttons(buttons)
    msg = await bot_client.send_message(
        admin_id,
        text,
        buttons=rows,
        **kwargs_clean,
    )
    interactive_views[admin_id] = {
        "session_id": session_id,
        "message_id": msg.id,
        "states": [
            {
                "text": text,
                "buttons": _clone_buttons(rows),
                "kwargs": dict(kwargs_clean),
            }
        ],
    }


async def interactive_go_back(admin_id: int, session_id: str) -> Tuple[bool, Optional[str]]:
    state = interactive_views.get(admin_id)
    if not state or state.get("session_id") != session_id:
        return False, "expired"
    history: List[InteractiveViewState] = state.get("states", [])
    if len(history) <= 1:
        return False, "root"
    history.pop()
    prev = history[-1]
    prev_buttons = _clone_buttons(prev.get("buttons"))
    prev_kwargs = dict(prev.get("kwargs", {}))
    try:
        await bot_client.edit_message(
            admin_id,
            state["message_id"],
            prev["text"],
            buttons=prev_buttons,
            **prev_kwargs,
        )
    except Exception as e:
        log.warning("Failed to restore interactive view for %s: %s", admin_id, e)
        return False, "error"
    return True, None


async def clear_interactive_message(admin_id: int) -> None:
    state = interactive_views.pop(admin_id, None)
    if not state:
        return
    message_id = state.get("message_id")
    if not message_id:
        return
    try:
        await bot_client.delete_messages(admin_id, message_id)
    except Exception as exc:
        log.debug("Не удалось удалить интерактивное сообщение для %s: %s", admin_id, exc)


def _schedule_message_deletion(chat_id: int, message_id: int, delay: float) -> None:
    async def _cleanup() -> None:
        try:
            await asyncio.sleep(delay)
            await bot_client.delete_messages(chat_id, message_id)
        except Exception as exc:
            log.debug("Не удалось удалить временное сообщение для %s: %s", chat_id, exc)

    asyncio.create_task(_cleanup())


async def send_temporary_message(chat_id: int, text: str, *, delay: float = 5.0) -> None:
    msg = await bot_client.send_message(chat_id, text)
    _schedule_message_deletion(chat_id, msg.id, delay)


menu_button_reset: Set[int] = set()
main_menu_messages: Dict[int, int] = {}

ADD_ACCOUNT_PROMPT = (
    "Добавляем новый аккаунт."
    "\n• Пришли параметры прокси в формате SOCKS5://host:port, host:port или host:port:логин:пароль"
    "\n• Или просто отправь номер телефона в формате +7XXXXXXXXXX"
    "\nНапиши 'без прокси' для прямого подключения или 'отмена' чтобы выйти."
)

ACCOUNT_PROXY_MANUAL_PROMPT = (
    "Пришли параметры прокси в формате\n"
    "SOCKS5://host:port, host:port или host:port:логин:пароль.\n"
    "Можно указать HTTP:// или SOCKS4://.\n"
    "Напиши 'без прокси' для прямого подключения или 'отмена' для отмены."
)

ACCOUNT_PHONE_PROMPT = (
    "Подключение будет без прокси. Пришли номер телефона (+7XXXXXXXXXX)"
)


def _init_account_add_manual(admin_id: int) -> str:
    pending[admin_id] = {"flow": "account", "step": "proxy_manual"}
    pending[admin_id].pop("proxy_config", None)
    return ACCOUNT_PROXY_MANUAL_PROMPT


def _init_account_add_direct(admin_id: int) -> str:
    pending[admin_id] = {
        "flow": "account",
        "step": "phone",
        "proxy_config": {"enabled": False},
    }
    return ACCOUNT_PHONE_PROMPT


async def _send_account_add_prompt(admin_id: int, prompt_text: Optional[str]) -> None:
    if not prompt_text:
        return
    try:
        await bot_client.send_message(admin_id, prompt_text)
    except Exception as send_error:
        log.debug(
            "[%s] не удалось отправить подсказку для добавления аккаунта: %s",
            admin_id,
            send_error,
        )

PHONE_CLEANUP_RE = re.compile(r"[\s()\-]")


def extract_phone_number(text: str) -> Optional[str]:
    """Try to normalize the provided text into a phone number."""

    cleaned = PHONE_CLEANUP_RE.sub("", text.strip())
    if not cleaned:
        return None
    if cleaned.startswith("+"):
        digits = "+" + "".join(ch for ch in cleaned[1:] if ch.isdigit())
    else:
        digits_only = "".join(ch for ch in cleaned if ch.isdigit())
        if not digits_only:
            return None
        if digits_only.startswith("8") and len(digits_only) == 11:
            digits_only = "7" + digits_only[1:]
        digits = "+" + digits_only
    if len(digits) < 8 or not digits[1:].isdigit():
        return None
    return digits

BOT_COMMANDS: List[types.BotCommand] = [
    types.BotCommand(command="start", description="Открыть главное меню"),
    types.BotCommand(command="add", description="Добавить аккаунт"),
    types.BotCommand(command="accounts", description="Список аккаунтов и статусы"),
    types.BotCommand(
        command="files_add",
        description="Добавить файл или шаблон в библиотеку",
    ),
    types.BotCommand(
        command="files_delete",
        description="Удалить файл или шаблон из библиотеки",
    ),
    types.BotCommand(
        command="grant",
        description="Выдать доступ пользователю (для супер-админов)",
    ),
]


async def edit_or_send_message(
    event: events.CallbackQuery.Event, admin_id: int, text: str, *, buttons=None, **kwargs
) -> bool:
    """Try to update the triggering message, falling back to sending a new one."""

    try:
        await event.edit(text, buttons=buttons, **kwargs)
        return True
    except Exception as exc:
        log.debug("Не удалось обновить сообщение для %s: %s", admin_id, exc)

    await bot_client.send_message(admin_id, text, buttons=buttons, **kwargs)
    return False


def get_worker(owner_id: int, phone: str) -> Optional[AccountWorker]:
    return WORKERS.get(owner_id, {}).get(phone)


def register_worker(owner_id: int, phone: str, worker: AccountWorker) -> None:
    WORKERS.setdefault(owner_id, {})[phone] = worker


def unregister_worker(owner_id: int, phone: str) -> None:
    owner_workers = WORKERS.get(owner_id)
    if not owner_workers:
        return
    owner_workers.pop(phone, None)
    if not owner_workers:
        WORKERS.pop(owner_id, None)


async def ensure_worker_running(owner_id: int, phone: str) -> Optional[AccountWorker]:
    worker = get_worker(owner_id, phone)
    if worker and worker.started:
        return worker
    if worker and not worker.started:
        try:
            await worker.start()
            if worker.started:
                return worker
        except AuthKeyDuplicatedError:
            log.warning("[%s] session invalid while restarting worker", phone)
            unregister_worker(owner_id, phone)
            return None
        except Exception as exc:
            log.warning("[%s] failed to restart worker: %s", phone, exc)
    meta = get_account_meta(owner_id, phone)
    if not meta:
        return None
    session_path = meta.get("session_file") or user_session_path(owner_id, phone)
    session_data: Optional[str] = None
    if session_path and os.path.exists(session_path):
        try:
            with open(session_path, "r", encoding="utf-8") as fh:
                session_data = fh.read().strip() or None
        except OSError as exc:
            log.warning("[%s] cannot read session file %s: %s", phone, session_path, exc)
    api_id = meta.get("api_id")
    api_hash: Optional[str] = None
    try:
        api_id = int(api_id)
    except (TypeError, ValueError):
        api_id = API_KEYS[0]["api_id"] if API_KEYS else 0
    for creds in API_KEYS:
        if creds.get("api_id") == api_id:
            api_hash = creds.get("api_hash")
            break
    if api_hash is None and API_KEYS:
        api_id = API_KEYS[0]["api_id"]
        api_hash = API_KEYS[0]["api_hash"]
    device_name = meta.get("device")
    device = next(
        (d for d in DEVICE_PROFILES if d.get("device_model") == device_name),
        DEVICE_PROFILES[0] if DEVICE_PROFILES else {},
    )
    if api_hash is None:
        log.warning("[%s] cannot restore worker: API hash not configured", phone)
        return worker
    worker = AccountWorker(owner_id, phone, api_id, api_hash, device, session_data)
    register_worker(owner_id, phone, worker)
    try:
        await worker.start()
    except AuthKeyDuplicatedError:
        log.warning("[%s] session invalid during worker restore", phone)
        unregister_worker(owner_id, phone)
        return None
    except Exception as exc:
        log.warning("[%s] failed to start worker on demand: %s", phone, exc)
        unregister_worker(owner_id, phone)
        return None
    return worker


def get_reply_context_for_admin(ctx_id: str, admin_id: int) -> Optional[Dict[str, Any]]:
    ctx = reply_contexts.get(ctx_id)
    if not ctx:
        return None
    if ctx.get("owner_id") != admin_id:
        return None
    return ctx

async def mark_dialog_read_for_context(ctx_info: Dict[str, Any]) -> None:
    worker = get_worker(ctx_info["owner_id"], ctx_info["phone"])
    if not worker:
        return
    try:
        await worker.mark_dialog_read(
            ctx_info["chat_id"],
            ctx_info.get("peer"),
            ctx_info.get("msg_id"),
        )
    except Exception as e:
        log.debug(
            "[%s] unable to mark dialog read for chat %s: %s",
            ctx_info.get("phone"),
            ctx_info.get("chat_id"),
            e,
        )

async def validate_all_accounts(admin_id: int) -> str:
    """Валидирует все аккаунты пользователя и возвращает отчёт."""
    accounts = get_accounts_meta(admin_id)
    if not accounts:
        return "Аккаунтов нет."

    results = []
    for phone in accounts:
        meta = accounts[phone]
        state = meta.get("state")
        worker = await ensure_worker_running(admin_id, phone)
        if not worker:
            if state == "banned":
                result_text = f"⛔️ {phone} заблокирован Telegram. Аккаунт отключён."
            elif state == "frozen":
                result_text = f"🧊 {phone} заморожен Telegram. Требуется разблокировка."
            else:
                result_text = f"⚠️ {phone} не активен."
        else:
            ok = await worker.validate()
            if ok:
                result_text = f"✅ {phone} активен и принимает сообщения."
            elif state == "banned":
                result_text = f"⛔️ {phone} заблокирован Telegram. Аккаунт отключён."
            elif state == "frozen":
                result_text = f"🧊 {phone} заморожен Telegram. Требуется разблокировка."
            else:
                result_text = f"❌ {phone} не отвечает. Проверь подключение."
        results.append(result_text)

    return "\n".join(results)

async def cancel_operations(admin_id: int, notify: bool = True) -> bool:
    """Сбрасывает незавершённые операции для конкретного админа."""
    cancelled = False
    if reply_waiting.pop(admin_id, None) is not None:
        cancelled = True
    if pending.pop(admin_id, None) is not None:
        cancelled = True
    if edit_waiting.pop(admin_id, None) is not None:
        cancelled = True
    if cancelled:
        await clear_interactive_message(admin_id)
        if notify:
            await send_temporary_message(admin_id, "❌ Текущая операция отменена.")
    return cancelled

async def ensure_menu_button_hidden(admin_id: int) -> None:
    if admin_id in menu_button_reset:
        return
    try:
        await bot_client(
            functions.bots.SetBotMenuButtonRequest(
                user_id=admin_id,
                button=types.BotMenuButtonDefault(),
            )
        )
    except Exception as exc:
        log.debug("Не удалось сбросить кнопку меню для %s: %s", admin_id, exc)
    else:
        menu_button_reset.add(admin_id)

def main_menu():
    return [
        [
            Button.switch_inline(
                "➕ Добавить аккаунт ↗", query="add account", same_peer=True
            )
        ],
        [Button.switch_inline("Список аккаунтов →", query="accounts_menu", same_peer=True)],
        [library_inline_button("", "📁 Файлы ↗")],
    ]


async def show_main_menu(admin_id: int, text: str = "Менеджер запущен. Выбери действие:") -> None:
    buttons = main_menu()
    message_id = main_menu_messages.get(admin_id)
    if message_id:
        try:
            await bot_client.edit_message(admin_id, message_id, text, buttons=buttons)
            return
        except Exception as exc:
            log.debug("Не удалось обновить главное меню для %s: %s", admin_id, exc)
            main_menu_messages.pop(admin_id, None)

    msg = await bot_client.send_message(admin_id, text, buttons=buttons)
    main_menu_messages[admin_id] = msg.id


def files_add_menu() -> List[List[Button]]:
    return [
        [
            Button.inline("📄 Пасты", b"files_paste"),
            Button.inline("🎙 Голосовые", b"files_voice"),
        ],
        [
            Button.inline("📹 Медиа", b"files_video"),
            Button.inline("💟 Стикеры", b"files_sticker"),
        ],
        [Button.inline("⬅️ Назад", b"back")],
    ]


def files_delete_menu() -> List[List[Button]]:
    return [
        [
            Button.inline("📄 Пасты", b"show_del_files:paste"),
            Button.inline("🎙 Голосовые", b"show_del_files:voice"),
        ],
        [
            Button.inline("📹 Медиа", b"show_del_files:video"),
            Button.inline("💟 Стикеры", b"show_del_files:sticker"),
        ],
        [Button.inline("⬅️ Назад", b"back")],
    ]



def _mask_secret(value: Optional[str]) -> str:
    if not value:
        return "нет"
    if len(value) <= 2:
        return "*" * len(value)
    return f"{value[0]}{'*' * (len(value) - 2)}{value[-1]}"


def format_proxy_settings(owner_id: int) -> str:
    cfg = get_tenant_proxy_config(owner_id)
    if not cfg or not cfg.get("host"):
        return (
            "Прокси не настроен.\n"
            "Нажми \"Добавить/Изменить\", чтобы указать данные провайдера перед добавлением аккаунтов."
        )
    lines = ["Текущие настройки прокси:"]
    proxy_type = str(cfg.get("type", "HTTP")).upper()
    lines.append(f"• Тип: {proxy_type}")
    lines.append(f"• Адрес: {cfg.get('host')}:{cfg.get('port')}")
    username = cfg.get("username")
    password = cfg.get("password")
    if username:
        lines.append(f"• Логин: {username}")
    if password:
        lines.append(f"• Пароль: {_mask_secret(password)}")
    if cfg.get("dynamic"):
        lines.append("• Режим: динамический (новый IP по запросу провайдера)")
    else:
        lines.append("• Режим: статический")
    updated_at = cfg.get("updated_at")
    if updated_at:
        try:
            ts = datetime.fromtimestamp(updated_at)
            lines.append(f"• Обновлено: {ts.strftime('%d.%m.%Y %H:%M:%S')}")
        except Exception:
            pass
    return "\n".join(lines)


def proxy_menu_buttons(owner_id: int) -> List[List[Button]]:
    cfg = get_tenant_proxy_config(owner_id)
    has_active = get_active_tenant_proxy(owner_id) is not None
    has_config = bool(cfg)
    has_overrides = owner_has_account_proxy_overrides(owner_id)
    rows: List[List[Button]] = []
    rows.append([Button.inline("➕ Добавить/Изменить", b"proxy_set")])
    if has_active:
        rows.append([Button.inline("🔄 Обновить IP", b"proxy_refresh")])
    if has_config:
        rows.append([Button.inline("🚫 Отключить", b"proxy_clear")])
    if has_overrides:
        rows.append([Button.inline("♻️ Сбросить прокси аккаунтов", b"proxy_reset_accounts")])
    rows.append([Button.inline("⬅️ Назад", b"back")])
    return rows

def account_control_menu():
    return [
        [
            Button.inline("✅ Валидация", b"val_select"),
            Button.inline("🗑 Удалить аккаунт", b"del_select")
        ],
        [Button.inline("⬅️ Назад", b"back")],
    ]


def build_account_buttons(owner_id: int, prefix: str, page: int = 0) -> Tuple[List[List[Button]], int, int, int]:
    phones = sorted(get_accounts_meta(owner_id).keys())
    page_items, current_page, total_pages, total_count = paginate_list(list(phones), page)
    rows: List[List[Button]] = []
    for phone in page_items:
        rows.append([Button.inline(phone, f"{prefix}:{phone}".encode())])
    if total_count > ITEMS_PER_PAGE:
        nav: List[Button] = []
        if current_page > 0:
            nav.append(Button.inline("◀️", f"acct_page:{prefix}:{current_page - 1}".encode()))
        nav.append(Button.inline(f"{current_page + 1}/{total_pages}", b"noop"))
        if current_page < total_pages - 1:
            nav.append(Button.inline("▶️", f"acct_page:{prefix}:{current_page + 1}".encode()))
        rows.append(nav)
    rows.append([Button.inline("⬅️ Назад", b"list")])
    return rows, current_page, total_pages, total_count


@bot_client.on(events.InlineQuery)
async def on_inline_query(ev):
    """Обработка inline-запросов.

    Используется для работы с файловой библиотекой и быстрого доступа к файлам:
    - файлы (главное меню Добавить/Удалить)
    - add_files, del_files (выбор типа файла)
    - add_paste, add_voice, add_video, add_sticker (добавление файлов)
    - del_paste, del_voice, del_video, del_sticker (удаление файлов)
    - library, lib, files (стандартный поиск по библиотеке)
    - library add/delete
    - library <type>
    - library delete <type>
    """
    user_id = _extract_event_user_id(ev)
    if user_id is None or not is_admin(user_id):
        await ev.answer(
            [],
            cache_time=0,
            switch_pm="🚫 Нет доступа",
            switch_pm_param="start",
        )
        return

    raw_query = (ev.text or "").strip()
    normalized_query = " ".join(raw_query.replace("_", " ").split()).strip().lower()

    # Обработка меню аккаунтов (ДОЛЖНА быть ДО проверки ключевых слов!)
    if raw_query == "accounts_menu":
        inline_results = [
            InlineArticle(
                id="validate_accounts",
                title="✅ Валидация",
                description="Проверить аккаунты на валидность",
                text="START_VALIDATE_ACCOUNTS",
            ),
            InlineArticle(
                id="delete_account",
                title="🗑 Удалить аккаунт",
                description="Удалить один из аккаунтов",
                text="START_DELETE_ACCOUNT",
            ),
        ]
        results = await _render_inline_articles(ev.builder, inline_results)
        await ev.answer(results, cache_time=0)
        return

    # Обработка списка аккаунтов для удаления
    if raw_query == "delete_account_list":
        accounts = get_accounts_meta(user_id)
        inline_results = []
        if not accounts:
            inline_results.append(
                InlineArticle(
                    id="no_accounts",
                    title="❌ Нет аккаунтов",
                    description="Сначала добавьте аккаунт",
                    text="Аккаунтов нет.",
                )
            )
        else:
            for phone in accounts:
                inline_results.append(
                    InlineArticle(
                        id=f"del_{phone}",
                        title=f"🗑 {phone}",
                        description="Нажмите для удаления",
                        text=f"DEL_ACCOUNT_{phone}",
                    )
                )
        results = await _render_inline_articles(ev.builder, inline_results)
        await ev.answer(results, cache_time=0)
        return

    # Проверка на запросы добавления аккаунта (содержащие ключевые слова)
    account_keywords = {"add", "аккаунт", "account", "добавить"}
    if any(keyword in normalized_query for keyword in account_keywords):
        results = await _render_inline_articles(
            ev.builder, _build_add_account_inline_results()
        )
        await ev.answer(results, cache_time=0)
        return
    
    reply_query = _parse_reply_inline_query(raw_query)
    if reply_query is not None:
        ctx_id, mode = reply_query
        if not ctx_id:
            results = [_reply_inline_help_article(mode, "Нажми кнопку в уведомлении ещё раз.")]
        else:
            results = _build_reply_inline_results(user_id, ctx_id, mode)
        rendered = await _render_inline_articles(ev.builder, results)
        await ev.answer(rendered, cache_time=0)
        return

    # Обработка специальных inline-запросов для красивой цепочки файлов
    # Главное меню: "файлы", "files"
    if normalized_query in ("файлы", "files"):
        results = await _render_inline_articles(
            ev.builder, _build_files_main_menu()
        )
        await ev.answer(results, cache_time=0)
        return

    # Меню добавления: показать 4 типа файлов
    if normalized_query == "files_add":
        file_types = [
            ("paste", "📄 Пасты", "Добавить текстовую пасту"),
            ("voice", "🎙 Голосовые", "Добавить голосовое сообщение"),
            ("video", "📹 Медиа", "Добавить медиа"),
            ("sticker", "💟 Стикеры", "Добавить стикер"),
        ]
        inline_results = []
        for file_type, title, desc in file_types:
            inline_results.append(
                InlineArticle(
                    id=f"add_type_{file_type}",
                    title=title,
                    description=desc,
                    text="Запускаю процесс добавления...",
                    buttons=[
                        [Button.switch_inline(
                            text=f"🚀 Начать добавление",
                            query=f"start_add_{file_type}",
                            same_peer=True
                        )]
                    ],
                )
            )
        results = await _render_inline_articles(ev.builder, inline_results)
        await ev.answer(results, cache_time=0)
        return


    # Запуск процесса добавления конкретного типа файла
    if normalized_query.startswith("start_add_"):
        file_type = normalized_query[10:]  # Убираем "start_add_"
        if file_type in FILE_TYPE_LABELS:
            label = FILE_TYPE_LABELS[file_type]
            # Плашка, которая при выборе запустит процесс
            inline_results = [
                InlineArticle(
                    id=f"trigger_add_{file_type}",
                    title=f"🚀 Начать добавление {label.lower()}",
                    description=f"Запустить процесс добавления {label.lower()}",
                    text=f"Запускаю процесс добавления {label.lower()}...",
                )
            ]
            results = await _render_inline_articles(ev.builder, inline_results)
            await ev.answer(results, cache_time=0)
            return



    # Обработка запросов выбора типа файла для добавления (add_files_paste и т.д.)
    if normalized_query.startswith("add_files_"):
        file_type = normalized_query[10:]  # Убираем "add_files_"
        if file_type in FILE_TYPE_LABELS:
            label = FILE_TYPE_LABELS[file_type]
            results = await _render_inline_articles(
                ev.builder,
                [
                    InlineArticle(
                        id=f"start_add_{file_type}",
                        title=f"➕ Добавить {label.lower()}",
                        description=f"Начать процесс добавления {label.lower()}",
                        text="🔹 Нажмите кнопку ниже для начала",
                        buttons=[
                            [Button.switch_pm(
                                text=f"🚀 Начать добавление",
                                start_parameter=f"add_{file_type}"
                            )]
                        ],
                    )
                ]
            )
            await ev.answer(results, cache_time=0)
            return
    
    # Обработка конкретных действий (add_paste, del_voice и т.д.)
    # Вызываются через switch_inline кнопки из главного меню
    if raw_query.startswith(("add_", "del_")):
        parts = raw_query.split("_", 1)
        if len(parts) == 2:
            action, file_type = parts
            if action in ("add", "del") and file_type in FILE_TYPE_LABELS:
                # Для добавления: показываем плашки с процессом добавления
                if action == "add":
                    results = await _render_inline_articles(
                        ev.builder, _build_add_file_results(user_id, file_type)
                    )
                    await ev.answer(results, cache_time=0)
                    return
                
                # Для удаления: показываем список файлов для удаления
                else:  # action == "del"
                    results = await _render_inline_articles(
                        ev.builder, _build_library_file_results(user_id, file_type, "", mode="delete")
                    )
                    await ev.answer(results, cache_time=0)
                    return

    parts = raw_query.split()
    # Сносим префикс library / files / file / lib
    if parts and parts[0].lower() in LIBRARY_INLINE_QUERY_PREFIXES:
        parts = parts[1:]

    # Режим (add/delete)
    mode: Optional[str] = None
    if parts and parts[0].lower() in {"add", "delete", "del", "remove"}:
        token = parts.pop(0).lower()
        mode = "add" if token == "add" else "delete"

    # Пустой запрос -> стартовый экран или выбор типа для add/delete
    if not parts:
        if mode in {"add", "delete"}:
            results = await _render_inline_articles(
                ev.builder, _build_inline_type_results(user_id, mode)
            )
        else:
            results = await _render_inline_articles(
                ev.builder, _build_library_overview_results(user_id)
            )
        await ev.answer(results, cache_time=0)
        return

    # Дальше первый параметр — это уже тип файла или "all/overview"
    raw_category = parts[0]
    category = raw_category.lower()
    remainder = " ".join(parts[1:]) if len(parts) > 1 else ""

    if category in FILE_TYPE_LABELS:
        # library paste / library add paste / library delete paste
        results = await _render_inline_articles(
            ev.builder,
            _build_library_file_results(user_id, category, remainder, mode=mode),
        )
    elif category in {"all", "overview"}:
        results = await _render_inline_articles(
            ev.builder,
            _build_library_overview_results(user_id),
        )
    else:
        if mode == "delete":
            search_tokens = [raw_category]
            if remainder:
                search_tokens.append(remainder)
            search_term = " ".join(search_tokens)
            results = await _render_inline_articles(
                ev.builder,
                _build_delete_search_results(user_id, search_term),
            )
        else:
            results = await _render_inline_articles(
                ev.builder,
                _build_library_unknown_results(category),
            )

    await ev.answer(results, cache_time=0)


async def _handle_reply_inline_send(update: types.UpdateBotInlineSend) -> None:
    admin_id = getattr(update, "user_id", None)
    if admin_id is None or not is_admin(admin_id):
        return

    result_id = getattr(update, "id", "") or ""
    
    # Обработка плашек запуска добавления файлов (trigger_add_paste и т.д.)
    if result_id.startswith("trigger_add_"):
        file_type = result_id[12:]  # Убираем "trigger_add_"
        if file_type in FILE_TYPE_LABELS:
            # Запускаем процесс добавления файла
            pending[admin_id] = {"flow": "file", "file_type": file_type, "step": "name"}
            prompt = FILE_TYPE_ADD_PROMPTS[file_type]
            try:
                await bot_client.send_message(admin_id, prompt)
            except Exception as e:
                logger.error(f"Failed to send file add prompt from inline: {e}")
        return
    
    
    # Обработка плашек добавления файлов (старый формат add_start:paste и т.д.)
    if result_id.startswith("add_start:"):
        file_type = result_id.split(":", 1)[1]
        if file_type in FILE_TYPE_LABELS:
            # Запускаем процесс добавления файла
            pending[admin_id] = {"flow": "file", "file_type": file_type, "step": "name"}
            prompt = FILE_TYPE_ADD_PROMPTS[file_type]
            try:
                await bot_client.send_message(admin_id, prompt)
            except Exception as e:
                logger.error(f"Failed to send file add prompt from inline: {e}")
        return
    
    # Обработка стандартных inline reply результатов
    if not result_id.startswith(INLINE_REPLY_RESULT_PREFIX):
        return

    token = result_id[len(INLINE_REPLY_RESULT_PREFIX) :]
    payload = _resolve_inline_reply_payload(token)
    if not payload:
        return
    if not _claim_inline_reply_token(token):
        return

    await _execute_inline_reply_payload(admin_id, payload)


@bot_client.on(events.Raw)
async def on_raw_update(ev):
    update = getattr(ev, "update", None)
    if isinstance(update, types.UpdateBotInlineSend):
        await _handle_reply_inline_send(update)


@bot_client.on(events.NewMessage(pattern=r"/start(?:\s+(.+))?"))
async def on_start(ev):
    """Обработчик команды /start с поддержкой payload для инлайн-цепочки файлов."""
    admin_id = _extract_event_user_id(ev)
    if admin_id is None or not is_admin(admin_id):
        await ev.respond("Доступ запрещён.")
        return
    
    # Извлекаем payload (параметр после /start)
    match = ev.pattern_match
    payload = match.group(1) if match and match.group(1) else None
    
    # Если нет payload - показываем главное меню
    if not payload:
        await cancel_operations(admin_id, notify=False)
        await show_main_menu(admin_id)
        await ensure_menu_button_hidden(admin_id)
        return
    
    # ============ Обработка инлайн-цепочки файлов ============

    # Обработка payload для меню удаления файлов
    if payload == "files_del":
        await ev.respond("Выбери тип файлов для удаления:", buttons=files_delete_menu())
        await ensure_menu_button_hidden(admin_id)
        return

    # Обработка payload для меню удаления файлов с пагинацией
    if payload.startswith("del_files_"):
        file_type = payload[10:]  # Убираем "del_files_"
        if file_type in FILE_TYPE_LABELS:
            files = list_templates_by_type(admin_id, file_type)
            if not files:
                label = FILE_TYPE_LABELS[file_type]
                await ev.respond(
                    f"{label} отсутствуют.",
                    buttons=files_delete_menu(),
                )
            else:
                buttons, current_page, total_pages, _ = build_file_delete_keyboard(
                    files, file_type, 0
                )
                caption = format_page_caption(
                    f"{FILE_TYPE_LABELS[file_type]} для удаления", current_page, total_pages
                )
                await ev.respond(caption, buttons=buttons)
            await ensure_menu_button_hidden(admin_id)
            return

    # Если payload не распознан - показываем главное меню
    await cancel_operations(admin_id, notify=False)
    await show_main_menu(admin_id)
    await ensure_menu_button_hidden(admin_id)

@bot_client.on(events.CallbackQuery)
async def on_cb(ev):
    admin_id = _extract_event_user_id(ev)
    if admin_id is None or not is_admin(admin_id):
        await answer_callback(ev, "Недоступно", alert=True); return
    data = ev.data.decode() if isinstance(ev.data, (bytes, bytearray)) else str(ev.data)

    notify_cancel = not data.startswith(("reply", "ui_back"))
    await cancel_operations(admin_id, notify=notify_cancel)
    await ensure_menu_button_hidden(admin_id)

    if data == "noop":
        await answer_callback(ev)
        return

        # ====== AI автоответы (выбор варианта / отправка / редактирование) ======
    if data.startswith("ai_pick:"):
        try:
            _, rest = data.split(":", 1)
            task_id, idx_str = rest.rsplit(":", 1)
        except ValueError:
            await answer_callback(ev, "Некорректные данные кнопки", alert=True)
            return

        pr = pending_ai_replies.get(task_id)
        if not pr:
            log.debug("AI pick callback for unknown/expired task_id: %s", task_id)
            await answer_callback(ev)  # Молча отвечаем, не показывая ошибку
            return

        try:
            idx = int(idx_str)
        except ValueError:
            await answer_callback(ev, "Некорректный номер варианта", alert=True)
            return

        if not pr.suggested_variants or idx < 0 or idx >= len(pr.suggested_variants):
            await answer_callback(ev, "Вариант недоступен", alert=True)
            return

        # Если вариант уже выбран и это тот же вариант, просто отвечаем
        if pr.chosen_index == idx:
            await answer_callback(ev)
            return

        pr.chosen_index = idx

        # Отвечаем на callback сразу
        await answer_callback(ev)

        text_for_admin, buttons = _format_ai_chosen_for_admin(task_id, pr)

        try:
            await ev.edit(text_for_admin, buttons=buttons)
        except Exception as e:
            log.debug("Не удалось обновить AI-подсказку: %s", e)
        return

    if data.startswith("ai_toggle_reply:"):
        try:
            _, task_id = data.split(":", 1)
        except ValueError:
            await answer_callback(ev, "Некорректные данные кнопки", alert=True)
            return

        pr = pending_ai_replies.get(task_id)
        if not pr:
            await answer_callback(ev, "Заявка уже обработана или устарела", alert=True)
            return

        pr.reply_to_source = not pr.reply_to_source

        if pr.chosen_index >= 0:
            text_for_admin, buttons = _format_ai_chosen_for_admin(task_id, pr)
        else:
            text_for_admin, buttons = _format_ai_variants_for_admin(task_id, pr)

        try:
            await ev.edit(text_for_admin, buttons=buttons)
        except Exception as e:
            log.debug("Не удалось переключить режим отправки: %s", e)
            await answer_callback(ev, "Сообщение устарело", alert=True)
            return

        await answer_callback(ev)
        return

    if data.startswith("ai_repick:"):
        try:
            _, task_id = data.split(":", 1)
        except ValueError:
            await answer_callback(ev, "Некорректные данные кнопки", alert=True)
            return

        pr = pending_ai_replies.get(task_id)
        if not pr:
            await answer_callback(ev, "Заявка уже обработана или устарела", alert=True)
            return

        text_for_admin, buttons = _format_ai_variants_for_admin(task_id, pr)
        try:
            await ev.edit(text_for_admin, buttons=buttons)
        except Exception as e:
            log.debug("Не удалось обновить AI-подсказку: %s", e)
            await answer_callback(ev, "Сообщение устарело", alert=True)
            return

        await answer_callback(ev)
        return

    if data.startswith(("ai_send:", "ai_cancel:", "ai_edit:")):
        try:
            action, task_id = data.split(":", 1)
        except ValueError:
            await answer_callback(ev, "Некорректные данные кнопки", alert=True)
            return

        pr = pending_ai_replies.get(task_id)
        if not pr:
            await answer_callback(ev, "Заявка уже обработана или устарела", alert=True)
            return

        if action == "ai_send":
            worker = get_worker(pr.owner_id, pr.phone)
            if not worker:
                pending_ai_replies.pop(task_id, None)
                await answer_callback(ev, "Аккаунт недоступен", alert=True)
                return

            variants = pr.suggested_variants or []
            idx = pr.chosen_index
            if not variants:
                await answer_callback(ev, "Нет текста для отправки", alert=True)
                return
            if idx < 0 or idx >= len(variants):
                idx = 0
            text_to_send = variants[idx]

            try:
                reply_to_id = pr.msg_id if pr.reply_to_source else None
                await worker.send_outgoing(
                    chat_id=pr.peer_id,
                    message=text_to_send,
                    peer=None,
                    reply_to_msg_id=reply_to_id,
                    mark_read_msg_id=pr.msg_id,
                )
            except Exception as e:
                await answer_callback(ev, f"Ошибка отправки: {e}", alert=True)
                return

            pending_ai_replies.pop(task_id, None)
            try:
                await ev.edit(f"✅ Ответ отправлен:\n\n{text_to_send}", buttons=None)
            except Exception:
                pass
            await answer_callback(ev)
            return

        if action == "ai_cancel":
            pending_ai_replies.pop(task_id, None)
            try:
                await ev.edit("❌ Ответ отменён", buttons=None)
            except Exception:
                pass
            await answer_callback(ev)
            return

        if action == "ai_edit":
            editing_ai_reply[admin_id] = task_id
            await answer_callback(ev)
            await bot_client.send_message(
                admin_id,
                "✏️ Отправь новый текст ответа одним сообщением.\n"
                "После этого он сразу будет отправлен пользователю.",
            )
            return

    if data.startswith(("ai_send_final:", "ai_edit_final:")):
        try:
            action, task_id = data.split(":", 1)
        except ValueError:
            await answer_callback(ev, "Некорректные данные кнопки", alert=True)
            return

        pr = pending_ai_replies.get(task_id)
        if not pr:
            await answer_callback(ev, "Заявка уже обработана или устарела", alert=True)
            return

        if action == "ai_send_final":
            # Отправляем выбранный вариант собеседнику
            worker = get_worker(pr.owner_id, pr.phone)
            if not worker:
                await answer_callback(ev, "Аккаунт недоступен", alert=True)
                return

            variants = pr.suggested_variants or []
            idx = pr.chosen_index
            if not variants or idx < 0 or idx >= len(variants):
                await answer_callback(ev, "Вариант недоступен", alert=True)
                return

            text_to_send = variants[idx]

            try:
                reply_to_id = pr.msg_id if pr.reply_to_source else None
                await worker.send_outgoing(
                    chat_id=pr.peer_id,
                    message=text_to_send,
                    peer=None,
                    reply_to_msg_id=reply_to_id,
                    mark_read_msg_id=pr.msg_id,
                )
            except Exception as e:
                await answer_callback(ev, f"Ошибка отправки: {e}", alert=True)
                return

            pending_ai_replies.pop(task_id, None)
            try:
                await ev.edit(f"✅ Ответ отправлен:\n\n{text_to_send}", buttons=None)
            except Exception:
                pass
            await answer_callback(ev)
            return

        if action == "ai_edit_final":
            # Запрашиваем исправление текста
            editing_ai_reply[admin_id] = task_id
            await answer_callback(ev)

            variants = pr.suggested_variants or []
            idx = pr.chosen_index
            if variants and idx >= 0 and idx < len(variants):
                original_text = variants[idx]
            else:
                original_text = ""

            await bot_client.send_message(
                admin_id,
                f"✏️ Отправь исправленный текст ответа одним сообщением.\n\n"
                f"Оригинальный текст для копирования:\n{original_text}\n\n"
                f"После отправки исправленного сообщения, оно будет отправлено пользователю.",
            )
            return


    if data.startswith("history_toggle:"):
        try:
            _, thread_id, mode = data.split(":", 2)
        except ValueError:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        state_map = notification_threads.get(admin_id)
        if not state_map:
            await answer_callback(ev, "История недоступна", alert=True)
            return
        state = state_map.get(thread_id)
        if not state:
            await answer_callback(ev, "История недоступна", alert=True)
            return
        if mode == "open":
            collapsed = False
        elif mode == "close":
            collapsed = True
        else:
            await answer_callback(ev, "Некорректное состояние", alert=True)
            return
        buttons = _build_notification_buttons(state.ctx_id, thread_id, collapsed)
        text = _build_notification_text(
            state.header_lines,
            state.bullets,
            state.history_html,
            collapsed,
        )
        try:
            await ev.edit(text, buttons=buttons, parse_mode="html", link_preview=False)
        except Exception as exc:
            log.debug("Не удалось обновить историю уведомления: %s", exc)
            state_map.pop(thread_id, None)
            if not state_map:
                notification_threads.pop(admin_id, None)
            await answer_callback(ev, "Сообщение устарело", alert=True)
            return
        state.history_collapsed = collapsed
        await answer_callback(ev)
        return

    if data == "proxy_menu":
        await answer_callback(ev)
        await edit_or_send_message(
            ev,
            admin_id,
            format_proxy_settings(admin_id),
            buttons=proxy_menu_buttons(admin_id),
        )
        return

    if data == "proxy_set":
        pending[admin_id] = {"flow": "proxy", "step": "type", "data": {}}
        await answer_callback(ev)
        await edit_or_send_message(
            ev,
            admin_id,
            "Укажи тип прокси (SOCKS5/SOCKS4/HTTP):",
        )
        return

    if data == "proxy_clear":
        cfg = get_tenant_proxy_config(admin_id)
        if not cfg or (not cfg.get("host") and not bool(cfg.get("enabled", True))):
            await answer_callback(ev, "Прокси уже отключён", alert=True)
            return
        clear_tenant_proxy_config(admin_id)
        await answer_callback(ev)
        restarted, errors = await apply_proxy_config_to_owner(admin_id, restart_active=True)
        text_lines = ["🚫 Прокси для ваших аккаунтов отключён.", "", format_proxy_settings(admin_id)]
        if restarted:
            text_lines.append(f"Перезапущено активных аккаунтов: {restarted}.")
        if errors:
            text_lines.append("⚠️ Ошибки при обновлении: " + "; ".join(errors))
        await edit_or_send_message(
            ev,
            admin_id,
            "\n".join(text_lines),
            buttons=proxy_menu_buttons(admin_id),
        )
        return

    if data == "proxy_reset_accounts":
        removed, phones = clear_account_proxy_overrides(admin_id)
        if not removed:
            await answer_callback(ev, "Нет аккаунтов с личным прокси", alert=True)
            return
        await answer_callback(ev)
        restarted, errors = await apply_proxy_config_to_owner(admin_id, restart_active=True)
        text_lines = [
            f"♻️ Удалены персональные прокси у {removed} аккаунтов.",
            "Теперь они используют настройки из раздела 'Прокси'.",
            "",
            format_proxy_settings(admin_id),
        ]
        if removed <= 5:
            phones.sort()
            text_lines.append("\n".join(["", "Аккаунты:"] + [f"• {p}" for p in phones]))
        if restarted:
            text_lines.append(f"Перезапущено активных аккаунтов: {restarted}.")
        if errors:
            text_lines.append("⚠️ Ошибки при обновлении: " + "; ".join(errors))
        await edit_or_send_message(
            ev,
            admin_id,
            "\n".join(text_lines),
            buttons=proxy_menu_buttons(admin_id),
        )
        return

    if data == "proxy_refresh":
        if not get_active_tenant_proxy(admin_id):
            await answer_callback(ev, "Сначала настройте прокси", alert=True)
            return
        await answer_callback(ev)
        restarted, errors = await apply_proxy_config_to_owner(admin_id, restart_active=True)
        summary = [
            "🔄 Переподключение аккаунтов выполнено."
        ]
        if restarted:
            summary.append(f"Обновлено аккаунтов: {restarted}.")
        if errors:
            summary.append("⚠️ Ошибки: " + "; ".join(errors))
        summary.extend(["", format_proxy_settings(admin_id)])
        await edit_or_send_message(
            ev,
            admin_id,
            "\n".join(summary),
            buttons=proxy_menu_buttons(admin_id),
        )
        return

    if data.startswith("ui_back:"):
        session_id = data.split(":", 1)[1]
        success, reason = await interactive_go_back(admin_id, session_id)
        if success:
            await answer_callback(ev)
        else:
            if reason == "expired":
                await answer_callback(ev, "Сообщение устарело", alert=True)
            else:
                await answer_callback(ev, "Возврат недоступен", alert=True)
        return

    if data.startswith("usernoop:"):
        _, user_id_str = data.split(":", 1)
        await answer_callback(ev, f"ID: {user_id_str}")
        return

    if data == "userlist_close":
        await answer_callback(ev)
        with contextlib.suppress(Exception):
            await ev.edit("Список закрыт.", buttons=None)
        return

    if data.startswith("userblock:"):
        if not is_root_admin(admin_id):
            await answer_callback(ev, "Недостаточно прав", alert=True)
            return
        try:
            target_id = int(data.split(":", 1)[1])
        except (TypeError, ValueError):
            await answer_callback(ev, "Некорректный ID", alert=True)
            return
        if is_root_admin(target_id):
            await answer_callback(ev, "Нельзя отключить супер-администратора.", alert=True)
            return
        tenant_data = tenants.get(tenant_key(target_id))
        if not tenant_data:
            await answer_callback(ev, "Пользователь не найден.", alert=True)
            return
        await clear_owner_runtime(target_id)
        archive_user_data(target_id)
        if remove_tenant(target_id):
            await safe_send_admin("Ваш доступ к менеджеру отключен.", owner_id=target_id)
            await send_user_access_list(admin_id, event=ev)
            await answer_callback(ev, "Доступ пользователя отключён и данные архивированы.", alert=True)
        else:
            await answer_callback(ev, "Не удалось отключить пользователя.", alert=True)
        return

    if data == "files_delete":
        await answer_callback(ev)
        await ev.edit("Выбери тип файлов для удаления:", buttons=files_delete_menu())
        return

    if data == "files_paste":
        pending[admin_id] = {"flow": "file", "file_type": "paste", "step": "name"}
        await answer_callback(ev)
        await edit_or_send_message(ev, admin_id, "Введите название пасты:")
        return

    if data == "files_voice":
        pending[admin_id] = {"flow": "file", "file_type": "voice", "step": "name"}
        await answer_callback(ev)
        await edit_or_send_message(ev, admin_id, "Введите название голосового:")
        return

    if data == "files_video":
        pending[admin_id] = {"flow": "file", "file_type": "video", "step": "name"}
        await answer_callback(ev)
        await edit_or_send_message(ev, admin_id, "Введите название медиа:")
        return

    if data == "files_sticker":
        pending[admin_id] = {"flow": "file", "file_type": "sticker", "step": "name"}
        await answer_callback(ev)
        await bot_client.send_message(admin_id, "Введите название стикера:")
        return


    if data == "add":
        pending[admin_id] = {"flow": "account", "step": "proxy_or_phone"}
        await answer_callback(ev)
        await edit_or_send_message(
            ev,
            admin_id,
            ADD_ACCOUNT_PROMPT,
        )
        return


    if data == "list":
        accounts = get_accounts_meta(admin_id)
        if not accounts:
            await answer_callback(ev, "Пусто", alert=True)
            await edit_or_send_message(
                ev,
                admin_id,
                "Аккаунтов нет.",
                buttons=main_menu(),
            )
            return
        await answer_callback(ev)
        await edit_or_send_message(
            ev,
            admin_id,
            "Выберите действие:",
            buttons=account_control_menu(),
        )
        return

    if data == "validate_all_accounts":
        await answer_callback(ev)
        result_text = await validate_all_accounts(admin_id)
        await edit_or_send_message(ev, admin_id, result_text, buttons=main_menu())
        return

    if data == "delete_account_menu":
        await answer_callback(ev)
        accounts = get_accounts_meta(admin_id)
        if not accounts:
            await edit_or_send_message(ev, admin_id, "Аккаунтов нет.", buttons=main_menu())
            return
        # Показываем список аккаунтов как inline кнопки
        buttons = []
        for phone in accounts:
            buttons.append([Button.inline(phone, f"del_account_{phone}".encode())])
        buttons.append([Button.inline("← Назад", b"back")])
        await edit_or_send_message(
            ev,
            admin_id,
            "Выберите аккаунт для удаления:",
            buttons=buttons
        )
        return

    if data.startswith("del_account_"):
        phone = data[len("del_account_"):]
        worker = get_worker(admin_id, phone)
        await answer_callback(ev)
        if worker:
            await worker.logout()
            unregister_worker(admin_id, phone)
        for ctx_key, ctx_val in list(reply_contexts.items()):
            if ctx_val.get("phone") == phone and ctx_val.get("owner_id") == admin_id:
                reply_contexts.pop(ctx_key, None)
                for admin_key, waiting_ctx in list(reply_waiting.items()):
                    if waiting_ctx.get("ctx") == ctx_key:
                        reply_waiting.pop(admin_key, None)
        threads = notification_threads.get(admin_id)
        if threads:
            prefix = f"{phone}:"
            for thread_id in list(threads.keys()):
                if thread_id.startswith(prefix):
                    threads.pop(thread_id, None)
            if not threads:
                notification_threads.pop(admin_id, None)
        accounts = get_accounts_meta(admin_id)
        meta = accounts.pop(phone, None)
        persist_tenants()
        if meta and meta.get("session_file") and os.path.exists(meta["session_file"]):
            with contextlib.suppress(OSError):
                os.remove(meta["session_file"])
        await edit_or_send_message(
            ev,
            admin_id,
            f"🗑 Аккаунт {phone} удалён.",
            buttons=main_menu(),
        )
        return

    if data == "back":
        await answer_callback(ev)
        await edit_or_send_message(ev, admin_id, "Главное меню", buttons=main_menu())
        return

    if data == "main_menu":
        await answer_callback(ev)
        await edit_or_send_message(ev, admin_id, "Главное меню", buttons=main_menu())
        return

    if data == "del_select":
        if not get_accounts_meta(admin_id):
            await answer_callback(ev, "Нет аккаунтов", alert=True); return
        await answer_callback(ev)
        buttons, page, total_pages, _ = build_account_buttons(admin_id, "del_do")
        caption = format_page_caption("Выбери аккаунт для удаления", page, total_pages)
        await edit_or_send_message(ev, admin_id, caption, buttons=buttons)
        return

    if data == "val_select":
        if not get_accounts_meta(admin_id):
            await answer_callback(ev, "Нет аккаунтов", alert=True); return
        await answer_callback(ev)
        buttons, page, total_pages, _ = build_account_buttons(admin_id, "val_do")
        caption = format_page_caption("Выбери аккаунт для проверки", page, total_pages)
        await edit_or_send_message(ev, admin_id, caption, buttons=buttons)
        return

    if data.startswith("acct_page:"):
        try:
            _, prefix, page_str = data.split(":", 2)
        except ValueError:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        try:
            page = int(page_str)
        except ValueError:
            await answer_callback(ev, "Некорректная страница", alert=True)
            return
        buttons, current_page, total_pages, total_count = build_account_buttons(
            admin_id, prefix, page
        )
        if total_count == 0:
            await answer_callback(ev, "Нет аккаунтов", alert=True)
            await ev.edit("Аккаунтов нет.", buttons=None)
            return
        if prefix == "del_do":
            caption = format_page_caption("Выбери аккаунт для удаления", current_page, total_pages)
        elif prefix == "val_do":
            caption = format_page_caption("Выбери аккаунт для проверки", current_page, total_pages)
        else:
            caption = format_page_caption("Выберите аккаунт", current_page, total_pages)
        await answer_callback(ev)
        await ev.edit(caption, buttons=buttons)
        return

    if data.startswith("file_del_page:"):
        try:
            _, file_type, page_str = data.split(":", 2)
        except ValueError:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        if file_type not in FILE_TYPE_LABELS:
            await answer_callback(ev, "Неизвестный тип", alert=True)
            return
        try:
            page = int(page_str)
        except ValueError:
            await answer_callback(ev, "Некорректная страница", alert=True)
            return
        files = list_templates_by_type(admin_id, file_type)
        if not files:
            await answer_callback(ev, "Файлы отсутствуют", alert=True)
            await ev.edit(
                f"{FILE_TYPE_LABELS[file_type]} отсутствуют.",
                buttons=files_delete_menu(),
            )
            return
        buttons, current_page, total_pages, _ = build_file_delete_keyboard(
            files, file_type, page
        )
        caption = format_page_caption(
            f"{FILE_TYPE_LABELS[file_type]} для удаления", current_page, total_pages
        )
        await answer_callback(ev)
        await ev.edit(caption, buttons=buttons)
        return

    if data.startswith("show_del_files:"):
        file_type = data.split(":", 1)[1]
        if file_type not in FILE_TYPE_LABELS:
            await answer_callback(ev, "Неизвестный тип файлов", alert=True)
            return

        files = list_templates_by_type(admin_id, file_type)
        if not files:
            label = FILE_TYPE_LABELS[file_type]
            await answer_callback(ev, f"{label} отсутствуют", alert=True)
            return

        buttons, current_page, total_pages, _ = build_file_delete_keyboard(
            files, file_type, 0
        )
        caption = format_page_caption(
            f"{FILE_TYPE_LABELS[file_type]} для удаления", current_page, total_pages
        )
        await answer_callback(ev)
        await ev.edit(caption, buttons=buttons)
        return

    if data.startswith("file_del_do:"):
        try:
            _, file_type, page_str, encoded = data.split(":", 3)
        except ValueError:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        if file_type not in FILE_TYPE_LABELS:
            await answer_callback(ev, "Неизвестный тип", alert=True)
            return
        try:
            page = int(page_str)
        except ValueError:
            page = 0
        path = _resolve_payload(encoded)
        if path is None:
            try:
                path = _decode_payload(encoded)
            except Exception:
                await answer_callback(ev, "Некорректные данные", alert=True)
                return
        abs_path = os.path.abspath(path)
        allowed_dirs = [
            os.path.abspath(d)
            for d in _allowed_template_directories(admin_id, file_type)
            if d
        ]
        if not any(_is_path_within(abs_path, base) for base in allowed_dirs):
            await answer_callback(ev, "Удаление запрещено", alert=True)
            return
        try:
            os.remove(abs_path)
        except FileNotFoundError:
            pass
        except OSError as e:
            await answer_callback(ev, f"Не удалось удалить файл: {e}", alert=True)
            return
        files = list_templates_by_type(admin_id, file_type)
        if not files:
            await ev.edit(
                f"{FILE_TYPE_LABELS[file_type]} отсутствуют.",
                buttons=files_delete_menu(),
            )
            await answer_callback(ev, "Файл удалён")
            return
        buttons, current_page, total_pages, _ = build_file_delete_keyboard(
            files, file_type, page
        )
        caption = format_page_caption(
            f"{FILE_TYPE_LABELS[file_type]} для удаления", current_page, total_pages
        )
        await ev.edit(caption, buttons=buttons)
        await answer_callback(ev, "Файл удалён")
        return

    if data.startswith("del_do:"):
        phone = data.split(":", 1)[1]
        worker = get_worker(admin_id, phone)
        await answer_callback(ev)
        if worker:
            await worker.logout()
            unregister_worker(admin_id, phone)
        for ctx_key, ctx_val in list(reply_contexts.items()):
            if ctx_val.get("phone") == phone and ctx_val.get("owner_id") == admin_id:
                reply_contexts.pop(ctx_key, None)
                for admin_key, waiting_ctx in list(reply_waiting.items()):
                    if waiting_ctx.get("ctx") == ctx_key:
                        reply_waiting.pop(admin_key, None)
        threads = notification_threads.get(admin_id)
        if threads:
            prefix = f"{phone}:"
            for thread_id in list(threads.keys()):
                if thread_id.startswith(prefix):
                    threads.pop(thread_id, None)
            if not threads:
                notification_threads.pop(admin_id, None)
        accounts = get_accounts_meta(admin_id)
        meta = accounts.pop(phone, None)
        persist_tenants()
        if meta and meta.get("session_file") and os.path.exists(meta["session_file"]):
            with contextlib.suppress(OSError):
                os.remove(meta["session_file"])
        await edit_or_send_message(
            ev,
            admin_id,
            f"🗑 Аккаунт {phone} удалён.",
            buttons=main_menu(),
        )
        return

    if data.startswith("val_do:"):
        phone = data.split(":", 1)[1]
        await answer_callback(ev)
        meta = get_account_meta(admin_id, phone) or {}
        state = meta.get("state")
        worker = await ensure_worker_running(admin_id, phone)
        if not worker:
            if state == "banned":
                result_text = f"⛔️ {phone} заблокирован Telegram. Аккаунт отключён."
            elif state == "frozen":
                result_text = f"🧊 {phone} заморожен Telegram. Требуется разблокировка."
            else:
                result_text = f"⚠️ Аккаунт {phone} не активен."
        else:
            ok = await worker.validate()
            if ok:
                result_text = f"✅ {phone} активен и принимает сообщения."
            elif state == "banned":
                result_text = f"⛔️ {phone} заблокирован Telegram. Аккаунт отключён."
            elif state == "frozen":
                result_text = f"🧊 {phone} заморожен Telegram. Требуется разблокировка."
            else:
                result_text = f"❌ {phone} не отвечает. Проверь подключение."

        await edit_or_send_message(
            ev,
            admin_id,
            result_text,
            buttons=main_menu(),
        )
        return

    if data.startswith("mark_read:"):
        ctx = data.split(":", 1)[1]
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        await answer_callback(ev, "Диалог помечен прочитанным")
        return

    if data.startswith("reply:") or data.startswith("reply_to:"):
        ctx = data.split(":", 1)[1]
        mode = "reply" if data.startswith("reply_to:") else "normal"
        error = await _activate_reply_session(admin_id, ctx, mode)
        if error:
            await answer_callback(ev, error, alert=True)
        else:
            await answer_callback(ev)
        return

    if data.startswith("reply_reaction_menu:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        menu_token, ctx, mode = parts
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        if mode != "reply":
            await answer_callback(ev, "Реакции доступны только для реплая", alert=True)
            return
        reply_waiting[admin_id] = {"ctx": ctx, "mode": mode}
        await answer_callback(ev)
        await show_interactive_message(
            admin_id,
            "Выбери реакцию для сообщения:",
            buttons=build_reaction_keyboard(ctx, mode),
            replace=True,
        )
        return

    if data.startswith("reply_reaction_back:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        _, ctx, mode = parts
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        reply_waiting[admin_id] = {"ctx": ctx, "mode": mode}
        await answer_callback(ev)
        await show_interactive_message(
            admin_id,
            build_reply_prompt(ctx_info, mode),
            buttons=build_reply_options_keyboard(ctx, mode),
            replace=True,
        )
        return

    if data.startswith("reply_mode:"):
        parts = data.split(":", 2)
        if len(parts) != 3:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        _, ctx, mode = parts
        if mode not in {"normal", "reply"}:
            await answer_callback(ev, "Неизвестный режим", alert=True)
            return
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        reply_waiting[admin_id] = {"ctx": ctx, "mode": mode}
        await answer_callback(ev)
        await show_interactive_message(
            admin_id,
            build_reply_prompt(ctx_info, mode),
            buttons=build_reply_options_keyboard(ctx, mode),
            replace=True,
        )
        return

    if data.startswith("reply_reaction:"):
        parts = data.split(":", 3)
        if len(parts) != 4:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        _, ctx, mode, encoded = parts
        if mode != "reply":
            await answer_callback(ev, "Реакции доступны только для реплая", alert=True)
            return
        try:
            emoji = _decode_payload(encoded)
        except Exception:
            await answer_callback(ev, "Некорректная реакция", alert=True)
            return
        if emoji not in REACTION_EMOJI_SET:
            await answer_callback(ev, "Реакция не поддерживается", alert=True)
            return
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        msg_id = ctx_info.get("msg_id")
        if msg_id is None:
            await answer_callback(ev, "Сообщение недоступно", alert=True)
            return
        worker = get_worker(admin_id, ctx_info["phone"])
        if not worker:
            await answer_callback(ev, "Аккаунт недоступен", alert=True)
            return
        try:
            await worker.send_reaction(
                ctx_info["chat_id"],
                emoji,
                ctx_info.get("peer"),
                msg_id=msg_id,
            )
        except Exception as e:
            await answer_callback(ev, f"Ошибка реакции: {e}", alert=True)
            return
        reply_waiting[admin_id] = {"ctx": ctx, "mode": mode}
        await answer_callback(ev, "Реакция отправлена")
        await bot_client.send_message(
            admin_id,
            f"✅ Реакция {emoji} добавлена к сообщению собеседника.",
        )
        return

    if data.startswith("reply_cancel:"):
        ctx = data.split(":", 1)[1]
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if ctx_info:
            await mark_dialog_read_for_context(ctx_info)
        reply_waiting.pop(admin_id, None)
        await answer_callback(ev)
        await clear_interactive_message(admin_id)
        await send_temporary_message(admin_id, "❌ Ответ отменён.")
        return

    if data.startswith("block_contact:"):
        ctx = data.split(":", 1)[1]
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        worker = get_worker(admin_id, ctx_info["phone"])
        if not worker:
            await answer_callback(ev, "Аккаунт недоступен", alert=True)
            return
        await answer_callback(ev)
        try:
            await worker.block_contact(ctx_info["chat_id"], ctx_info.get("peer"))
        except Exception as e:
            await bot_client.send_message(
                admin_id,
                f"❌ Не удалось заблокировать собеседника: {e}",
            )
        else:
            reply_contexts.pop(ctx, None)
            clear_notification_thread(
                admin_id, _make_thread_id(ctx_info["phone"], ctx_info["chat_id"])
            )
            await bot_client.send_message(
                admin_id,
                "🚫 Собеседник заблокирован. Диалог удалён для обеих сторон.",
            )
            return

    if data.startswith(
        (
            "reply_paste_menu:",
            "reply_voice_menu:",
            "reply_video_menu:",
            "reply_sticker_menu:",
        )
    ):
        parts = data.split(":", 2)
        if len(parts) != 3:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        _, ctx, mode = parts
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        file_type = menu_token.split("_", 2)[1] if "_" in menu_token else ""
        menu = _prepare_reply_asset_menu(ctx_info["owner_id"], file_type)
        if not menu:
            await answer_callback(ev, "Неизвестный тип", alert=True)
            return
        files, empty_text, title, prefix = menu
        if not files:
            await answer_callback(ev, empty_text, alert=True)
            return
        await answer_callback(ev)
        await show_interactive_message(
            admin_id,
            title,
            buttons=build_asset_keyboard(files, file_type, prefix, ctx, mode),
            replace=True,
        )
        return

    if data.startswith("paste_send:"):
        parts = data.split(":")
        if len(parts) not in (3, 4):
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        if len(parts) == 3:
            _, ctx, idx_str = parts
            mode = "normal"
        else:
            _, ctx, mode, idx_str = parts
            if mode not in {"normal", "reply"}:
                mode = "normal"
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        try:
            idx = int(idx_str)
        except ValueError:
            await answer_callback(ev, "Некорректный выбор", alert=True)
            return
        files = list_text_templates(admin_id)
        if idx < 0 or idx >= len(files):
            await answer_callback(ev, "Файл не найден", alert=True)
            return
        file_path = files[idx]
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
        except Exception as e:
            await answer_callback(ev, f"Ошибка чтения: {e}", alert=True)
            return
        if not content:
            await answer_callback(ev, "Файл пуст", alert=True)
            return
        worker = get_worker(admin_id, ctx_info["phone"])
        if not worker:
            await answer_callback(ev, "Аккаунт недоступен", alert=True)
            return
        reply_to_msg_id = ctx_info.get("msg_id") if mode == "reply" else None
        reply_to_msg_id = ctx_info.get("msg_id") if mode == "reply" else None
        try:
            sent = await worker.send_outgoing(
                ctx_info["chat_id"],
                content,
                ctx_info.get("peer"),
                reply_to_msg_id=reply_to_msg_id,
                mark_read_msg_id=ctx_info.get("msg_id"),
            )
        except Exception as e:
            await answer_callback(ev, f"Ошибка отправки: {e}", alert=True)
            return
        token = register_outgoing_action(
            admin_id,
            phone=ctx_info["phone"],
            chat_id=ctx_info["chat_id"],
            peer=ctx_info.get("peer"),
            msg_id=_extract_message_id(sent),
            message_type="text",
        )
        buttons = (
            build_outgoing_control_buttons(token, allow_edit=True) if token else None
        )
        clear_notification_thread(
            admin_id, _make_thread_id(ctx_info["phone"], ctx_info["chat_id"])
        )
        await answer_callback(ev, "✅ Паста отправлена")
        await bot_client.send_message(
            admin_id,
            "✅ Паста отправлена собеседнику.",
            buttons=buttons,
        )
        return

    if data.startswith("voice_send:"):
        parts = data.split(":")
        if len(parts) not in (3, 4):
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        if len(parts) == 3:
            _, ctx, idx_str = parts
            mode = "normal"
        else:
            _, ctx, mode, idx_str = parts
            if mode not in {"normal", "reply"}:
                mode = "normal"
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        try:
            idx = int(idx_str)
        except ValueError:
            await answer_callback(ev, "Некорректный выбор", alert=True)
            return
        files = list_voice_templates(admin_id)
        if idx < 0 or idx >= len(files):
            await answer_callback(ev, "Файл не найден", alert=True)
            return
        file_path = files[idx]
        worker = get_worker(admin_id, ctx_info["phone"])
        if not worker:
            await answer_callback(ev, "Аккаунт недоступен", alert=True)
            return
        reply_to_msg_id = ctx_info.get("msg_id") if mode == "reply" else None
        try:
            sent = await worker.send_voice(
                ctx_info["chat_id"],
                file_path,
                ctx_info.get("peer"),
                reply_to_msg_id=reply_to_msg_id,
                mark_read_msg_id=ctx_info.get("msg_id"),
            )
        except Exception as e:
            await answer_callback(ev, f"Ошибка отправки: {e}", alert=True)
            return
        await answer_callback(ev, "✅ Голосовое отправлено")
        token = register_outgoing_action(
            admin_id,
            phone=ctx_info["phone"],
            chat_id=ctx_info["chat_id"],
            peer=ctx_info.get("peer"),
            msg_id=_extract_message_id(sent),
            message_type="voice",
        )
        buttons = (
            build_outgoing_control_buttons(token, allow_edit=False) if token else None
        )
        clear_notification_thread(
            admin_id, _make_thread_id(ctx_info["phone"], ctx_info["chat_id"])
        )
        await bot_client.send_message(
            admin_id,
            "✅ Голосовое сообщение отправлено собеседнику.",
            buttons=buttons,
        )
        return

    if data.startswith("sticker_send:"):
        parts = data.split(":")
        if len(parts) not in (3, 4):
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        if len(parts) == 3:
            _, ctx, idx_str = parts
            mode = "normal"
        else:
            _, ctx, mode, idx_str = parts
            if mode not in {"normal", "reply"}:
                mode = "normal"
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        try:
            idx = int(idx_str)
        except ValueError:
            await answer_callback(ev, "Некорректный выбор", alert=True)
            return
        files = list_sticker_templates(admin_id)
        if idx < 0 or idx >= len(files):
            await answer_callback(ev, "Файл не найден", alert=True)
            return
        file_path = files[idx]
        worker = get_worker(admin_id, ctx_info["phone"])
        if not worker:
            await answer_callback(ev, "Аккаунт недоступен", alert=True)
            return
        reply_to_msg_id = ctx_info.get("msg_id") if mode == "reply" else None
        try:
            sent = await worker.send_sticker(
                ctx_info["chat_id"],
                file_path,
                ctx_info.get("peer"),
                reply_to_msg_id=reply_to_msg_id,
                mark_read_msg_id=ctx_info.get("msg_id"),
            )
        except Exception as e:
            await answer_callback(ev, f"Ошибка отправки: {e}", alert=True)
            return
        await answer_callback(ev, "✅ Стикер отправлен")
        token = register_outgoing_action(
            admin_id,
            phone=ctx_info["phone"],
            chat_id=ctx_info["chat_id"],
            peer=ctx_info.get("peer"),
            msg_id=_extract_message_id(sent),
            message_type="sticker",
        )
        buttons = build_outgoing_control_buttons(token, allow_edit=False) if token else None
        clear_notification_thread(
            admin_id, _make_thread_id(ctx_info["phone"], ctx_info["chat_id"])
        )
        await bot_client.send_message(
            admin_id,
            "✅ Стикер отправлен собеседнику.",
            buttons=buttons,
        )
        return

    if data.startswith("video_send:"):
        parts = data.split(":")
        if len(parts) not in (3, 4):
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        if len(parts) == 3:
            _, ctx, idx_str = parts
            mode = "normal"
        else:
            _, ctx, mode, idx_str = parts
            if mode not in {"normal", "reply"}:
                mode = "normal"
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await answer_callback(ev, "Контекст истёк", alert=True)
            return
        await mark_dialog_read_for_context(ctx_info)
        try:
            idx = int(idx_str)
        except ValueError:
            await answer_callback(ev, "Некорректный выбор", alert=True)
            return
        files = list_video_templates(admin_id)
        if idx < 0 or idx >= len(files):
            await answer_callback(ev, "Файл не найден", alert=True)
            return
        file_path = files[idx]
        worker = get_worker(admin_id, ctx_info["phone"])
        if not worker:
            await answer_callback(ev, "Аккаунт недоступен", alert=True)
            return
        reply_to_msg_id = ctx_info.get("msg_id") if mode == "reply" else None
        try:
            sent = await worker.send_media(
                ctx_info["chat_id"],
                file_path,
                ctx_info.get("peer"),
                reply_to_msg_id=reply_to_msg_id,
                mark_read_msg_id=ctx_info.get("msg_id"),
            )
        except Exception as e:
            await answer_callback(ev, f"Ошибка отправки: {e}", alert=True)
            return
        await answer_callback(ev, "✅ Медиа отправлено")
        token = register_outgoing_action(
            admin_id,
            phone=ctx_info["phone"],
            chat_id=ctx_info["chat_id"],
            peer=ctx_info.get("peer"),
            msg_id=_extract_message_id(sent),
            message_type="video",
        )
        buttons = (
            build_outgoing_control_buttons(token, allow_edit=False) if token else None
        )
        clear_notification_thread(
            admin_id, _make_thread_id(ctx_info["phone"], ctx_info["chat_id"])
        )
        await bot_client.send_message(
            admin_id,
            "✅ Медиа отправлено собеседнику.",
            buttons=buttons,
        )
        return
    if data.startswith("out_edit:"):
        token = data.split(":", 1)[1]
        info = outgoing_actions.get(token)
        if not info or info.get("admin_id") != admin_id:
            await answer_callback(ev, "Контекст недоступен", alert=True)
            return
        if info.get("type") != "text":
            await answer_callback(ev, "Сообщение нельзя исправить", alert=True)
            return
        edit_waiting[admin_id] = {"token": token}
        await answer_callback(ev)
        await bot_client.send_message(
            admin_id,
            "✏️ Пришли исправленный текст. Для отмены нажми MENU.",
        )
        return
    if data.startswith("out_delete:"):
        token = data.split(":", 1)[1]
        info = outgoing_actions.get(token)
        if not info or info.get("admin_id") != admin_id:
            await answer_callback(ev, "Контекст недоступен", alert=True)
            return
        worker = get_worker(admin_id, info.get("phone")) if info.get("phone") else None
        if not worker:
            await answer_callback(ev, "Аккаунт недоступен", alert=True)
            return
        try:
            await worker.delete_message(
                info["chat_id"],
                info["msg_id"],
                info.get("peer"),
            )
        except Exception as e:
            await answer_callback(ev, f"Ошибка удаления: {e}", alert=True)
            return
        outgoing_actions.pop(token, None)
        if edit_waiting.get(admin_id, {}).get("token") == token:
            edit_waiting.pop(admin_id, None)
        await answer_callback(ev, "Сообщение стерто")
        await bot_client.send_message(
            admin_id,
            "🗑 Сообщение стерто для обеих сторон.",
        )
        return
    if data.startswith("asset_page:"):
        parts = data.split(":", 4)
        if len(parts) != 5:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        _, file_type, ctx, mode_token, page_str = parts
        try:
            page = int(page_str)
        except ValueError:
            await answer_callback(ev, "Некорректные данные", alert=True)
            return
        mode = mode_token if mode_token in {"normal", "reply"} else None
        await answer_callback(ev)
        error = await _open_reply_asset_menu(
            admin_id, ctx, mode, file_type, page=page
        )
        if error:
            await send_temporary_message(admin_id, f"❌ {error}")
        return

    if data == "asset_close":
        await answer_callback(ev)
        return

@bot_client.on(events.NewMessage)

async def on_text(ev):
    admin_id = _extract_event_user_id(ev)
    if admin_id is None or not is_admin(admin_id):
        return
    text = (ev.raw_text or "").strip()

    await ensure_menu_button_hidden(admin_id)

    # Обработка служебных фраз для добавления аккаунта
    if text == "START_ADD_WITH_PROXY":
        await ev.delete()  # Удаляем служебное сообщение
        await _send_account_add_prompt(admin_id, _init_account_add_manual(admin_id))
        return
    elif text == "START_ADD_WITHOUT_PROXY":
        await ev.delete()  # Удаляем служебное сообщение
        await _send_account_add_prompt(admin_id, _init_account_add_direct(admin_id))
        return

    # Обработка служебных фраз для меню аккаунтов
    elif text == "START_VALIDATE_ACCOUNTS":
        await ev.delete()  # Удаляем служебное сообщение
        accounts = get_accounts_meta(admin_id)
        if not accounts:
            await bot_client.send_message(admin_id, "Аккаунтов нет.")
            return
        # Запуск валидации всех аккаунтов
        result_text = await validate_all_accounts(admin_id)
        await bot_client.send_message(admin_id, result_text, buttons=main_menu())
        return
    elif text == "START_DELETE_ACCOUNT":
        await ev.delete()  # Удаляем служебное сообщение
        accounts = get_accounts_meta(admin_id)
        if not accounts:
            await bot_client.send_message(admin_id, "Аккаунтов нет.")
            return
        # Показываем кнопку для открытия inline режима со списком аккаунтов
        buttons = [[Button.switch_inline("Выбрать аккаунт для удаления", query="delete_account_list", same_peer=True)]]
        await bot_client.send_message(admin_id, "Нажмите кнопку для выбора аккаунта:", buttons=buttons)
        return
    
    # Обработка удаления аккаунта из inline плашки
    elif text.startswith("DEL_ACCOUNT_"):
        await ev.delete()  # Удаляем служебное сообщение
        phone = text[len("DEL_ACCOUNT_"):]
        worker = get_worker(admin_id, phone)
        if worker:
            await worker.logout()
            unregister_worker(admin_id, phone)
        # Очищаем контексты
        for ctx_key, ctx_val in list(reply_contexts.items()):
            if ctx_val.get("phone") == phone and ctx_val.get("owner_id") == admin_id:
                reply_contexts.pop(ctx_key, None)
                for admin_key, waiting_ctx in list(reply_waiting.items()):
                    if waiting_ctx.get("ctx") == ctx_key:
                        reply_waiting.pop(admin_key, None)
        # Очищаем уведомления
        threads = notification_threads.get(admin_id)
        if threads:
            prefix = f"{phone}:"
            for thread_id in list(threads.keys()):
                if thread_id.startswith(prefix):
                    threads.pop(thread_id, None)
            if not threads:
                notification_threads.pop(admin_id, None)
        await bot_client.send_message(admin_id, f"Аккаунт {phone} удалён.", buttons=main_menu())
        return

    sentinel_index = text.find(INLINE_REPLY_SENTINEL)
    if sentinel_index != -1:
        payload_token = text[sentinel_index + len(INLINE_REPLY_SENTINEL) :].strip()
        handled = await _process_inline_reply_token(admin_id, payload_token)
        if not handled:
            tokens = [token for token in payload_token.split(":") if token]
            if len(tokens) >= 2:
                mode_token, ctx = tokens[0], tokens[1]
                mode = "reply" if mode_token == "reply" else "normal"
                error = await _activate_reply_session(admin_id, ctx, mode)
                if error:
                    await send_temporary_message(admin_id, f"❌ {error}")
                else:
                    if len(tokens) >= 4 and tokens[2] == "picker":
                        file_type = tokens[3]
                        picker_error = await _open_reply_asset_menu(
                            admin_id, ctx, mode, file_type
                        )
                        if picker_error:
                            await send_temporary_message(admin_id, f"❌ {picker_error}")
        with contextlib.suppress(Exception):
            await ev.delete()
        return

    # Инлайновое удаление файла через служебные тексты DEL_FILETYPE_IDX
    if text.startswith("DEL_PASTE_") or text.startswith("DEL_VOICE_") or text.startswith("DEL_VIDEO_") or text.startswith("DEL_STICKER_"):
        # Парсим служебный текст: DEL_FILETYPE_IDX
        parts = text.split("_", 2)
        if len(parts) == 3:
            _, file_type_str, idx_str = parts
            file_type = file_type_str.lower()
            if file_type in FILE_TYPE_LABELS:
                try:
                    idx = int(idx_str)
                    files = list_templates_by_type(admin_id, file_type)
                    if 0 <= idx < len(files):
                        file_path = files[idx]
                        file_name = os.path.basename(file_path)

                        # Удаляем файл
                        try:
                            if os.path.exists(file_path):
                                os.remove(file_path)
                                await bot_client.send_message(
                                    admin_id,
                                    f"✅ **Файл удалён:**\n`{file_name}`"
                                )
                            else:
                                await bot_client.send_message(
                                    admin_id,
                                    f"⚠️ **Файл уже отсутствует:**\n`{file_name}`"
                                )
                        except Exception as e:
                            logger.error(f"Failed to delete file {file_path}: {e}")
                            await bot_client.send_message(
                                admin_id,
                                f"❌ **Ошибка при удалении:**\n`{e}`"
                            )
                except (ValueError, IndexError) as e:
                    logger.error(f"Failed to parse delete command: {text}, error: {e}")

        # Удаляем служебное сообщение
        with contextlib.suppress(Exception):
            await ev.delete()
        return

    # Инлайновое удаление файла (служебное сообщение из inline-результата)
    if text.startswith("INLINE_DEL:"):
        parts = text.split(":", 2)
        if len(parts) == 3:
            _, file_type, encoded = parts
            if file_type in FILE_TYPE_LABELS:
                path = _resolve_payload(encoded)
                if path is None:
                    # Пытаемся декодировать как base64-путь (на всякий случай)
                    with contextlib.suppress(Exception):
                        path = _decode_payload(encoded)
                if path is not None:
                    allowed_dirs = _allowed_template_directories(admin_id, file_type)
                    if any(_is_path_within(d, path) for d in allowed_dirs):
                        name = os.path.basename(path)
                        try:
                            if os.path.exists(path):
                                os.remove(path)
                                await ev.respond(f"🗑 Файл «{name}» удалён.")
                            else:
                                await ev.respond(f"Файл «{name}» уже отсутствует.")
                        except Exception as e:
                            await ev.respond(f"Не удалось удалить файл: {e}")
        # Служебное сообщение можно удалить, чтобы не мусорить
        with contextlib.suppress(Exception):
            await ev.delete()
        return

    library_query = _extract_library_command_query(text)
    if library_query:
        tokens = library_query.split()
        if tokens and tokens[0].lower() in LIBRARY_INLINE_QUERY_PREFIXES:
            tokens = tokens[1:]

        if len(tokens) == 1:
            mode_token = tokens[0].lower()
            if mode_token == "add":
                await bot_client.send_message(
                    admin_id,
                    "Выбери тип файлов для сохранения:",
                    buttons=files_add_menu(),
                )
                with contextlib.suppress(Exception):
                    await ev.delete()
                return

            if mode_token in {"delete", "del", "remove"}:
                await bot_client.send_message(
                    admin_id,
                    "Выбери тип файлов для удаления:",
                    buttons=files_delete_menu(),
                )
                with contextlib.suppress(Exception):
                    await ev.delete()
                return

        rendered = _render_library_command(admin_id, library_query)
        if rendered:
            await bot_client.send_message(admin_id, rendered)
        with contextlib.suppress(Exception):
            await ev.delete()
        return

    if text.startswith("INLINE_ADD:"):
        _, _, file_type = text.partition(":")
        file_type = file_type.strip().lower()
        if file_type in FILE_TYPE_LABELS:
            pending[admin_id] = {"flow": "file", "file_type": file_type, "step": "name"}
            prompt = FILE_TYPE_ADD_PROMPTS.get(file_type) or "Введите название файла:"
            await bot_client.send_message(admin_id, prompt)
        with contextlib.suppress(Exception):
            await ev.delete()
        return

    # Если админ редактирует AI-подсказку
    task_id = editing_ai_reply.get(admin_id)
    if task_id:
        pr = pending_ai_replies.get(task_id)
        if not pr:
            editing_ai_reply.pop(admin_id, None)
            await ev.reply("Заявка не найдена или уже обработана.")
            return

        if not text:
            await ev.reply("Пустой текст, ничего не отправлено.")
            return

        worker = get_worker(pr.owner_id, pr.phone)
        if not worker:
            editing_ai_reply.pop(admin_id, None)
            pending_ai_replies.pop(task_id, None)
            await ev.reply("Аккаунт недоступен, отправка невозможна.")
            return

        try:
            reply_to_id = pr.msg_id if pr.reply_to_source else None
            await worker.send_outgoing(
                chat_id=pr.peer_id,
                message=text,
                peer=None,
                reply_to_msg_id=reply_to_id,
                mark_read_msg_id=pr.msg_id,
            )
        except Exception as e:
            await ev.reply(f"Ошибка отправки: {e}")
            return

        pending_ai_replies.pop(task_id, None)
        editing_ai_reply.pop(admin_id, None)
        await ev.reply("✅ Отправлено с твоим текстом:\n\n" + text)
        return

    if text.startswith("/"):
        await cancel_operations(admin_id)
        parts = text.split()
        cmd_full = parts[0].lower()
        cmd_base = cmd_full.split("@", 1)[0]
        if cmd_base == "/start":
            return
        elif cmd_base in {"/add", "/addaccount"}:
            pending[admin_id] = {"flow": "account", "step": "proxy_or_phone"}
            await ev.respond(ADD_ACCOUNT_PROMPT)
        elif cmd_base in {"/accounts", "/list"}:
            accounts = get_accounts_meta(admin_id)
            if not accounts:
                await ev.respond("Аккаунтов нет.", buttons=main_menu())
                return
            lines = ["Аккаунты:"]
            for p, m in accounts.items():
                worker = get_worker(admin_id, p)
                active = bool(worker and worker.started)
                state = m.get("state")
                note_extra = ""
                if m.get("state_note"):
                    note_extra = f" ({m['state_note']})"
                if state == "banned":
                    status = "⛔️"
                    note = " | заблокирован Telegram"
                elif state == "frozen":
                    status = "🧊"
                    note = " | заморожен Telegram"
                elif m.get("session_invalid"):
                    status = "❌"
                    note = " | требуется повторный вход"
                elif active:
                    status = "🟢"
                    note = ""
                else:
                    status = "⚠️"
                    note = " | неактивен"
                proxy_label = m.get("proxy_desc") or "None"
                if m.get("proxy_dynamic"):
                    proxy_label = f"{proxy_label} (dyn)"
                lines.append(
                    f"• {status} {p} | api:{m.get('api_id')} | dev:{m.get('device','')} | proxy:{proxy_label}{note}{note_extra}"
                )
            await ev.respond("\n".join(lines), buttons=account_control_menu())
        elif (
            cmd_base in {"/files_add", "/filesadd"}
            or (cmd_base == "/files" and len(parts) >= 2 and parts[1].lower() == "add")
        ):
            await ev.respond("Выбери тип файлов для сохранения:", buttons=files_add_menu())
        elif (
            cmd_base in {"/files_delete", "/filesdelete"}
            or (cmd_base == "/files" and len(parts) >= 2 and parts[1].lower() == "delete")
        ):
            await ev.respond("Выбери тип файлов для удаления:", buttons=files_delete_menu())
        elif cmd_base == "/files":
            await ev.respond(
                "Команда /files больше не используется. Выбери /files add или /files delete.",
                buttons=main_menu(),
            )
        elif cmd_base == "/grant":
            if not is_root_admin(admin_id):
                await ev.respond("Команда доступна только супер-администратору.")
                return
            if len(parts) < 2:
                await ev.respond("Использование: /grant <user_id> [root]")
                return
            try:
                new_id = int(parts[1])
            except ValueError:
                await ev.respond("ID должен быть числом.")
                return
            role = "root" if len(parts) >= 3 and parts[2].lower() == "root" else "user"
            ensure_tenant(new_id, role=role)
            await ev.respond(f"Доступ выдан пользователю {new_id}. Роль: {role}.")
            await safe_send_admin("Вам выдан доступ к менеджеру. Отправьте /start", owner_id=new_id)
        elif cmd_base == "/users":
            if not is_root_admin(admin_id):
                await ev.respond("Команда доступна только супер-администратору.")
                return
            await send_user_access_list(admin_id)
        elif cmd_base == "/revoke":
            if not is_root_admin(admin_id):
                await ev.respond("Команда доступна только супер-администратору.")
                return
            if len(parts) < 2:
                await ev.respond("Использование: /revoke <user_id>")
                return
            try:
                target_id = int(parts[1])
            except ValueError:
                await ev.respond("ID должен быть числом.")
                return
            if target_id in ROOT_ADMIN_IDS:
                await ev.respond("Нельзя удалить первоначального супер-администратора.")
                return
            await clear_owner_runtime(target_id)
            if remove_tenant(target_id):
                await ev.respond(f"Доступ пользователя {target_id} отключен.")
                await safe_send_admin("Ваш доступ к менеджеру отключен.", owner_id=target_id)
            else:
                await ev.respond("Пользователь не найден или уже удалён.") 
        else:
            await ev.respond("Неизвестная команда. Используй меню.")
        return

    edit_ctx = edit_waiting.get(admin_id)
    if edit_ctx:
        if not text:
            await ev.reply("Пустое сообщение. Пришли текст для исправления.")
            return
        edit_waiting.pop(admin_id, None)
        token = edit_ctx.get("token")
        info = outgoing_actions.get(token) if token else None
        if not info or info.get("admin_id") != admin_id:
            await ev.reply("Контекст редактирования устарел.")
            return
        worker = get_worker(admin_id, info.get("phone")) if info.get("phone") else None
        if not worker:
            await ev.reply("Аккаунт недоступен.")
            return
        try:
            await worker.edit_message(
                info["chat_id"],
                info["msg_id"],
                text,
                info.get("peer"),
            )
            await ev.reply("✅ Сообщение исправлено.")
        except Exception as e:
            await ev.reply(f"Ошибка редактирования: {e}")
        return

    waiting = reply_waiting.get(admin_id)
    if waiting:
        if not text:
            await ev.reply("Пустое сообщение. Пришли текст для отправки.")
            return
        reply_waiting.pop(admin_id, None)
        ctx_id = waiting.get("ctx")
        ctx = get_reply_context_for_admin(ctx_id, admin_id)
        if not ctx:
            await ev.reply("Контекст ответа устарел.")
            return
        worker = get_worker(admin_id, ctx["phone"])
        if not worker:
            await ev.reply("Аккаунт недоступен.")
            return
        mode = waiting.get("mode", "normal")
        reply_to_msg_id = ctx.get("msg_id") if mode == "reply" else None
        try:
            sent = await worker.send_outgoing(
                ctx["chat_id"],
                text,
                ctx.get("peer"),
                reply_to_msg_id=reply_to_msg_id,
                mark_read_msg_id=ctx.get("msg_id"),
            )
            token = register_outgoing_action(
                admin_id,
                phone=ctx["phone"],
                chat_id=ctx["chat_id"],
                peer=ctx.get("peer"),
                msg_id=_extract_message_id(sent),
                message_type="text",
            )
            buttons = (
                build_outgoing_control_buttons(token, allow_edit=True)
                if token
                else None
            )
            clear_notification_thread(
                admin_id, _make_thread_id(ctx["phone"], ctx["chat_id"])
            )
            await ev.reply("✅ Сообщение отправлено.", buttons=buttons)
        except Exception as e:
            await ev.reply(f"Ошибка отправки: {e}")
        return

    st = pending.get(admin_id)

    if st:
        flow = st.get("flow")
        if flow == "file":
            file_type = st.get("file_type")
            if st.get("step") == "name":
                if not text:
                    await ev.reply("Название не может быть пустым. Попробуйте снова.")
                    return
                filename = sanitize_filename(text, default=file_type or "file")
                pending[admin_id]["name"] = filename
                pending[admin_id]["step"] = "content"
                if file_type == "paste":
                    await ev.reply("Теперь пришлите текст пасты.")
                elif file_type == "voice":
                    await ev.reply(
                        "Пришлите голосовое сообщение или перешлите готовое."
                    )
                elif file_type == "video":
                    await ev.reply("Пришлите медиа (кружок, видео или фото в форматах jpg, jpeg, png).")
                elif file_type == "sticker":
                    await ev.reply("Пришлите стикер (поддерживаются .webp и .tgs).")
                else:
                    pending.pop(admin_id, None)
                    await ev.reply("Неизвестный тип файла. Операция отменена.")
                return

            if st.get("step") == "content":
                name = st.get("name") or sanitize_filename("file")
                if file_type == "paste":
                    if not text:
                        await ev.reply("Текст пасты не может быть пустым.")
                        return
                    file_path = os.path.join(user_library_dir(admin_id, "pastes"), f"{name}.txt")
                    try:
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(text)
                    except OSError as e:
                        await ev.reply(f"Не удалось сохранить пасту: {e}")
                        return
                    pending.pop(admin_id, None)
                    await ev.reply(f"✅ Паста сохранена как {os.path.basename(file_path)}")
                    return

                msg = ev.message
                if file_type == "voice":
                    if not getattr(msg, "voice", None):
                        await ev.reply(
                            "Ожидается голосовое сообщение (поддерживаются .ogg, .oga, .mp3)."
                        )
                        return
                    ext = ".ogg"
                    if msg.file and msg.file.ext:
                        ext = msg.file.ext
                    file_path = os.path.join(user_library_dir(admin_id, "voices"), f"{name}{ext}")
                    try:
                        await msg.download_media(file=file_path)
                    except Exception as e:
                        await ev.reply(f"Не удалось сохранить голосовое: {e}")
                        return
                    pending.pop(admin_id, None)
                    await ev.reply(f"✅ Голосовое сохранено как {os.path.basename(file_path)}")
                    return

                if file_type == "video":
                    if not (getattr(msg, "video_note", None) or getattr(msg, "video", None) or getattr(msg, "photo", None)):
                        await ev.reply("Ожидается медиа (кружок, видео или фото в форматах jpg, jpeg, png).")
                        return
                    # Определяем тип медиа
                    media_type = "video"  # По умолчанию обычное видео
                    if getattr(msg, "video_note", None):
                        media_type = "video_note"  # Кружок
                    elif getattr(msg, "photo", None):
                        media_type = "photo"  # Фото
                    ext = ".mp4"
                    if msg.file and msg.file.ext:
                        ext = msg.file.ext
                    elif getattr(msg, "photo", None):
                        ext = ".jpg"  # Для фото используем jpg по умолчанию
                    file_path = os.path.join(user_library_dir(admin_id, "video"), f"{name}{ext}")
                    try:
                        await msg.download_media(file=file_path)
                        # Сохраняем метаданные о типе медиа
                        _save_media_metadata(file_path, media_type)
                    except Exception as e:
                        await ev.reply(f"Не удалось сохранить медиа: {e}")
                        return
                    pending.pop(admin_id, None)
                    await ev.reply(f"✅ Медиа сохранено как {os.path.basename(file_path)}")
                    return

                if file_type == "sticker":
                    if not getattr(msg, "sticker", None):
                        await ev.reply("Ожидается стикер (форматы .webp, .tgs).")
                        return
                    ext = ".webp"
                    if msg.file and msg.file.ext:
                        ext = msg.file.ext
                    file_path = os.path.join(
                        user_library_dir(admin_id, "stickers"), f"{name}{ext}"
                    )
                    try:
                        await msg.download_media(file=file_path)
                    except Exception as e:
                        await ev.reply(f"Не удалось сохранить стикер: {e}")
                        return
                    pending.pop(admin_id, None)
                    await ev.reply(f"✅ Стикер сохранён как {os.path.basename(file_path)}")
                    return

                await ev.reply("Неизвестный тип файла. Операция отменена.")
                pending.pop(admin_id, None)
                return

        if flow == "proxy":
            if text.lower() in {"отмена", "cancel", "стоп", "stop"}:
                pending.pop(admin_id, None)
                await ev.reply("Настройка прокси отменена.")
                return
            step = st.get("step")
            data_store = st.setdefault("data", {})
            if step == "type":
                if not text:
                    await ev.reply("Пришли тип прокси (SOCKS5/SOCKS4/HTTP).")
                    return
                proxy_type = text.strip().upper()
                if proxy_type == "SOCKS":
                    proxy_type = "SOCKS5"
                if proxy_type not in {"SOCKS5", "SOCKS4", "HTTP"}:
                    await ev.reply("Некорректный тип. Доступно: SOCKS5, SOCKS4 или HTTP.")
                    return
                data_store["type"] = proxy_type
                pending[admin_id]["step"] = "host"
                await ev.reply("Пришли адрес прокси (домен или IP).")
                return
            if step == "host":
                host_value = text.strip()
                if not host_value:
                    await ev.reply("Адрес не может быть пустым. Попробуй ещё раз.")
                    return
                data_store["host"] = host_value
                pending[admin_id]["step"] = "port"
                await ev.reply("Укажи порт (1-65535).")
                return
            if step == "port":
                try:
                    port_value = int(text.strip())
                except (TypeError, ValueError):
                    await ev.reply("Порт должен быть числом.")
                    return
                if not (1 <= port_value <= 65535):
                    await ev.reply("Порт вне диапазона 1-65535. Пришли корректное значение.")
                    return
                data_store["port"] = port_value
                pending[admin_id]["step"] = "username"
                await ev.reply("Укажи логин прокси (или -, если не требуется).")
                return
            if step == "username":
                value = text.strip()
                if value and value not in {"-", "нет", "no", "none", "без"}:
                    data_store["username"] = value
                else:
                    data_store["username"] = None
                pending[admin_id]["step"] = "password"
                await ev.reply("Укажи пароль прокси (или -, если не требуется).")
                return
            if step == "password":
                value = text.strip()
                if value and value not in {"-", "нет", "no", "none", "без"}:
                    data_store["password"] = value
                else:
                    data_store["password"] = None
                pending[admin_id]["step"] = "dynamic"
                await ev.reply("Прокси динамический? (да/нет)")
                return
            if step == "dynamic":
                value = text.strip().lower()
                if value in {"да", "yes", "y", "true", "1", "+"}:
                    data_store["dynamic"] = True
                elif value in {"нет", "no", "n", "false", "0", "-"}:
                    data_store["dynamic"] = False
                else:
                    await ev.reply("Ответьте 'да' или 'нет'.")
                    return
                cfg = {
                    "enabled": True,
                    "type": data_store.get("type", "HTTP"),
                    "host": data_store.get("host"),
                    "port": data_store.get("port"),
                    "username": data_store.get("username"),
                    "password": data_store.get("password"),
                    "rdns": True,
                    "dynamic": bool(data_store.get("dynamic")),
                    "updated_at": int(datetime.now().timestamp()),
                }
                if not cfg.get("username"):
                    cfg.pop("username", None)
                if not cfg.get("password"):
                    cfg.pop("password", None)
                set_tenant_proxy_config(admin_id, cfg)
                pending.pop(admin_id, None)
                restarted, errors = await apply_proxy_config_to_owner(admin_id, restart_active=True)
                response_lines = ["✅ Прокси сохранён.", "", format_proxy_settings(admin_id)]
                if restarted:
                    response_lines.append(f"Перезапущено активных аккаунтов: {restarted}.")
                if errors:
                    response_lines.append("⚠️ Ошибки при обновлении: " + "; ".join(errors))
                await ev.reply("\n".join(response_lines))
                await bot_client.send_message(
                    admin_id,
                    "Готово. Управление прокси:",
                    buttons=proxy_menu_buttons(admin_id),
                )
                return

        if flow == "account":
            step = st.get("step")
            lowered = text.lower()
            cancel_words = {"отмена", "cancel", "стоп", "stop"}
            no_proxy_words = {"без прокси", "без", "no proxy", "безпрокси"}

            forced_phone_value: Optional[str] = None
            if step in {"proxy_choice", "proxy_manual", "proxy_or_phone"}:
                phone_candidate = extract_phone_number(text)
                if phone_candidate:
                    if "proxy_config" not in st:
                        st["proxy_config"] = {"enabled": False}
                    st["step"] = "phone"
                    forced_phone_value = phone_candidate
                else:
                    if lowered in cancel_words:
                        pending.pop(admin_id, None)
                        await ev.reply("Добавление аккаунта отменено.")
                        return
                    if lowered in no_proxy_words:
                        st["proxy_config"] = {"enabled": False}
                        st["step"] = "phone"
                        await ev.reply(
                            "Подключение будет без прокси. Пришли номер телефона (+7XXXXXXXXXX)"
                        )
                        return
                    try:
                        cfg = parse_proxy_input(text)
                    except ValueError as parse_error:
                        await ev.reply(f"Некорректный формат прокси: {parse_error}.")
                        return
                    cfg.setdefault("dynamic", False)
                    st["proxy_config"] = cfg
                    st["step"] = "phone"
                    try:
                        store_user_proxy_config(admin_id, cfg)
                    except Exception as save_error:
                        log.warning("[%s] cannot store proxy config: %s", admin_id, save_error)
                    await ev.reply("Прокси сохранён. Пришли номер телефона (+7XXXXXXXXXX)")
                    return

            step = st.get("step")

            if step == "phone":
                phone = forced_phone_value or extract_phone_number(text)
                if not phone:
                    await ev.reply("Неверный формат. Пример: +7XXXXXXXXXX")
                    return
                if not API_KEYS:
                    await ev.reply("Добавь API_KEYS в конфиг.")
                    pending.pop(admin_id, None)
                    return

                api = API_KEYS[next_index(admin_id, "api_idx", len(API_KEYS))]
                dev = (
                    DEVICE_PROFILES[next_index(admin_id, "dev_idx", len(DEVICE_PROFILES))]
                    if DEVICE_PROFILES
                    else {}
                )

                sess = None
                existing_meta = get_account_meta(admin_id, phone)
                if existing_meta and os.path.exists(existing_meta.get("session_file", "")):
                    with open(existing_meta["session_file"], "r", encoding="utf-8") as fh:
                        sess = fh.read().strip() or None

                proxy_cfg = st.get("proxy_config")

                meta = ensure_account_meta(admin_id, phone)
                meta.update(
                    {
                        "phone": phone,
                        "api_id": api["api_id"],
                        "device": dev.get("device_model", ""),
                        "session_file": user_session_path(admin_id, phone),
                    }
                )
                if proxy_cfg is not None:
                    meta["proxy_override"] = dict(proxy_cfg)
                else:
                    meta.pop("proxy_override", None)
                persist_tenants()

                w = AccountWorker(admin_id, phone, api["api_id"], api["api_hash"], dev, sess)
                extra_lines: List[str] = []
                try:
                    await w.send_code()
                except Exception as send_err:
                    if proxy_cfg and proxy_cfg.get("enabled", True):
                        extra_lines.append(
                            f"⚠️ Не удалось подключиться через указанный прокси: {send_err}."
                            " Пробую напрямую."
                        )
                        meta.pop("proxy_override", None)
                        persist_tenants()
                        w = AccountWorker(admin_id, phone, api["api_id"], api["api_hash"], dev, sess)
                        try:
                            await w.send_code()
                        except Exception as direct_err:
                            pending.pop(admin_id, None)
                            await ev.reply(f"Не удалось отправить код: {direct_err}")
                            return
                    else:
                        pending.pop(admin_id, None)
                        await ev.reply(f"Не удалось отправить код: {send_err}")
                        return

                meta["proxy_dynamic"] = w.using_dynamic_proxy
                meta["proxy_desc"] = w.proxy_description
                persist_tenants()

                delivery_hint = w.code_delivery_hint
                hint_lines: List[str] = []
                if delivery_hint == "sms_forced":
                    hint_lines.append(
                        "⚠️ Если код уже пришёл в приложение Telegram — он больше не действителен."
                        " Дождись SMS и введи код из SMS."
                    )
                elif delivery_hint == "sms":
                    hint_lines.append("ℹ️ Код отправлен по SMS. Обычно он приходит в течение пары минут.")
                elif delivery_hint == "app":
                    hint_lines.append(
                        "ℹ️ Код отправлен в Telegram. Если удобнее получить SMS, нажми Отмена и попробуй ещё раз."
                    )
                elif delivery_hint in {"call", "flash_call", "missed_call"}:
                    hint_lines.append(
                        "ℹ️ Код поступит звонком. Ответь и запомни названные цифры."
                    )
                elif delivery_hint == "email":
                    hint_lines.append("ℹ️ Код придёт на e-mail, привязанный к аккаунту.")

                response_lines = extra_lines + [f"Код отправлен на {phone}. Пришли код."] + hint_lines
                pending[admin_id] = {
                    "flow": "account",
                    "step": "code",
                    "phone": phone,
                    "worker": w,
                }
                await ev.reply("\n".join(response_lines))
                return

            if step == "code":
                code = text
                w: AccountWorker = st["worker"]
                phone = st.get("phone", "")
                try:
                    await w.sign_in_code(code)
                except SessionPasswordNeededError:
                    pending[admin_id] = {
                        "flow": "account",
                        "step": "2fa",
                        "phone": phone,
                        "worker": w,
                    }
                    await ev.reply("Включена двухэтапная защита. Пришли пароль 2FA для аккаунта.")
                    return
                except Exception as e:
                    await ev.reply(f"Ошибка входа: {e}")
                    pending.pop(admin_id, None)
                    return
                register_worker(admin_id, phone, w)
                try:
                    await w.start()
                except AuthKeyDuplicatedError:
                    pending.pop(admin_id, None)
                    await ev.reply(
                        "Сессия была аннулирована Telegram из-за одновременного входа с разных IP."
                        " Попробуй ещё раз через несколько минут."
                    )
                    return
                pending.pop(admin_id, None)
                await ev.reply(f"✅ {phone} добавлен. Слушаю входящие.")
                return

            if step == "2fa":
                pwd = text
                w: AccountWorker = st["worker"]
                phone = st.get("phone", "")
                try:
                    await w.sign_in_2fa(pwd)
                except Exception as e:
                    await ev.reply(f"2FA ошибка: {e}")
                    pending.pop(admin_id, None)
                    return
                register_worker(admin_id, phone, w)
                try:
                    await w.start()
                except AuthKeyDuplicatedError:
                    pending.pop(admin_id, None)
                    await ev.reply(
                        "Сессия была аннулирована Telegram из-за одновременного входа с разных IP."
                        " Попробуй ещё раз через несколько минут."
                    )
                    return
                pending.pop(admin_id, None)
                await ev.reply(f"✅ {phone} добавлен (2FA). Слушаю входящие.")
                return

            await ev.reply("Неизвестный шаг добавления аккаунта. Операция отменена.")
            pending.pop(admin_id, None)
            return

# ---- startup ----
async def startup():
    await bot_client.start(bot_token=BOT_TOKEN)
    global BOT_USERNAME
    try:
        me = await bot_client.get_me()
    except Exception as err:
        BOT_USERNAME = None
        log.warning("Не удалось получить имя пользователя бота: %s", err)
    else:
        username = getattr(me, "username", None)
        BOT_USERNAME = username or None
    try:
        await bot_client(
            functions.bots.SetBotCommandsRequest(
                scope=types.BotCommandScopeDefault(),
                lang_code="",
                commands=BOT_COMMANDS,
            )
        )
    except Exception as err:
        log.warning("Не удалось обновить меню команд: %s", err)
    log.info("Bot started. Restore workers...")
    for owner_key, tenant_data in tenants.items():
        try:
            owner_id = int(owner_key)
        except (TypeError, ValueError):
            continue
        accounts = tenant_data.get("accounts", {}) if isinstance(tenant_data, dict) else {}
        for phone, meta in accounts.items():
            sess = None
            session_path = meta.get("session_file") or user_session_path(owner_id, phone)
            if session_path and os.path.exists(session_path):
                try:
                    with open(session_path, "r", encoding="utf-8") as fh:
                        sess = fh.read().strip() or None
                except OSError:
                    sess = None
            api_id = meta.get("api_id")
            try:
                api_id = int(api_id)
            except (TypeError, ValueError):
                api_id = API_KEYS[0]["api_id"] if API_KEYS else 0
            api_hash = None
            for k in API_KEYS:
                if k["api_id"] == api_id:
                    api_hash = k["api_hash"]
                    break
            if not api_hash and API_KEYS:
                api_hash = API_KEYS[0]["api_hash"]
            dev = next((d for d in DEVICE_PROFILES if d.get("device_model") == meta.get("device")), None) or (DEVICE_PROFILES[0] if DEVICE_PROFILES else {})
            w = AccountWorker(owner_id, phone, api_id, api_hash, dev, sess)
            register_worker(owner_id, phone, w)
            try:
                await w.start()
            except AuthKeyDuplicatedError:
                log.warning("Worker %s session invalid; waiting for re-login.", phone)
            except Exception as e:
                log.warning("Worker %s not started yet: %s", phone, e)
    log.info("Startup notification suppressed to avoid spamming users.")

def main():
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.run_until_complete(startup())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        for owner_workers in list(WORKERS.values()):
            for w in owner_workers.values():
                try:
                    loop.run_until_complete(w.stop())
                except Exception:
                    pass
        try: loop.run_until_complete(bot_client.disconnect())
        except: pass

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Exception:
        import traceback
        traceback.print_exc()
        print("\nОШИБКА! Смотри трейс выше и файл bot.log.")
        input("Нажми Enter, чтобы закрыть окно...")
    else:
        print("\nГотово. Нажми Enter, чтобы закрыть окно...")
        input()
