import asyncio
import contextlib
import os
import json
import logging
import sys
import random
import secrets
import html
import re
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional, Any, List, Tuple, Set
from io import BytesIO
from telethon import TelegramClient, events, Button
from telethon.utils import get_display_name
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
)
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
from telethon.tl.types import PeerUser, User

import socks  # PySocks

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

BOT_TOKEN = "8377353888:AAFj_l3l1XAie5RA8PMwxD1gXtb2eEDOdJw"   # токен бота от @BotFather
# Изначальные супер-администраторы, которые могут выдавать доступ другим пользователям
ROOT_ADMIN_IDS = {8099997426, 7519364639}

# ДИНАМИЧЕСКИЙ ПРОКСИ: одна точка, новый IP выдаётся провайдером при новом соединении
# Если боту прокси не нужен — enabled=False
DYNAMIC_PROXY = {
    "enabled": True,
    "type": "HTTP",  # "HTTP" или "SOCKS5"
    "host": "185.162.130.86",
    "port": 10000,
    "username": "cILkIEh3louyuDuw7tlK",
    "password": "IhsbIca9567aZ9yUZBs7bglTE6e1V8as",
    "rdns": True,
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
TEXT_EXTENSIONS = {".txt", ".md"}
VOICE_EXTENSIONS = {".ogg"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".webm"}
for _dir in (LIBRARY_DIR, PASTES_DIR, VOICES_DIR, VIDEO_DIR):
    os.makedirs(_dir, exist_ok=True)
ASSET_TITLE_MAX = 32
ACCOUNTS_META = "accounts.json"
ROTATION_STATE = ".rotation_state.json"
TENANTS_DB = "tenants.json"


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
            return json.load(open(path, "r", encoding="utf-8"))
        except Exception:
            return default
    return default


tenants: Dict[str, Dict[str, Any]] = _ensure_dict(_load(TENANTS_DB, {}))
_tenants_initially_empty = not tenants


def persist_tenants() -> None:
    _save(tenants, TENANTS_DB)


def tenant_key(user_id: int) -> str:
    return str(int(user_id))


def ensure_user_dirs(user_id: int) -> None:
    base = os.path.join(LIBRARY_DIR, str(user_id))
    os.makedirs(base, exist_ok=True)
    for sub in ("pastes", "voices", "video"):
        os.makedirs(os.path.join(base, sub), exist_ok=True)
    os.makedirs(os.path.join(SESSIONS_DIR, str(user_id)), exist_ok=True)


def user_library_dir(user_id: int, kind: str) -> str:
    ensure_user_dirs(user_id)
    return os.path.join(LIBRARY_DIR, str(user_id), kind)


def user_sessions_dir(user_id: int) -> str:
    ensure_user_dirs(user_id)
    return os.path.join(SESSIONS_DIR, str(user_id))


def user_session_path(user_id: int, phone: str) -> str:
    return os.path.join(user_sessions_dir(user_id), f"{phone}.session")


def ensure_tenant(user_id: int, *, role: str = "user") -> Dict[str, Any]:
    key = tenant_key(user_id)
    data = tenants.setdefault(key, {})
    if data.get("role") not in {"root", "user"}:
        data["role"] = role
    elif role == "root" and data.get("role") != "root":
        data["role"] = "root"
    data.setdefault("accounts", {})
    data.setdefault("rotation_state", {})
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


async def clear_owner_runtime(owner_id: int) -> None:
    owner_workers = WORKERS.pop(owner_id, {})
    for worker in owner_workers.values():
        with contextlib.suppress(Exception):
            await worker.logout()
    pending.pop(owner_id, None)
    reply_waiting.pop(owner_id, None)
    menu_keyboard_shown.discard(owner_id)
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


def all_admin_ids() -> Set[int]:
    ids: Set[int] = set()
    for key in tenants.keys():
        try:
            ids.add(int(key))
        except (TypeError, ValueError):
            continue
    return ids


def is_root_admin(user_id: int) -> bool:
    data = tenants.get(tenant_key(user_id))
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
TYPING_CHAR_SPEED = (7.0, 14.0)  # символов в секунду
TYPING_DURATION_LIMITS = (0.6, 4.0)  # минимальная и максимальная продолжительность «печати»
TYPING_DURATION_VARIANCE = (0.85, 1.2)
VOICE_RECORD_DURATION = (2.0, 4.0)  # секунд имитации записи голосового
VIDEO_NOTE_RECORD_DURATION = (2.0, 4.0)  # секунд имитации записи кружка
CHAT_ACTION_REFRESH = 4.5  # секунды между повторными действиями, если требуется
# ============================================

def _rand_delay(span: Tuple[int, int]) -> float:
    low, high = span
    if low >= high:
        return float(low)
    return random.uniform(low, high)

def _typing_duration(message: str) -> float:
    message = message or ""
    variance = random.uniform(*TYPING_DURATION_VARIANCE)
    if message:
        speed = random.uniform(*TYPING_CHAR_SPEED)
        estimated = len(message) / max(speed, 1e-3)
        duration = estimated * variance
    else:
        low, high = TYPING_DURATION_LIMITS
        duration = random.uniform(low, min(high, low + 0.5)) * variance
    low, high = TYPING_DURATION_LIMITS
    return max(low, min(duration, high))

def _list_files(directory: str, allowed_ext: Set[str]) -> List[str]:
    if not os.path.isdir(directory):
        return []
    files: List[str] = []
    for name in sorted(os.listdir(directory)):
        full = os.path.join(directory, name)
        if not os.path.isfile(full):
            continue
        ext = os.path.splitext(name)[1].lower()
        if allowed_ext and ext not in allowed_ext:
            continue
        files.append(full)
    return files


def sanitize_filename(name: str, default: str = "file") -> str:
    """Convert arbitrary text to a safe filename."""
    cleaned = re.sub(r"[^\w\s.-]", "", name, flags=re.UNICODE).strip()
    cleaned = re.sub(r"\s+", "_", cleaned)
    cleaned = cleaned[:64]
    return cleaned or default


def list_text_templates(owner_id: int) -> List[str]:
    return _list_files(user_library_dir(owner_id, "pastes"), TEXT_EXTENSIONS)


def list_voice_templates(owner_id: int) -> List[str]:
    return _list_files(user_library_dir(owner_id, "voices"), VOICE_EXTENSIONS)


def list_video_templates(owner_id: int) -> List[str]:
    return _list_files(user_library_dir(owner_id, "video"), VIDEO_EXTENSIONS)


def build_asset_keyboard(
    files: List[str],
    prefix: str,
    ctx: str,
    mode: Optional[str] = None,
) -> List[List[Button]]:
    rows: List[List[Button]] = []
    for idx, path in enumerate(files):
        base = os.path.splitext(os.path.basename(path))[0]
        title = base if len(base) <= ASSET_TITLE_MAX else base[: ASSET_TITLE_MAX - 1] + "…"
        payload = f"{prefix}:{ctx}:{idx}" if mode is None else f"{prefix}:{ctx}:{mode}:{idx}"
        rows.append([Button.inline(title, payload.encode())])
    rows.append([Button.inline("⬅️ Закрыть", b"asset_close")])
    return rows


def build_reply_options_keyboard(ctx: str, mode: str) -> List[List[Button]]:
    return [
        [
            Button.inline("📄 Пасты", f"reply_paste_menu:{ctx}:{mode}".encode()),
            Button.inline("🎙 Голосовые", f"reply_voice_menu:{ctx}:{mode}".encode()),
        ],
        [
            Button.inline("📹 Кружки", f"reply_video_menu:{ctx}:{mode}".encode()),
        ],
        [Button.inline("❌ Отмена", f"reply_cancel:{ctx}".encode())],
    ]

def next_index(owner_id: int, key: str, length: int) -> int:
    rotation_state = get_rotation_state(owner_id)
    cur = rotation_state.get(key, -1)
    cur = (cur + 1) % max(1, length)
    rotation_state[key] = cur
    persist_tenants()
    return cur

# ---- bot client ----
# Используем первую пару API_KEYS для бота
bot_client = TelegramClient(
    StringSession(),
    API_KEYS[0]["api_id"],
    API_KEYS[0]["api_hash"]
)

# безопасная отправка админу (не падаем, если админ ещё не нажал /start)
def is_admin(user_id: int) -> bool:
    return tenant_key(user_id) in tenants


async def safe_send_admin(text: str, *, owner_id: Optional[int] = None, **kwargs):
    targets = {owner_id} if owner_id is not None else all_admin_ids()
    for admin_id in targets:
        try:
            await bot_client.send_message(admin_id, text, **kwargs)
        except Exception as e:
            logging.getLogger("mgrbot").warning(
                "Cannot DM admin %s yet (probably admin hasn't started the bot): %s",
                admin_id,
                e,
            )
            continue


async def safe_send_admin_file(file_data: bytes, filename: str, *, owner_id: Optional[int] = None, **kwargs) -> None:
    if not file_data:
        return
    targets = {owner_id} if owner_id is not None else all_admin_ids()
    for admin_id in targets:
        try:
            bio = BytesIO(file_data)
            bio.name = filename
            await bot_client.send_file(admin_id, bio, **kwargs)
        except Exception as e:
            logging.getLogger("mgrbot").warning(
                "Cannot send file to admin %s yet (probably admin hasn't started the bot): %s",
                admin_id,
                e,
            )
            continue

# ---- dynamic proxy tuple ----
def _proxy_tuple_from_config(config: Optional[Dict[str, Any]], *, context: str = "dynamic") -> Optional[Tuple]:
    if not config:
        return None
    if not bool(config.get("enabled", True)):
        return None
    host = config.get("host")
    port = config.get("port")
    if not host or port is None:
        log.warning("Proxy config %s is missing host/port: %s", context, config)
        return None
    try:
        port_int = int(port)
    except (TypeError, ValueError):
        log.warning("Proxy config %s has invalid port %r", context, port)
        return None
    proxy_type = str(config.get("type", "HTTP")).upper()
    if proxy_type in {"SOCKS", "SOCKS5"}:
        proxy_const = socks.SOCKS5
    elif proxy_type == "SOCKS4":
        proxy_const = socks.SOCKS4
    else:
        proxy_const = socks.HTTP
    rdns = bool(config.get("rdns", True))
    username = config.get("username")
    password = config.get("password")
    return (proxy_const, host, port_int, rdns, username, password)


def build_dynamic_proxy_tuple() -> Optional[Tuple]:
    return _proxy_tuple_from_config(DYNAMIC_PROXY, context="dynamic")

def proxy_desc(p: Optional[Tuple]) -> str:
    if not p: return "None"
    tp, host, port, *_ = p
    name = {socks.SOCKS5:"SOCKS5", socks.SOCKS4:"SOCKS4", socks.HTTP:"HTTP"}.get(tp, str(tp))
    return f"{name}://{host}:{port}"

# ---- worker ----
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

    def _select_proxy(self, *, force_new: bool = False) -> Optional[Tuple]:
        meta = get_account_meta(self.owner_id, self.phone) or {}
        raw_override = meta.get("proxy_override")
        override_signature = "__none__"
        override_cfg: Optional[Dict[str, Any]] = None
        if raw_override is None:
            override_signature = "__none__"
        elif isinstance(raw_override, dict):
            try:
                override_signature = json.dumps(raw_override, sort_keys=True, ensure_ascii=False, default=str)
            except (TypeError, ValueError):
                override_signature = str(raw_override)
            override_cfg = raw_override
        else:
            override_signature = "__invalid_type__"
            if self._proxy_override_signature != override_signature:
                log.warning(
                    "[%s] proxy_override must be a mapping, got %r. Игнорирую переопределение.",
                    self.phone,
                    type(raw_override).__name__,
                )

        need_refresh = force_new or self._proxy_tuple is None or override_signature != self._proxy_override_signature
        if not need_refresh:
            return self._proxy_tuple

        proxy_tuple: Optional[Tuple] = None
        is_dynamic = False

        if override_cfg is not None:
            if not bool(override_cfg.get("enabled", True)):
                proxy_tuple = None
            else:
                proxy_tuple = _proxy_tuple_from_config(override_cfg, context=f"account:{self.phone}")
                if proxy_tuple is None and self._proxy_override_signature != override_signature:
                    log.warning(
                        "[%s] proxy_override указано, но конфигурация некорректна. Пытаюсь использовать динамический прокси.",
                        self.phone,
                    )

        if proxy_tuple is None and (override_cfg is None or bool(override_cfg.get("enabled", True))):
            dynamic_tuple = build_dynamic_proxy_tuple()
            if dynamic_tuple is not None:
                proxy_tuple = dynamic_tuple
                is_dynamic = True

        if proxy_tuple is None and override_cfg is not None and bool(override_cfg.get("enabled", True)) and self._proxy_override_signature != override_signature:
            log.warning(
                "[%s] proxy_override не удалось применить и динамический прокси отключён. Подключение пойдёт без прокси.",
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

    def _make_client(self) -> TelegramClient:
        proxy_cfg = self._select_proxy()
        return TelegramClient(
            self.session, self.api_id, self.api_hash,
            proxy=proxy_cfg,
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

    async def _simulate_voice_recording(self, client: TelegramClient, peer: Any) -> None:
        if VOICE_RECORD_DURATION[0] < VOICE_RECORD_DURATION[1]:
            duration = random.uniform(*VOICE_RECORD_DURATION)
        else:
            duration = float(VOICE_RECORD_DURATION[0])
        await self._simulate_chat_action(client, peer, "record-audio", duration)

    async def _simulate_round_recording(self, client: TelegramClient, peer: Any) -> None:
        if VIDEO_NOTE_RECORD_DURATION[0] < VIDEO_NOTE_RECORD_DURATION[1]:
            duration = random.uniform(*VIDEO_NOTE_RECORD_DURATION)
        else:
            duration = float(VIDEO_NOTE_RECORD_DURATION[0])
        await self._simulate_chat_action(client, peer, "record-round", duration)

    async def _ensure_client(self) -> TelegramClient:
        if not self.client:
            self.client = self._make_client()
        if not self.client.is_connected():
            await self.client.connect()
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
                txt = ev.raw_text or "<media>"
                ctx_id = secrets.token_hex(4)
                peer = None
                try:
                    peer = await ev.get_input_chat()
                except Exception:
                    try:
                        peer = await ev.get_input_sender()
                    except Exception:
                        peer = None
                account_meta = get_account_meta(self.owner_id, self.phone) or {}
                account_display = self.account_name or account_meta.get("full_name")
                if not account_display:
                    account_display = self.phone
                sender_entity = None
                with contextlib.suppress(Exception):
                    sender_entity = await ev.get_sender()
                sender_name = get_display_name(sender_entity) if sender_entity else None
                sender_username = getattr(sender_entity, "username", None) if sender_entity else None
                sender_tag = f"@{sender_username}" if sender_username else (
                    f"ID: {ev.sender_id}" if ev.sender_id else "ID: unknown"
                )            
                avatar_bytes: Optional[bytes] = None
                if sender_entity:
                    try:
                        buffer = BytesIO()
                        result = await self.client.download_profile_photo(sender_entity, file=buffer)
                        if isinstance(result, BytesIO):
                            avatar_bytes = result.getvalue()
                        elif isinstance(result, bytes):
                            avatar_bytes = result
                        else:
                            data = buffer.getvalue()
                            avatar_bytes = data if data else None
                    except Exception:
                        avatar_bytes = None

                if sender_username:
                    profile_url = f"https://t.me/{sender_username}"
                elif ev.sender_id:
                    profile_url = f"tg://user?id={ev.sender_id}"
                else:
                    profile_url = None

                if sender_name:
                    link_label = sender_name
                elif sender_username:
                    link_label = f"@{sender_username}"
                else:
                    link_label = sender_tag

                forward_anchor = None
                forward_header = getattr(ev, "forward", None) or getattr(ev, "fwd_from", None)
                if forward_header:
                    forward_label: Optional[str] = None
                    forward_profile_url: Optional[str] = None
                    forward_username: Optional[str] = None
                    forward_id: Optional[int] = None
                    forward_entity = None
                    from_peer = getattr(forward_header, "from_id", None)
                    if from_peer:
                        with contextlib.suppress(Exception):
                            forward_entity = await self.client.get_entity(from_peer)
                    if forward_entity:
                        forward_label = get_display_name(forward_entity) or None
                        forward_username = getattr(forward_entity, "username", None)
                        if isinstance(forward_entity, User):
                            forward_id = getattr(forward_entity, "id", None)
                    if isinstance(from_peer, PeerUser) and not forward_id:
                        forward_id = from_peer.user_id
                    if not forward_label:
                        forward_label = getattr(forward_header, "from_name", None)
                    if not forward_username and forward_entity:
                        forward_username = getattr(forward_entity, "username", None)
                    if forward_username:
                        forward_profile_url = f"https://t.me/{forward_username}"
                    elif forward_id:
                        forward_profile_url = f"tg://user?id={forward_id}"
                    if not forward_label:
                        if forward_username:
                            forward_label = f"@{forward_username}"
                        elif forward_id:
                            forward_label = f"ID: {forward_id}"
                    if forward_label:
                        if forward_profile_url:
                            forward_anchor = (
                                f"<a href=\"{html.escape(forward_profile_url)}\">{html.escape(forward_label)}</a>"
                            )
                        else:
                            forward_anchor = html.escape(forward_label)
                if not forward_anchor:
                    if profile_url:
                        forward_anchor = f"<a href=\"{html.escape(profile_url)}\">{html.escape(link_label)}</a>"
                    else:
                        forward_anchor = html.escape(link_label)

                info_lines = [
                    f"👤 Аккаунт: <b>{html.escape(account_display)}</b>",
                    f"👥 Собеседник: <b>{html.escape(sender_name) if sender_name else '—'}</b>",
                    f"🔗 {html.escape(sender_tag)}",
                ]
                if forward_anchor:
                    info_lines.extend(["", f"Forwarded from {forward_anchor}"])
                if txt:
                    escaped_txt = html.escape(txt)
                    info_lines.extend([
                        "",
                        "💬 <b>Сообщение:</b>",
                        escaped_txt,
                    ])
                info_caption = "\n".join(info_lines)

                reply_contexts[ctx_id] = {
                    "owner_id": self.owner_id,
                    "phone": self.phone,
                    "chat_id": ev.chat_id,
                    "sender_id": ev.sender_id,
                    "peer": peer,
                    "msg_id": ev.id,
                }
                buttons: List[List[Button]] = [
                    [
                        Button.inline("✉️ Ответить", f"reply:{ctx_id}".encode()),
                        Button.inline("↩️ Реплай", f"reply_to:{ctx_id}".encode()),
                        Button.inline("📹 Кружок", f"video_menu:{ctx_id}".encode()),
                    ],
                    [
                        Button.inline("📄 Пасты", f"paste_menu:{ctx_id}".encode()),
                        Button.inline("🎙 Голосовые", f"voice_menu:{ctx_id}".encode()),
                    ],
                ]
                if profile_url:
                    buttons.append([Button.url("🔗 Открыть профиль", profile_url)])

                if avatar_bytes:
                    await safe_send_admin_file(
                        avatar_bytes,
                        filename=f"avatar_{ev.sender_id or 'unknown'}.jpg",
                        caption=info_caption,
                        buttons=buttons,
                        parse_mode="html",
                        owner_id=self.owner_id,
                    )
                else:
                    await safe_send_admin(
                        info_caption,
                        buttons=buttons,
                        parse_mode="html",
                        link_preview=False,
                        owner_id=self.owner_id,
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
        if self.client:
            try: await self.client.disconnect()
            except: pass
        self.started = False

    async def send_code(self):
        await self._ensure_client()
        await asyncio.sleep(_rand_delay(LOGIN_DELAY_SECONDS))
        try:
            return await self.client.send_code_request(self.phone)
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
            await client.send_message(peer, message, reply_to=reply_to_msg_id)
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")

    async def send_voice(
        self,
        chat_id: int,
        file_path: str,
        peer: Optional[Any] = None,
        reply_to_msg_id: Optional[int] = None,
    ):
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        await self._simulate_voice_recording(client, peer)
        try:
            await client.send_file(
                peer,
                file_path,
                voice_note=True,
                reply_to=reply_to_msg_id,
            )
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")
        
    async def send_video_note(
        self,
        chat_id: int,
        file_path: str,
        peer: Optional[Any] = None,
        reply_to_msg_id: Optional[int] = None,
    ):
        client = await self._ensure_client()
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт не авторизован")
        if peer is None:
            try:
                peer = await client.get_input_entity(chat_id)
            except Exception:
                peer = chat_id
        await self._simulate_round_recording(client, peer)
        try:
            await client.send_file(
                peer,
                file_path,
                video_note=True,
                reply_to=reply_to_msg_id,
            )
        except (UserDeactivatedBanError, PhoneNumberBannedError) as e:
            await self._handle_account_disabled("banned", e)
            raise RuntimeError("Аккаунт заблокирован Telegram")
        except UserDeactivatedError as e:
            await self._handle_account_disabled("frozen", e)
            raise RuntimeError("Аккаунт заморожен Telegram")

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
MENU_BUTTON_TEXT = "MENU"
menu_keyboard_shown: Set[int] = set()


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


def get_reply_context_for_admin(ctx_id: str, admin_id: int) -> Optional[Dict[str, Any]]:
    ctx = reply_contexts.get(ctx_id)
    if not ctx:
        return None
    if ctx.get("owner_id") != admin_id:
        return None
    return ctx

async def cancel_operations(admin_id: int, notify: bool = True) -> bool:
    """Сбрасывает незавершённые операции для конкретного админа."""
    cancelled = False
    if reply_waiting.pop(admin_id, None) is not None:
        cancelled = True
    if pending.pop(admin_id, None) is not None:
        cancelled = True
    if cancelled and notify:
        await bot_client.send_message(admin_id, "❌ Текущая операция отменена.")
    return cancelled

def menu_keyboard() -> List[List[Button]]:
    return [[Button.text(MENU_BUTTON_TEXT, resize=True)]]

async def ensure_menu_keyboard(admin_id: int) -> None:
    if admin_id in menu_keyboard_shown:
        return
    try:
        await bot_client.send_message(
            admin_id,
            "⌨️ Чтобы вернуться в главное меню, нажимай кнопку MENU слева от скрепки.",
            buttons=menu_keyboard(),
        )
        menu_keyboard_shown.add(admin_id)
    except Exception as e:
        log.warning("Cannot show MENU keyboard to %s: %s", admin_id, e)

def main_menu():
    return [
        [Button.inline("➕ Добавить аккаунт", b"add")],
        [Button.inline("📋 Список аккаунтов", b"list")],
        [Button.inline("📁 Файлы", b"files")],
        [Button.inline("🧪 Ping", b"ping")],
    ]


def files_menu() -> List[List[Button]]:
    return [
        [Button.inline("📄 Пасты", b"files_paste")],
        [Button.inline("🎙 Голосовые", b"files_voice")],
        [Button.inline("📹 Кружки", b"files_video")],
        [Button.inline("⬅️ Назад", b"back")],
    ]

def account_control_menu():
    return [
        [Button.inline("🗑 Удалить аккаунт", b"del_select")],
        [Button.inline("✅ Валидация", b"val_select")],
        [Button.inline("⬅️ Назад", b"back")],
    ]

def build_account_buttons(owner_id: int, prefix: str) -> List[List[Button]]:
    rows: List[List[Button]] = []
    for phone in list(get_accounts_meta(owner_id).keys()):
        rows.append([Button.inline(phone, f"{prefix}:{phone}".encode())])
    rows.append([Button.inline("⬅️ Назад", b"list")])
    return rows

@bot_client.on(events.NewMessage(pattern="/start"))
async def on_start(ev):
    if not is_admin(ev.sender_id):
        await ev.respond("Доступ запрещён."); return
    await cancel_operations(ev.sender_id, notify=False)
    await ev.respond("Менеджер запущен. Выбери действие:", buttons=main_menu())
    await ensure_menu_keyboard(ev.sender_id)

@bot_client.on(events.CallbackQuery)
async def on_cb(ev):
    if not is_admin(ev.sender_id):
        await ev.answer("Недоступно", alert=True); return
    data = ev.data.decode() if isinstance(ev.data, (bytes, bytearray)) else str(ev.data)
    admin_id = ev.sender_id

    notify_cancel = not data.startswith(("reply",))
    await cancel_operations(admin_id, notify=notify_cancel)
    await ensure_menu_keyboard(admin_id)

    if data == "files":
        await ev.answer()
        await bot_client.send_message(
            admin_id,
            "Выбери тип файлов для сохранения:",
            buttons=files_menu(),
        )
        return

    if data == "files_paste":
        pending[admin_id] = {"flow": "file", "file_type": "paste", "step": "name"}
        await ev.answer()
        await bot_client.send_message(admin_id, "Введите название пасты:")
        return

    if data == "files_voice":
        pending[admin_id] = {"flow": "file", "file_type": "voice", "step": "name"}
        await ev.answer()
        await bot_client.send_message(admin_id, "Введите название голосового:")
        return

    if data == "files_video":
        pending[admin_id] = {"flow": "file", "file_type": "video", "step": "name"}
        await ev.answer()
        await bot_client.send_message(admin_id, "Введите название кружка:")
        return

    if data == "add":
        pending[admin_id] = {"step":"phone"}
        await ev.answer(); await bot_client.send_message(admin_id, "Пришли номер телефона (+7XXXXXXXXXX)")
        return

    if data == "list":
        accounts = get_accounts_meta(admin_id)
        if not accounts:
            await ev.answer("Пусто", alert=True); await bot_client.send_message(admin_id, "Аккаунтов нет."); return
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
            lines.append(
                f"• {status} {p} | api:{m.get('api_id')} | dev:{m.get('device','')}{note}{note_extra}"
            )
        await ev.answer()
        await bot_client.send_message(admin_id, "\n".join(lines), buttons=account_control_menu())
        return

    if data == "back":
        await ev.answer()
        await bot_client.send_message(admin_id, "Главное меню", buttons=main_menu())
        return

    if data == "del_select":
        if not get_accounts_meta(admin_id):
            await ev.answer("Нет аккаунтов", alert=True); return
        await ev.answer()
        await bot_client.send_message(
            admin_id,
            "Выбери аккаунт для удаления:",
            buttons=build_account_buttons(admin_id, "del_do"),
        )
        return

    if data == "val_select":
        if not get_accounts_meta(admin_id):
            await ev.answer("Нет аккаунтов", alert=True); return
        await ev.answer()
        await bot_client.send_message(
            admin_id,
            "Выбери аккаунт для проверки:",
            buttons=build_account_buttons(admin_id, "val_do"),
        )
        return

    if data.startswith("del_do:"):
        phone = data.split(":", 1)[1]
        worker = get_worker(admin_id, phone)
        await ev.answer()
        if worker:
            await worker.logout()
            unregister_worker(admin_id, phone)
        for ctx_key, ctx_val in list(reply_contexts.items()):
            if ctx_val.get("phone") == phone and ctx_val.get("owner_id") == admin_id:
                reply_contexts.pop(ctx_key, None)
                for admin_key, waiting_ctx in list(reply_waiting.items()):
                    if waiting_ctx.get("ctx") == ctx_key:
                        reply_waiting.pop(admin_key, None)
        accounts = get_accounts_meta(admin_id)
        meta = accounts.pop(phone, None)
        persist_tenants()
        if meta and meta.get("session_file") and os.path.exists(meta["session_file"]):
            with contextlib.suppress(OSError):
                os.remove(meta["session_file"])
        await bot_client.send_message(admin_id, f"🗑 Аккаунт {phone} удалён.", buttons=main_menu())
        return

    if data.startswith("val_do:"):
        phone = data.split(":", 1)[1]
        worker = get_worker(admin_id, phone)
        await ev.answer()
        meta = get_account_meta(admin_id, phone) or {}
        state = meta.get("state")
        if not worker:
            if state == "banned":
                await bot_client.send_message(
                    admin_id,
                    f"⛔️ {phone} заблокирован Telegram. Аккаунт отключён.",
                    buttons=main_menu(),
                )
            elif state == "frozen":
                await bot_client.send_message(
                    admin_id,
                    f"🧊 {phone} заморожен Telegram. Требуется разблокировка.",
                    buttons=main_menu(),
                )
            else:
                await bot_client.send_message(admin_id, f"⚠️ Аккаунт {phone} не активен.", buttons=main_menu())
            return
        ok = await worker.validate()
        if ok:
            await bot_client.send_message(admin_id, f"✅ {phone} активен и принимает сообщения.", buttons=main_menu())
        elif state == "banned":
            await bot_client.send_message(
                admin_id,
                f"⛔️ {phone} заблокирован Telegram. Аккаунт отключён.",
                buttons=main_menu(),
            )
        elif state == "frozen":
            await bot_client.send_message(
                admin_id,
                f"🧊 {phone} заморожен Telegram. Требуется разблокировка.",
                buttons=main_menu(),
            )
        else:
            await bot_client.send_message(admin_id, f"❌ {phone} не отвечает. Проверь подключение.", buttons=main_menu())
        return

    if data.startswith("reply:") or data.startswith("reply_to:"):
        ctx = data.split(":", 1)[1]
        if ctx not in reply_contexts:
            await ev.answer("Контекст истёк", alert=True)
            return
        if reply_waiting.get(admin_id):
            await ev.answer("Уже жду сообщение", alert=True)
            return
        mode = "reply" if data.startswith("reply_to:") else "normal"
        reply_waiting[admin_id] = {"ctx": ctx, "mode": mode}
        await ev.answer()
        ctx_info = reply_contexts[ctx]
        hint_suffix = " (будет отправлено как reply)." if mode == "reply" else "."
        await bot_client.send_message(
            admin_id,
            (
                f"Ответ для {ctx_info['phone']} (chat_id {ctx_info['chat_id']}): "
                f"пришли текст сообщения{hint_suffix}\n"
                "Или выбери шаблон ниже."
            ),
            buttons=build_reply_options_keyboard(ctx, mode),
        )
        return

    if data.startswith("reply_cancel:"):
        await ev.answer()
        await bot_client.send_message(admin_id, "❌ Ответ отменён.")
        return


    if data.startswith(("reply_paste_menu:", "reply_voice_menu:", "reply_video_menu:")):
        parts = data.split(":", 2)
        if len(parts) != 3:
            await ev.answer("Некорректные данные", alert=True)
            return
        _, ctx, mode = parts
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await ev.answer("Контекст истёк", alert=True)
            return
        owner_for_ctx = ctx_info["owner_id"]
        if data.startswith("reply_paste_menu:"):
            files = list_text_templates(owner_for_ctx)
            empty_text = "Папка с пастами пуста"
            title = "📄 Выбери пасту для отправки:"
            prefix = "paste_send"
        elif data.startswith("reply_voice_menu:"):
            files = list_voice_templates(owner_for_ctx)
            empty_text = "Папка с голосовыми пуста"
            title = "🎙 Выбери голосовое сообщение:"
            prefix = "voice_send"
        else:
            files = list_video_templates(owner_for_ctx)
            empty_text = "Папка с кружками пуста"
            title = "📹 Выбери кружок для отправки:"
            prefix = "video_send"
        if not files:
            await ev.answer(empty_text, alert=True)
            return
        await ev.answer()
        await bot_client.send_message(
            admin_id,
            title,
            buttons=build_asset_keyboard(files, prefix, ctx, mode),
        )
        return
    
    if data.startswith("paste_menu:"):
        ctx = data.split(":", 1)[1]
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await ev.answer("Контекст истёк", alert=True)
            return
        files = list_text_templates(admin_id)
        if not files:
            await ev.answer("Папка с пастами пуста", alert=True)
            return
        await ev.answer()
        await bot_client.send_message(
            admin_id,
            "📄 Выбери пасту для отправки:",
            buttons=build_asset_keyboard(files, "paste_send", ctx),
        )
        return

    if data.startswith("paste_send:"):
        parts = data.split(":")
        if len(parts) not in (3, 4):
            await ev.answer("Некорректные данные", alert=True)
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
            await ev.answer("Контекст истёк", alert=True)
            return
        try:
            idx = int(idx_str)
        except ValueError:
            await ev.answer("Некорректный выбор", alert=True)
            return
        files = list_text_templates(admin_id)
        if idx < 0 or idx >= len(files):
            await ev.answer("Файл не найден", alert=True)
            return
        file_path = files[idx]
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
        except Exception as e:
            await ev.answer(f"Ошибка чтения: {e}", alert=True)
            return
        if not content:
            await ev.answer("Файл пуст", alert=True)
            return
        worker = get_worker(admin_id, ctx_info["phone"])
        if not worker:
            await ev.answer("Аккаунт недоступен", alert=True)
            return
        reply_to_msg_id = ctx_info.get("msg_id") if mode == "reply" else None
        try:
            await worker.send_outgoing(
                ctx_info["chat_id"],
                content,
                ctx_info.get("peer"),
                reply_to_msg_id=reply_to_msg_id,
            )
        except Exception as e:
            await ev.answer(f"Ошибка отправки: {e}", alert=True)
            return
        await ev.answer("✅ Паста отправлена")
        await bot_client.send_message(admin_id, "✅ Паста отправлена собеседнику.")
        return

    if data.startswith("voice_menu:"):
        ctx = data.split(":", 1)[1]
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await ev.answer("Контекст истёк", alert=True)
            return
        files = list_voice_templates(admin_id)
        if not files:
            await ev.answer("Папка с голосовыми пуста", alert=True)
            return
        await ev.answer()
        await bot_client.send_message(
            admin_id,
            "🎙 Выбери голосовое сообщение:",
            buttons=build_asset_keyboard(files, "voice_send", ctx),
        )
        return

    if data.startswith("voice_send:"):
        parts = data.split(":")
        if len(parts) not in (3, 4):
            await ev.answer("Некорректные данные", alert=True)
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
            await ev.answer("Контекст истёк", alert=True)
            return
        try:
            idx = int(idx_str)
        except ValueError:
            await ev.answer("Некорректный выбор", alert=True)
            return
        files = list_voice_templates(admin_id)
        if idx < 0 or idx >= len(files):
            await ev.answer("Файл не найден", alert=True)
            return
        file_path = files[idx]
        worker = get_worker(admin_id, ctx_info["phone"])
        if not worker:
            await ev.answer("Аккаунт недоступен", alert=True)
            return
        reply_to_msg_id = ctx_info.get("msg_id") if mode == "reply" else None
        try:
            await worker.send_voice(
                ctx_info["chat_id"],
                file_path,
                ctx_info.get("peer"),
                reply_to_msg_id=reply_to_msg_id,
            )
        except Exception as e:
            await ev.answer(f"Ошибка отправки: {e}", alert=True)
            return
        await ev.answer("✅ Голосовое отправлено")
        await bot_client.send_message(admin_id, "✅ Голосовое сообщение отправлено собеседнику.")
        return

    if data.startswith("video_menu:"):
        ctx = data.split(":", 1)[1]
        ctx_info = get_reply_context_for_admin(ctx, admin_id)
        if not ctx_info:
            await ev.answer("Контекст истёк", alert=True)
            return
        files = list_video_templates(admin_id)
        if not files:
            await ev.answer("Папка с кружками пуста", alert=True)
            return
        await ev.answer()
        await bot_client.send_message(
            admin_id,
            "📹 Выбери кружок для отправки:",
            buttons=build_asset_keyboard(files, "video_send", ctx),
        )
        return

    if data.startswith("video_send:"):
        parts = data.split(":")
        if len(parts) not in (3, 4):
            await ev.answer("Некорректные данные", alert=True)
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
            await ev.answer("Контекст истёк", alert=True)
            return
        try:
            idx = int(idx_str)
        except ValueError:
            await ev.answer("Некорректный выбор", alert=True)
            return
        files = list_video_templates(admin_id)
        if idx < 0 or idx >= len(files):
            await ev.answer("Файл не найден", alert=True)
            return
        file_path = files[idx]
        worker = get_worker(admin_id, ctx_info["phone"])
        if not worker:
            await ev.answer("Аккаунт недоступен", alert=True)
            return
        reply_to_msg_id = ctx_info.get("msg_id") if mode == "reply" else None
        try:
            await worker.send_video_note(
                ctx_info["chat_id"],
                file_path,
                ctx_info.get("peer"),
                reply_to_msg_id=reply_to_msg_id,
            )
        except Exception as e:
            await ev.answer(f"Ошибка отправки: {e}", alert=True)
            return
        await ev.answer("✅ Кружок отправлен")
        await bot_client.send_message(admin_id, "✅ Кружок отправлен собеседнику.")
        return
    if data == "asset_close":
        await ev.answer()
        return

    if data == "ping":
        await ev.answer(); await bot_client.send_message(admin_id, "✅ OK", buttons=main_menu()); return

@bot_client.on(events.NewMessage)
async def on_text(ev):
    if not is_admin(ev.sender_id): return
    text = (ev.raw_text or "").strip()
    admin_id = ev.sender_id

    await ensure_menu_keyboard(admin_id)

    if text.upper() == MENU_BUTTON_TEXT:
        await cancel_operations(admin_id)
        await bot_client.send_message(admin_id, "Менеджер запущен. Выбери действие:", buttons=main_menu())
        return

    if text.startswith("/"):
        await cancel_operations(admin_id)
        parts = text.split()
        cmd = parts[0].lower()
        if cmd == "/start":
            await ev.respond("Менеджер запущен. Выбери действие:", buttons=main_menu())
        elif cmd == "/grant":
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
        elif cmd == "/revoke":
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
        reply_to_msg_id = ctx.get("msg_id") if waiting.get("mode") == "reply" else None
        try:
            await worker.send_outgoing(
                ctx["chat_id"],
                text,
                ctx.get("peer"),
                reply_to_msg_id=reply_to_msg_id,
            )
            await ev.reply("✅ Сообщение отправлено.")
        except Exception as e:
            await ev.reply(f"Ошибка отправки: {e}")
        return

    st = pending.get(admin_id)

    if st:
        if st.get("flow") == "file":
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
                    await ev.reply("Пришлите кружок (видео-сообщение).")
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
                        await ev.reply("Ожидается голосовое сообщение в формате .ogg.")
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
                    if not (getattr(msg, "video_note", None) or getattr(msg, "video", None)):
                        await ev.reply("Ожидается кружок (видео-сообщение).")
                        return
                    ext = ".mp4"
                    if msg.file and msg.file.ext:
                        ext = msg.file.ext
                    file_path = os.path.join(user_library_dir(admin_id, "video"), f"{name}{ext}")
                    try:
                        await msg.download_media(file=file_path)
                    except Exception as e:
                        await ev.reply(f"Не удалось сохранить кружок: {e}")
                        return
                    pending.pop(admin_id, None)
                    await ev.reply(f"✅ Кружок сохранён как {os.path.basename(file_path)}")
                    return

                await ev.reply("Неизвестный тип файла. Операция отменена.")
                pending.pop(admin_id, None)
                return

        if st["step"] == "phone":
            phone = text
            if not phone.startswith("+") or len(phone)<8:
                await ev.reply("Неверный формат. Пример: +7XXXXXXXXXX"); return
            if not API_KEYS:
                await ev.reply("Добавь API_KEYS в конфиг."); pending.pop(admin_id,None); return

            api = API_KEYS[next_index(admin_id, "api_idx", len(API_KEYS))]
            dev = DEVICE_PROFILES[next_index(admin_id, "dev_idx", len(DEVICE_PROFILES))] if DEVICE_PROFILES else {}

            # восстановить сессию, если уже есть
            sess = None
            meta = get_account_meta(admin_id, phone)
            if meta and os.path.exists(meta.get("session_file", "")):
                with open(meta["session_file"], "r", encoding="utf-8") as fh:
                    sess = fh.read().strip() or None

            w = AccountWorker(admin_id, phone, api["api_id"], api["api_hash"], dev, sess)
            try:
                await w.send_code()
            except Exception as e:
                await ev.reply(f"Не удалось отправить код: {e}")
                pending.pop(admin_id,None); return

            meta = ensure_account_meta(admin_id, phone)
            meta.update(
                {
                    "phone": phone,
                    "api_id": api["api_id"],
                    "device": dev.get("device_model", ""),
                    "session_file": user_session_path(admin_id, phone),
                }
            )
            meta["proxy_dynamic"] = w.using_dynamic_proxy
            meta["proxy_desc"] = w.proxy_description
            persist_tenants()

            pending[admin_id] = {"step":"code","phone":phone,"worker":w}
            await ev.reply(f"Код отправлен на {phone}. Пришли код.")
            return
        if st["step"] == "code":
            code = text
            w: AccountWorker = st["worker"]; phone = st["phone"]
            try:
                await w.sign_in_code(code)
            except SessionPasswordNeededError:
                pending[admin_id]["step"] = "2fa"
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


        if st["step"] == "2fa":
            pwd = text
            w: AccountWorker = st["worker"]; phone = st["phone"]
            try:
                await w.sign_in_2fa(pwd)
            except Exception as e:
                await ev.reply(f"2FA ошибка: {e}"); pending.pop(admin_id,None); return
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
            pending.pop(admin_id,None)
            await ev.reply(f"✅ {phone} добавлен (2FA). Слушаю входящие.")
            return

# ---- startup ----
async def startup():
    await bot_client.start(bot_token=BOT_TOKEN)
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
    await safe_send_admin("🚀 Бот запущен. /start", buttons=main_menu())

def main():
    loop = asyncio.get_event_loop()
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
