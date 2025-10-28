import asyncio
import os
import json
import logging
import sys
import random
import secrets
from logging.handlers import RotatingFileHandler
from typing import Dict, Optional, Any, List, Tuple
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
)

try:  # Telethon <= 1.33.1
    from telethon.errors import PhoneCodeFloodError  # type: ignore[attr-defined]
except ImportError:  # Telethon >= 1.34 moved/renamed the error
    try:
        from telethon.errors.rpcerrorlist import PhoneCodeFloodError  # type: ignore[attr-defined]
    except ImportError:
        from telethon.errors.rpcerrorlist import (
            PhoneNumberFloodError as PhoneCodeFloodError,  # type: ignore[attr-defined]
        )
import socks  # python-socks

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
ADMIN_ID  = 8099997426                 # твой user id (например 8099997426)

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
ACCOUNTS_META = "accounts.json"
ROTATION_STATE = ".rotation_state.json"
# ============================================

def _rand_delay(span: Tuple[int, int]) -> float:
    low, high = span
    if low >= high:
        return float(low)
    return random.uniform(low, high)

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

rotation_state: Dict[str,int] = _load(ROTATION_STATE, {})
accounts_meta: Dict[str,Dict[str,Any]] = _load(ACCOUNTS_META, {})

def next_index(key: str, length: int) -> int:
    cur = rotation_state.get(key, -1)
    cur = (cur + 1) % max(1, length)
    rotation_state[key] = cur
    _save(rotation_state, ROTATION_STATE)
    return cur

# ---- bot client ----
# Используем первую пару API_KEYS для бота
bot_client = TelegramClient(
    StringSession(),
    API_KEYS[0]["api_id"],
    API_KEYS[0]["api_hash"]
)

# безопасная отправка админу (не падаем, если админ ещё не нажал /start)
async def safe_send_admin(text: str, **kwargs):
    try:
        await bot_client.send_message(ADMIN_ID, text, **kwargs)
    except Exception as e:
        logging.getLogger("mgrbot").warning(
            "Cannot DM admin yet (probably admin hasn't started the bot): %s", e
        )

# ---- dynamic proxy tuple ----
def build_dynamic_proxy_tuple() -> Optional[Tuple]:
    if not DYNAMIC_PROXY.get("enabled"):
        return None
    t = DYNAMIC_PROXY.get("type","HTTP").upper()
    host = DYNAMIC_PROXY["host"]; port = int(DYNAMIC_PROXY["port"])
    rdns = bool(DYNAMIC_PROXY.get("rdns", True))
    user = DYNAMIC_PROXY.get("username"); pwd = DYNAMIC_PROXY.get("password")
    if t == "SOCKS5":
        return (socks.SOCKS5, host, port, rdns, user, pwd)
    elif t == "SOCKS4":
        return (socks.SOCKS4, host, port, rdns, user, pwd)
    else:
        return (socks.HTTP, host, port, rdns, user, pwd)

def proxy_desc(p: Optional[Tuple]) -> str:
    if not p: return "None"
    tp, host, port, *_ = p
    name = {socks.SOCKS5:"SOCKS5", socks.SOCKS4:"SOCKS4", socks.HTTP:"HTTP"}.get(tp, str(tp))
    return f"{name}://{host}:{port}"

# ---- worker ----
class AccountWorker:
    def __init__(self, phone: str, api_id: int, api_hash: str, device: Dict[str,str], session_str: Optional[str]):
        self.phone = phone
        self.api_id = api_id
        self.api_hash = api_hash
        self.device = device
        self.session_file = os.path.join(SESSIONS_DIR, f"{phone}.session")
        self.session = StringSession(session_str) if session_str else StringSession()
        self.client: Optional[TelegramClient] = None
        self.started = False
        self._keepalive_task: Optional[asyncio.Task] = None

    def _make_client(self) -> TelegramClient:
        return TelegramClient(
            self.session, self.api_id, self.api_hash,
            proxy=build_dynamic_proxy_tuple(),
            device_model=self.device.get("device_model"),
            system_version=self.device.get("system_version"),
            app_version=self.device.get("app_version"),
            lang_code=self.device.get("lang_code"),
        )

    async def _ensure_client(self) -> TelegramClient:
        if not self.client:
            self.client = self._make_client()
        if not self.client.is_connected():
            await self.client.connect()
        return self.client

    async def start(self):
        self.client = await self._ensure_client()
        if not await self.client.is_user_authorized():
            return

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
            reply_contexts[ctx_id] = {
                "phone": self.phone,
                "chat_id": ev.chat_id,
                "sender_id": ev.sender_id,
                "peer": peer,
                "msg_id": ev.id,
            }          
            msg = (f"📥 <b>{self.phone}</b>\n"
                   f"proxy: <code>{proxy_desc(build_dynamic_proxy_tuple())}</code>\n"
                   f"chat_id: <code>{ev.chat_id}</code>\n"
                   f"sender_id: <code>{ev.sender_id}</code>\n\n{txt}")
            await safe_send_admin(
                msg,
                parse_mode="html",
                buttons=[[
                    Button.inline("✉️ Ответить", f"reply:{ctx_id}".encode()),
                    Button.inline("↩️ Реплай", f"reply_to:{ctx_id}".encode()),
                ]]
            )

        await self.client.start()
        self.started = True
        with open(self.session_file, "w", encoding="utf-8") as f:
            f.write(self.client.session.save())
        log.info("[%s] started on %s; device=%s",
                 self.phone, proxy_desc(build_dynamic_proxy_tuple()), self.device.get("device_model"))

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
        except FloodWaitError as e:
            wait = getattr(e, "seconds", getattr(e, "value", 60))
            log.warning("[%s] flood wait %ss on sign_in", self.phone, wait)
            await asyncio.sleep(wait + 5)
            raise
        with open(self.session_file, "w", encoding="utf-8") as f:
            f.write(self.client.session.save())

    async def sign_in_2fa(self, password: str):
        await asyncio.sleep(_rand_delay(LOGIN_DELAY_SECONDS))
        try:
            await self.client.sign_in(password=password)
        except FloodWaitError as e:
            wait = getattr(e, "seconds", getattr(e, "value", 60))
            log.warning("[%s] flood wait %ss on 2FA", self.phone, wait)
            await asyncio.sleep(wait + 5)
            raise
        with open(self.session_file, "w", encoding="utf-8") as f:
            f.write(self.client.session.save())

    async def _reconnect(self):
        # Разорвать соединение и создать новое — получим новый IP от динамического прокси
        try:
            if self.client:
                await self.client.disconnect()
        except Exception:
            pass
        self.client = self._make_client()
        await self.start()

    async def validate(self) -> bool:
        try:
            client = await self._ensure_client()
            if not await client.is_user_authorized():
                return False
            await client.get_me()
            return True
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
        await client.send_message(peer, message, reply_to=reply_to_msg_id)

    async def _keepalive(self):
        """Поддержание соединения: по ошибкам — reconnect; по таймеру (если включён) — тоже."""
        while True:
            try:
                await self.client.get_me()
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
WORKERS: Dict[str, AccountWorker] = {}
reply_contexts: Dict[str, Dict[str, Any]] = {}
reply_waiting: Dict[int, Dict[str, Any]] = {}

def main_menu():
    return [
        [Button.inline("➕ Добавить аккаунт", b"add")],
        [Button.inline("📋 Список аккаунтов", b"list")],
        [Button.inline("🧪 Ping", b"ping")],
    ]

def account_control_menu():
    return [
        [Button.inline("🗑 Удалить аккаунт", b"del_select")],
        [Button.inline("✅ Валидация", b"val_select")],
        [Button.inline("⬅️ Назад", b"back")],
    ]

def build_account_buttons(prefix: str) -> List[List[Button]]:
    rows: List[List[Button]] = []
    for phone in list(accounts_meta.keys()):
        rows.append([Button.inline(phone, f"{prefix}:{phone}".encode())])
    rows.append([Button.inline("⬅️ Назад", b"list")])
    return rows

@bot_client.on(events.NewMessage(pattern="/start"))
async def on_start(ev):
    if ev.sender_id != ADMIN_ID:
        await ev.respond("Доступ запрещён."); return
    await ev.respond("Менеджер запущен. Выбери действие:", buttons=main_menu())

@bot_client.on(events.CallbackQuery)
async def on_cb(ev):
    if ev.sender_id != ADMIN_ID:
        await ev.answer("Недоступно", alert=True); return
    data = ev.data.decode() if isinstance(ev.data, (bytes, bytearray)) else str(ev.data)

    if data == "add":
        pending[ADMIN_ID] = {"step":"phone"}
        await ev.answer(); await bot_client.send_message(ADMIN_ID, "Пришли номер телефона (+7XXXXXXXXXX)")
        return

    if data == "list":
        if not accounts_meta:
            await ev.answer("Пусто", alert=True); await bot_client.send_message(ADMIN_ID, "Аккаунтов нет."); return
        lines = ["Аккаунты:"]
        for p,m in accounts_meta.items():
            lines.append(f"• {p} | api:{m.get('api_id')} | dev:{m.get('device','')}")
        await ev.answer()
        await bot_client.send_message(ADMIN_ID, "\n".join(lines), buttons=account_control_menu())
        return

    if data == "back":
        await ev.answer()
        await bot_client.send_message(ADMIN_ID, "Главное меню", buttons=main_menu())
        return

    if data == "del_select":
        if not accounts_meta:
            await ev.answer("Нет аккаунтов", alert=True); return
        await ev.answer()
        await bot_client.send_message(ADMIN_ID, "Выбери аккаунт для удаления:", buttons=build_account_buttons("del_do"))
        return

    if data == "val_select":
        if not accounts_meta:
            await ev.answer("Нет аккаунтов", alert=True); return
        await ev.answer()
        await bot_client.send_message(ADMIN_ID, "Выбери аккаунт для проверки:", buttons=build_account_buttons("val_do"))
        return

    if data.startswith("del_do:"):
        phone = data.split(":", 1)[1]
        worker = WORKERS.get(phone)
        await ev.answer()
        if worker:
            await worker.logout()
            WORKERS.pop(phone, None)
        for ctx_key, ctx_val in list(reply_contexts.items()):
            if ctx_val.get("phone") == phone:
                reply_contexts.pop(ctx_key, None)
                waiting_ctx = reply_waiting.get(ADMIN_ID)
                if waiting_ctx and waiting_ctx.get("ctx") == ctx_key:
                    reply_waiting.pop(ADMIN_ID, None)
        meta = accounts_meta.pop(phone, None)
        _save(accounts_meta, ACCOUNTS_META)
        if meta and meta.get("session_file") and os.path.exists(meta["session_file"]):
            try:
                os.remove(meta["session_file"])
            except OSError:
                pass
        await bot_client.send_message(ADMIN_ID, f"🗑 Аккаунт {phone} удалён.", buttons=main_menu())
        return

    if data.startswith("val_do:"):
        phone = data.split(":", 1)[1]
        worker = WORKERS.get(phone)
        await ev.answer()
        if not worker:
            await bot_client.send_message(ADMIN_ID, f"⚠️ Аккаунт {phone} не активен.", buttons=main_menu())
            return
        ok = await worker.validate()
        if ok:
            await bot_client.send_message(ADMIN_ID, f"✅ {phone} активен и принимает сообщения.", buttons=main_menu())
        else:
            await bot_client.send_message(ADMIN_ID, f"❌ {phone} не отвечает. Проверь подключение.", buttons=main_menu())
        return

    if data.startswith("reply:") or data.startswith("reply_to:"):
        ctx = data.split(":", 1)[1]
        if ctx not in reply_contexts:
            await ev.answer("Контекст истёк", alert=True)
            return
        if pending.get(ADMIN_ID):
            await ev.answer("Заверши текущую операцию", alert=True)
            return
        if reply_waiting.get(ADMIN_ID):
            await ev.answer("Уже жду сообщение", alert=True)
            return
        mode = "reply" if data.startswith("reply_to:") else "normal"
        reply_waiting[ADMIN_ID] = {"ctx": ctx, "mode": mode}
        await ev.answer()
        ctx_info = reply_contexts[ctx]
        hint_suffix = " (будет отправлено как reply)." if mode == "reply" else "."
        await bot_client.send_message(
            ADMIN_ID,
            f"Ответ для {ctx_info['phone']} (chat_id {ctx_info['chat_id']}): пришли текст сообщения{hint_suffix}"
        )
        return

    if data == "ping":
        await ev.answer(); await bot_client.send_message(ADMIN_ID, "✅ OK", buttons=main_menu()); return

@bot_client.on(events.NewMessage)
async def on_text(ev):
    if ev.sender_id != ADMIN_ID: return
    text = (ev.raw_text or "").strip()

    waiting = reply_waiting.get(ADMIN_ID)
    if waiting:
        if not text:
            await ev.reply("Пустое сообщение. Пришли текст для отправки.")
            return
        reply_waiting.pop(ADMIN_ID, None)
        ctx_id = waiting.get("ctx")
        ctx = reply_contexts.get(ctx_id)
        if not ctx:
            await ev.reply("Контекст ответа устарел.")
            return
        worker = WORKERS.get(ctx["phone"])
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

    st = pending.get(ADMIN_ID)

    if st:
        if st["step"] == "phone":
            phone = text
            if not phone.startswith("+") or len(phone)<8:
                await ev.reply("Неверный формат. Пример: +7XXXXXXXXXX"); return
            if not API_KEYS:
                await ev.reply("Добавь API_KEYS в конфиг."); pending.pop(ADMIN_ID,None); return

            api = API_KEYS[next_index("api_idx", len(API_KEYS))]
            dev = DEVICE_PROFILES[next_index("dev_idx", len(DEVICE_PROFILES))] if DEVICE_PROFILES else {}

            # восстановить сессию, если уже есть
            sess = None
            meta = accounts_meta.get(phone)
            if meta and os.path.exists(meta.get("session_file","")):
                sess = open(meta["session_file"], "r", encoding="utf-8").read().strip() or None

            w = AccountWorker(phone, api["api_id"], api["api_hash"], dev, sess)
            try:
                await w.send_code()
            except Exception as e:
                await ev.reply(f"Не удалось отправить код: {e}")
                pending.pop(ADMIN_ID,None); return

            accounts_meta[phone] = {
                "phone": phone,
                "api_id": api["api_id"],
                "device": dev.get("device_model",""),
                "session_file": os.path.join(SESSIONS_DIR, f"{phone}.session"),
                "proxy_dynamic": DYNAMIC_PROXY.get("enabled", False),
                "proxy_desc": proxy_desc(build_dynamic_proxy_tuple()),
            }
            _save(accounts_meta, ACCOUNTS_META)

            pending[ADMIN_ID] = {"step":"code","phone":phone,"worker":w}
            await ev.reply(f"Код отправлен на {phone}. Пришли код.")
            return
        if st["step"] == "code":
            code = text
            w: AccountWorker = st["worker"]; phone = st["phone"]
            try:
                await w.sign_in_code(code)
            except SessionPasswordNeededError:
                pending[ADMIN_ID]["step"] = "2fa"
                await ev.reply("Включена двухэтапная защита. Пришли пароль 2FA для аккаунта.")
                return
            except Exception as e:
                await ev.reply(f"Ошибка входа: {e}")
                pending.pop(ADMIN_ID, None)
                return
            WORKERS[phone] = w
            await w.start()
            pending.pop(ADMIN_ID, None)
            await ev.reply(f"✅ {phone} добавлен. Слушаю входящие.")
            return


        if st["step"] == "2fa":
            pwd = text
            w: AccountWorker = st["worker"]; phone = st["phone"]
            try:
                await w.sign_in_2fa(pwd)
            except Exception as e:
                await ev.reply(f"2FA ошибка: {e}"); pending.pop(ADMIN_ID,None); return
            WORKERS[phone]=w; await w.start(); pending.pop(ADMIN_ID,None)
            await ev.reply(f"✅ {phone} добавлен (2FA). Слушаю входящие.")
            return

# ---- startup ----
async def startup():
    await bot_client.start(bot_token=BOT_TOKEN)
    log.info("Bot started. Restore workers...")
    for phone, meta in accounts_meta.items():
        sess = None
        sf = meta.get("session_file")
        if sf and os.path.exists(sf):
            sess = open(sf, "r", encoding="utf-8").read().strip() or None
        api_id = int(meta.get("api_id"))
        api_hash = None
        for k in API_KEYS:
            if k["api_id"] == api_id:
                api_hash = k["api_hash"]; break
        if not api_hash and API_KEYS:
            api_hash = API_KEYS[0]["api_hash"]
        dev = next((d for d in DEVICE_PROFILES if d.get("device_model")==meta.get("device")), None) or (DEVICE_PROFILES[0] if DEVICE_PROFILES else {})
        w = AccountWorker(phone, api_id, api_hash, dev, sess)
        WORKERS[phone] = w
        try:
            await w.start()
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
        for w in WORKERS.values():
            try: loop.run_until_complete(w.stop())
            except: pass
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
