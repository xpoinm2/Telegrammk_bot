TG Manager Bot (Telethon) — dynamic proxy + multi-API
====================================================

1) Установка
-----------
python -m venv venv
# Windows: venv\Scripts\activate
# Linux/macOS: source venv/bin/activate
pip install telethon python-socks

2) Настройка
------------
- Открой tg_manager_bot_dynamic.py и заполни:
  * API_KEYS  — до 5 пар {"api_id": ..., "api_hash": "..."}
  * BOT_TOKEN — токен от @BotFather
  * ADMIN_ID  — твой user id (например 8099997426)
  * DYNAMIC_PROXY — оставь enabled=True и свои прокси-параметры; или поставь False

3) Запуск
--------
python tg_manager_bot_dynamic.py

В Telegram открой своего бота:
/start -> Добавить аккаунт -> номер -> код (и 2FA при необходимости).

Логи пишутся в bot.log. Окно не закроется при ошибке — ждёт Enter.
