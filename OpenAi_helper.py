import os
from typing import Optional, Literal
from openai import AsyncOpenAI

# Импорт логгера из основного файла
try:
    from tg_manager_bot_dynamic import logger
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# Доступные модели OpenAI
OpenAIModel = Literal[
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-4-turbo",
    "gpt-4",
    "gpt-3.5-turbo",
    "o1",
    "o1-preview",
    "o1-mini",
]

# Дефолтные значения промптов
DEFAULT_SYSTEM_PROMPT = ""
DEFAULT_USER_PROMPT = ""

# Инициализация OpenAI клиента
# API ключ можно задать через переменную окружения OPENAI_API_KEY
# или передать напрямую при создании клиента
_openai_client: Optional[AsyncOpenAI] = None
_cached_api_key: Optional[str] = None


def get_openai_client(api_key: Optional[str] = None) -> AsyncOpenAI:
    """Получить или создать OpenAI клиент."""
    global _openai_client, _cached_api_key
    api_key = api_key or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OpenAI API key не найден. Установите OPENAI_API_KEY или передайте api_key параметр.")
    
    # Пересоздаем клиент, если изменился API ключ
    if _openai_client is None or _cached_api_key != api_key:
        _openai_client = AsyncOpenAI(api_key=api_key)
        _cached_api_key = api_key
    
    return _openai_client


async def gpt(
    model: OpenAIModel,
    system_prompt: str,
    user_prompt: str,
    api_key: Optional[str] = None,
    temperature: float = 1.0,
    max_tokens: Optional[int] = None,
) -> str:
    """
    Обертка над OpenAI API для асинхронных запросов.
    
    Args:
        model: Название модели (например, "gpt-4o", "gpt-3.5-turbo")
        system_prompt: Системный промпт
        user_prompt: Пользовательский промпт
        api_key: API ключ OpenAI (опционально, можно использовать OPENAI_API_KEY env var)
        temperature: Температура для генерации (по умолчанию 1.0)
        max_tokens: Максимальное количество токенов (опционально)
    
    Returns:
        Ответ от модели в виде строки
    
    Raises:
        ValueError: Если API ключ не найден
        Exception: При ошибках API запроса
    """
    logger.info(f"GPT request: model={model}, system_prompt_length={len(system_prompt)}, user_prompt_length={len(user_prompt)}")
    logger.debug(f"system_prompt={system_prompt}")
    logger.debug(f"user_prompt={user_prompt}")
    
    # Валидация: o1 модели не поддерживают system prompts
    o1_models = {"o1", "o1-preview", "o1-mini"}
    if model in o1_models and system_prompt:
        logger.warning(f"Model {model} не поддерживает system prompts. System prompt будет проигнорирован.")
        system_prompt = ""
    
    # Валидация: пустые промпты
    if not user_prompt.strip():
        raise ValueError("user_prompt не может быть пустым")
    
    client = get_openai_client(api_key)
    
    try:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})
        
        kwargs = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
        }
        
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens
        
        response = await client.chat.completions.create(**kwargs)
        
        # Проверка на пустой ответ
        if not response.choices:
            raise ValueError("OpenAI API вернул пустой список choices")
        
        if not response.choices[0].message:
            raise ValueError("OpenAI API вернул пустое сообщение")
        
        result = response.choices[0].message.content
        if result is None:
            raise ValueError("OpenAI API вернул None в качестве содержимого ответа")
        
        logger.debug(f"GPT response: {result}")
        
        return result
        
    except ValueError as e:
        logger.error(f"Ошибка валидации при запросе к OpenAI API: {e}")
        raise
    except Exception as e:
        logger.error(f"Ошибка при запросе к OpenAI API: {e}")
        raise


async def gpt_answer(
    prompt: str,
    system_prompt: str = DEFAULT_SYSTEM_PROMPT,
    api_key: Optional[str] = None,
    model: OpenAIModel = "gpt-4o",
    temperature: float = 1.0,
    max_tokens: Optional[int] = None,
) -> str:
    """
    Обертка над GPT для обработки одного промпта.
    
    Args:
        prompt: Промпт для передачи в нейросеть
        system_prompt: Системный промпт (опционально, по умолчанию пустая строка)
        api_key: API ключ OpenAI (опционально)
        model: Модель для использования (по умолчанию "gpt-4o")
        temperature: Температура для генерации (по умолчанию 1.0)
        max_tokens: Максимальное количество токенов (опционально)
    
    Returns:
        Ответ от модели в виде строки
    """
    logger.info(f"GPT answer request: prompt_length={len(prompt)}")
    logger.debug(f"prompt={prompt}")
    
    response = await gpt(model, system_prompt, prompt, api_key, temperature, max_tokens)
    
    return response

