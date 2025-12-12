import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Настройки приложения"""
    
    # Telegram
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    
    # Database
    DATABASE_URL = os.getenv("DATABASE_URL")
    
    # LLM
    LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
    
    # Validation
    @classmethod
    def validate(cls):
        """Проверка обязательных настроек"""
        errors = []
        
        if not cls.TELEGRAM_BOT_TOKEN:
            errors.append("TELEGRAM_BOT_TOKEN не установлен")
        
        if not cls.DATABASE_URL:
            errors.append("DATABASE_URL не установлен")
        
        if cls.LLM_PROVIDER == "openai" and not cls.OPENAI_API_KEY:
            errors.append("OPENAI_API_KEY не установлен для провайдера openai")
        
        if cls.LLM_PROVIDER == "groq" and not cls.GROQ_API_KEY:
            errors.append("GROQ_API_KEY не установлен для провайдера groq")
        
        if errors:
            raise ValueError(f"Ошибки конфигурации: {', '.join(errors)}")


settings = Settings()