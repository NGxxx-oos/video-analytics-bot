import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.enums import ParseMode

from config import settings
from database import Database
from llm_sql_generator import LLMSQLGenerator


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
db = Database(settings.DATABASE_URL)
llm_sql = LLMSQLGenerator(provider=settings.LLM_PROVIDER)


@dp.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    welcome_text = """
    🎬 *Бот аналитики видео-креаторов*
    
    Я умею отвечать на вопросы о статистике видео на естественном языке.
    
    *Примеры запросов:*
    • Сколько всего видео есть в системе?
    • Сколько видео у креатора с id 123 вышло с 1 по 5 ноября 2025?
    • Сколько видео набрало больше 100000 просмотров?
    • На сколько просмотров выросли все видео 28 ноября 2025?
    • Сколько разных видео получали новые просмотры 27 ноября 2025?
    
    Просто напишите вопрос, и я верну число-ответ!
    """
    await message.answer(welcome_text, parse_mode=ParseMode.MARKDOWN)


@dp.message(Command("help"))
async def cmd_help(message: Message):
    """Обработчик команды /help"""
    help_text = """
    *Как пользоваться ботом:*
    
    1. Задавайте вопросы на русском языке
    2. Используйте естественные формулировки
    3. Указывайте даты в формате "28 ноября 2025" или "с 1 по 5 ноября 2025"
    4. Бот вернет одно число - ответ на ваш вопрос
    
    *Поддерживаемые типы запросов:*
    • Подсчет видео (COUNT)
    • Суммирование метрик (SUM)
    • Подсчет уникальных значений (COUNT DISTINCT)
    • Фильтрация по датам, креаторам, пороговым значениям
    
    *Пример:* "Сколько видео набрало больше 50000 лайков?"
    """
    await message.answer(help_text, parse_mode=ParseMode.MARKDOWN)


@dp.message()
async def handle_text_query(message: Message):
    """Обработчик текстовых запросов"""
    user_query = message.text.strip()
    user_id = message.from_user.id
    
    logger.info(f"Query from user {user_id}: {user_query}")
    
    
    await message.bot.send_chat_action(chat_id=message.chat.id, action="typing")
    
    try:
        
        sql_query = llm_sql.generate_sql(user_query)
        logger.info(f"Generated SQL: {sql_query}")
        
        
        result = await db.execute_query(sql_query)
        
        
        if result is None:
            response = "0"
        else:
            
            if isinstance(result, (list, tuple)) and len(result) > 0:
                if isinstance(result[0], (list, tuple)) and len(result[0]) > 0:
                    response = str(result[0][0])
                else:
                    response = str(result[0])
            else:
                response = str(result)
        
        
        try:
            num = int(float(response))
            response = f"{num:,}".replace(",", " ")
        except:
            pass
        
        
        await message.answer(f"📊 *Ответ:* {response}", parse_mode=ParseMode.MARKDOWN)
        
        
        logger.info(f"Success response to user {user_id}: {response}")
        
    except Exception as e:
        logger.error(f"Error processing query from user {user_id}: {str(e)}")
        
        
        error_msg = (
            "❌ *Произошла ошибка при обработке запроса*\n\n"
            "Попробуйте:\n"
            "1. Переформулировать вопрос более четко\n"
            "2. Проверить корректность указанных данных\n"
            "3. Использовать примеры из /help\n\n"
            f"*Техническая информация:* `{str(e)[:100]}`"
        )
        await message.answer(error_msg, parse_mode=ParseMode.MARKDOWN)


async def main():
    """Основная функция запуска бота"""
    logger.info("Starting video analytics bot...")
    
    
    try:
        settings.validate()
        logger.info("Settings validation passed")
    except ValueError as e:
        logger.error(f"Settings validation failed: {e}")
        print(f"❌ Ошибка конфигурации: {e}")
        print("Проверьте файл .env")
        return
    
    
    try:
        await db.connect()
        logger.info("Database connection established")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        print(f"❌ Ошибка подключения к БД: {e}")
        return
    
    
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())