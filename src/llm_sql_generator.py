import json
import re
from typing import Dict, Any, Optional
import openai
from anthropic import Anthropic
import groq
import os
from datetime import datetime


class LLMSQLGenerator:
    def __init__(self, provider: str = "openai"):
        self.provider = provider
        self.schema_description = self._get_schema_description()
        
    def _get_schema_description(self) -> str:
        """Описание схемы данных для промпта LLM"""
        return """
        ## Database Schema for Video Analytics
        
        ### Table: videos
        - id: BIGINT (PRIMARY KEY) - уникальный идентификатор видео
        - creator_id: BIGINT - идентификатор креатора
        - video_created_at: TIMESTAMP - дата и время публикации видео
        - views_count: BIGINT - финальное количество просмотров
        - likes_count: BIGINT - финальное количество лайков
        - comments_count: BIGINT - финальное количество комментариев
        - reports_count: BIGINT - финальное количество жалоб
        - created_at, updated_at: TIMESTAMP - служебные поля
        
        ### Table: video_snapshots
        - id: BIGSERIAL (PRIMARY KEY) - уникальный идентификатор снапшота
        - video_id: BIGINT (FOREIGN KEY references videos.id) - ссылка на видео
        - views_count, likes_count, comments_count, reports_count: BIGINT - текущие значения на момент замера
        - delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count: BIGINT - приращение с прошлого замера
        - created_at: TIMESTAMP - время замера (раз в час)
        - updated_at: TIMESTAMP - служебное поле
        
        ### Важные моменты:
        1. video_snapshots.created_at - это время снятия снапшота (раз в час)
        2. videos.video_created_at - это время публикации видео
        3. delta_* поля показывают изменение за последний час
        4. Для подсчета "прироста за день" нужно SUM(delta_*_count) за нужную дату
        5. Для подсчета "сколько видео получали новые просмотры" нужно COUNT(DISTINCT video_id) WHERE delta_views_count > 0
        """
    
    def _extract_date_range(self, text: str) -> Dict[str, str]:
        """Извлечение дат из русского текста"""
        date_patterns = [
            (r'(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})', 'single'),
            (r'с\s+(\d{1,2})\s+по\s+(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})', 'range'),
            (r'(\d{1,2})\s*[-–]\s*(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})', 'range'),
        ]
        
        month_map = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        
        for pattern, pattern_type in date_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if pattern_type == 'single':
                    day, month, year = match.groups()
                    date_str = f"{year}-{month_map[month.lower()]}-{int(day):02d}"
                    return {'start_date': date_str, 'end_date': date_str}
                elif pattern_type == 'range':
                    if len(match.groups()) == 4:
                        day1, day2, month, year = match.groups()
                        start_date = f"{year}-{month_map[month.lower()]}-{int(day1):02d}"
                        end_date = f"{year}-{month_map[month.lower()]}-{int(day2):02d}"
                        return {'start_date': start_date, 'end_date': end_date}
        
        return {'start_date': None, 'end_date': None}
    
    def _build_prompt(self, user_query: str, date_info: Dict[str, str]) -> str:
        """Строим промпт для LLM"""
        date_context = ""
        if date_info['start_date']:
            if date_info['start_date'] == date_info['end_date']:
                date_context = f"Дата: {date_info['start_date']}"
            else:
                date_context = f"Период: с {date_info['start_date']} по {date_info['end_date']}"
        
        prompt = f"""
        {self.schema_description}
        
        {date_context}
        
        Пользовательский запрос: "{user_query}"
        
        Преобразуй этот запрос в единственный SQL-запрос к PostgreSQL, который вернет ОДНО число.
        
        ### КРИТИЧЕСКИ ВАЖНЫЕ ПРАВИЛА:
        1. Запрос должен возвращать ТОЛЬКО одно число (одно значение, одна колонка)
        2. Используй правильные имена таблиц и колонок из схемы выше
        3. Учитывай логику данных:
           - Для подсчета видео используй таблицу `videos`
           - Для подсчета прироста (просмотров, лайков и т.д.) используй `delta_*` поля в `video_snapshots`
           - Для фильтрации по дате публикации видео используй `videos.video_created_at`
           - Для фильтрации по дате снятия снапшота используй `video_snapshots.created_at`
        4. Если в запросе указана конкретная дата или период, добав соответствующие условия WHERE
        5. Не добавляй LIMIT, OFFSET, ORDER BY если они не нужны для получения одного числа
        6. Используй агрегатные функции: COUNT(), SUM(), AVG() когда нужно
        7. Убедись, что JOIN выполняется правильно при работе с двумя таблицами
        
        ### Примеры правильных SQL-запросов:
        1. "Сколько всего видео есть в системе?" → SELECT COUNT(*) FROM videos;
        2. "Сколько видео у креатора с id 123 вышло с 2025-11-01 по 2025-11-05?" → SELECT COUNT(*) FROM videos WHERE creator_id = 123 AND video_created_at BETWEEN '2025-11-01' AND '2025-11-06';
        3. "Сколько видео набрало больше 100000 просмотров?" → SELECT COUNT(*) FROM videos WHERE views_count > 100000;
        4. "На сколько просмотров в сумме выросли все видео 2025-11-28?" → SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';
        5. "Сколько разных видео получали новые просмотры 2025-11-27?" → SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;
        
        Верни ТОЛЬКО SQL-запрос, без пояснений, без обратных кавычек, без markdown.
        SQL-запрос:
        """
        
        return prompt
    
    def generate_sql(self, user_query: str) -> str:
        """Генерация SQL из текстового запроса"""
        # Сначала извлекаем даты
        date_info = self._extract_date_range(user_query)
        
        # Строим промпт
        prompt = self._build_prompt(user_query, date_info)
        
        try:
            if self.provider == "openai":
                client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
                response = client.chat.completions.create(
                    model="gpt-4-turbo-preview",
                    messages=[
                        {"role": "system", "content": "Ты эксперт по SQL и анализу данных. Преобразуй текстовые запросы в точные SQL-запросы."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    max_tokens=500
                )
                sql = response.choices[0].message.content.strip()
                
            elif self.provider == "anthropic":
                client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
                response = client.messages.create(
                    model="claude-3-opus-20240229",
                    max_tokens=500,
                    temperature=0.1,
                    system="Ты эксперт по SQL и анализу данных. Преобразуй текстовые запросы в точные SQL-запросы.",
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )
                sql = response.content[0].text.strip()
                
            elif self.provider == "groq":
                client = groq.Groq(api_key=os.getenv("GROQ_API_KEY"))
                response = client.chat.completions.create(
                    model="mixtral-8x7b-32768",
                    messages=[
                        {"role": "system", "content": "Ты эксперт по SQL и анализу данных. Преобразуй текстовые запросы в точные SQL-запросы."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    max_tokens=500
                )
                sql = response.choices[0].message.content.strip()
                
            elif self.provider == "ollama":
                # Для локальной LLM
                import requests
                response = requests.post(
                    "http://localhost:11434/api/generate",
                    json={
                        "model": "codellama:13b",
                        "prompt": prompt,
                        "stream": False,
                        "options": {"temperature": 0.1}
                    }
                )
                sql = response.json()["response"].strip()
            
            else:
                raise ValueError(f"Unsupported provider: {self.provider}")
            
            # Очистка SQL от markdown и лишних символов
            sql = sql.replace("```sql", "").replace("```", "").strip()
            sql = sql.split(';')[0] + ';' if ';' in sql else sql + ';'
            
            return sql
            
        except Exception as e:
            # Fallback на правила для простых запросов
            return self._fallback_sql_generation(user_query, date_info)
    
    def _fallback_sql_generation(self, user_query: str, date_info: Dict[str, str]) -> str:
        """Резервная логика генерации SQL если LLM недоступна"""
        query_lower = user_query.lower()
        
        # Базовые шаблоны
        if "сколько всего видео" in query_lower:
            return "SELECT COUNT(*) FROM videos;"
        
        elif "сколько видео у креатора" in query_lower:
            # Извлекаем creator_id
            match = re.search(r'креатора\s+с\s+id\s+(\d+)', user_query, re.IGNORECASE)
            if match:
                creator_id = match.group(1)
                if date_info['start_date']:
                    return f"SELECT COUNT(*) FROM videos WHERE creator_id = {creator_id} AND DATE(video_created_at) BETWEEN '{date_info['start_date']}' AND '{date_info['end_date']}';"
                else:
                    return f"SELECT COUNT(*) FROM videos WHERE creator_id = {creator_id};"
        
        elif "больше" in query_lower and "просмотров" in query_lower:
            # Извлекаем число
            match = re.search(r'больше\s+([\d\s]+)\s+просмотров', user_query)
            if match:
                number = match.group(1).replace(' ', '')
                return f"SELECT COUNT(*) FROM videos WHERE views_count > {number};"
        
        elif "на сколько просмотров" in query_lower and "выросли" in query_lower:
            if date_info['start_date']:
                return f"SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '{date_info['start_date']}';"
        
        elif "сколько разных видео получали новые просмотры" in query_lower:
            if date_info['start_date']:
                return f"SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '{date_info['start_date']}' AND delta_views_count > 0;"
        
        # Дефолтный запрос
        return "SELECT 'Запрос не распознан' as result;"
