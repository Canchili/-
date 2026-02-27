import re
import asyncio
import aiohttp
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time
from typing import List, Dict, Optional, Set
import logging

from database import save_to_primamedia, primamedia_url_exists

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Очищает сырой текст статьи от мусора.
def clean_primamedia_text(text: str) -> str:
    """Очищает текст """
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'http[s]?://\S+|www\.\S+', '', text)
    text = re.sub(r'[ \t]+\n', '\n', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\s+\n', '\n', text)
    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r'[ \t]+', ' ', text).strip()
    return text[:10000]

# Парсит одну статью полностью.
async def parse_primamedia_article(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> bool:
    """Асинхронно парсит ОДНУ статью с семафором для ограничения потоков"""
    async with semaphore:  # Ограничиваем до 5 одновременных запросов
        try:
            logger.info(f"     Парсим: {url}")
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return False

                text = await resp.text()
                soup = BeautifulSoup(text, 'html.parser')

                # Заголовок
                title_elem = soup.select_one('h1')
                title = title_elem.get_text(strip=True) if title_elem else 'Без заголовка'

                # Текст статьи
                text_parts = []
                text_blocks = soup.select('div[class*="article"] p, div[class*="text"] p, article p')
                if text_blocks:
                    text_parts = [p.get_text(strip=True) for p in text_blocks[:30]]
                else:
                    selectors = ['.entry-content p', '.article-content p', '.news-text p', '.content p']
                    for selector in selectors:
                        paragraphs = soup.select(selector)
                        if paragraphs:
                            text_parts = [p.get_text(strip=True) for p in paragraphs[:25]]
                            break

                description = clean_primamedia_text('\n\n'.join(text_parts))
                logger.info(f" ℹ️ Длина текста: {len(description)} симв.")

                if len(description) < 300:
                    return False

                # Дата
                date_elem = soup.select_one('time, .date, .published, .news-date, .entry-date')
                published_at = date_elem.get('datetime') if date_elem else None

                article_data = {
                    'title': title[:255],
                    'description': description,
                    'url': url,
                    'published_at': published_at
                }

                guid = save_to_primamedia(article_data)
                if guid:
                    logger.info(f"      {title[:70]}... ({len(description)} симв.)")
                    return True
                return False

        except Exception as e:
            logger.error(f"      Ошибка: {e}")
            return False

# С страницы архива собирает список ссылок на новости.
async def get_news_links_from_page(session: aiohttp.ClientSession, page_url: str, semaphore: asyncio.Semaphore) -> List[
    str]:
    """Получает ссылки на новости с одной страницы"""
    async with semaphore:
        try:
            async with session.get(page_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status != 200:
                    return []

                text = await resp.text()
                soup = BeautifulSoup(text, 'html.parser')

                all_links = soup.select('a[href*="/news/"]')
                news_hrefs = []

                for link in all_links:
                    href = link.get('href') or ''
                    if href.startswith('http'):
                        if '/news/' not in href:
                            continue
                        path = href.split('://', 1)[1].split('/', 1)[1]
                        path = '/' + path
                    else:
                        path = href

                    path = path.split('?', 1)[0]
                    if re.fullmatch(r"/news/\d+/", path):
                        news_hrefs.append(path)

                return list(dict.fromkeys(news_hrefs))  # убираем дубликаты
        except Exception as e:
            logger.error(f"Ошибка получения ссылок со страницы {page_url}: {e}")
            return []

# Обработка страницы
async def process_page(session: aiohttp.ClientSession, page_num: int, semaphore: asyncio.Semaphore,
                       processed_urls: Set[str]) -> int:
    """Обрабатывает одну страницу архива"""
    base_url = "https://primamedia.ru"
    page_url = f"https://primamedia.ru/news/?page={page_num}"
    logger.info(f"\n Страница {page_num}: {page_url}")

    # Получаем ссылки
    news_paths = await get_news_links_from_page(session, page_url, semaphore)
    if not news_paths:
        return 0

    page_saved = 0
    tasks = []

    for path in news_paths:
        article_url = urljoin(base_url, path)
        if primamedia_url_exists(article_url) or article_url in processed_urls:
            continue

        processed_urls.add(article_url)
        tasks.append(parse_primamedia_article(session, article_url, semaphore))

    # Выполняем параллельно с ограничением семафором
    results = await asyncio.gather(*tasks, return_exceptions=True)
    page_saved = sum(1 for r in results if r is True)

    logger.info(f"   Сохранено на странице: {page_saved}")
    return page_saved


async def parse_primamedia_news_async(max_concurrent: int = 5, max_pages: int = 300):
    """Основная асинхронная функция парсинга"""
    semaphore = asyncio.Semaphore(max_concurrent)
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
    timeout = aiohttp.ClientTimeout(total=30)

    total_saved = 0
    processed_urls: Set[str] = set()

    logger.info(" Начинаем асинхронный парсинг статей...")

    async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
    ) as session:

        for page in range(1, max_pages + 1):
            page_saved = await process_page(session, page, semaphore, processed_urls)
            total_saved += page_saved

            if page_saved == 0 and page > 3:
                logger.info("  Похоже, дальше нет новых статей, выходим.")
                break

            # Пауза между страницами
            await asyncio.sleep(2)

    logger.info(f"\n ГОТОВО! Сохранено {total_saved} статей в primamedia.db")
    return total_saved