import asyncio
from database import init_database, get_primamedia_stats
from primamedia_parser import parse_primamedia_news_async

async def main():
    print("=== АСИНХРОННЫЙ ПАРСЕР PRIMA MEDIA ===")
    print("1. Создание базы primamedia.db...")
    init_database()

    print("\n2. Асинхронный парсинг новостей (5 потоков)...")
    total_saved = await parse_primamedia_news_async(max_concurrent=5)

    print("\n3. Финальная статистика:")
    total, avg_len = get_primamedia_stats()
    print(f"   Всего статей в таблице primamedia: {total}")
    print(f"   Средняя длина текста: {avg_len} символов")
    print(f"   Файл: primamedia.db")
    print(f"   ⚡ Сохранено за сессию: {total_saved}")

if __name__ == "__main__":
    asyncio.run(main())