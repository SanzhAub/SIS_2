import sqlite3
import json
import os

DB_PATH = "data/output.db"
JSON_PATH = "data/cleaned_manga.json"


def create_table(conn):
    """Создаём таблицу, если её ещё нет"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS manga (
            manga_id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            year INTEGER,
            rating REAL,
            has_cover INTEGER,
            cover_url TEXT,
            url TEXT,
            scraped_at TEXT
        );
    """)
    conn.commit()


def load_data_to_db():
    """Загрузка очищенных данных в SQLite"""
    if not os.path.exists(JSON_PATH):
        print("Файл cleaned_manga.json не найден. Сначала запусти cleaner.py")
        return

    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = sqlite3.connect(DB_PATH)
    create_table(conn)

    cursor = conn.cursor()

    inserted = 0
    for item in data:
        cursor.execute("""
            INSERT OR REPLACE INTO manga
            (manga_id, title, description, year, rating, has_cover, cover_url, url, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item.get("manga_id"),
            item.get("title"),
            item.get("description"),
            item.get("year"),
            item.get("rating"),
            1 if item.get("has_cover") else 0,
            item.get("cover_url"),
            item.get("url"),
            item.get("scraped_at")
        ))
        inserted += 1

    conn.commit()
    conn.close()

    print(f"Данные успешно записаны в SQLite! Всего записей: {inserted}")


if __name__ == "__main__":
    load_data_to_db()
