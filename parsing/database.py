import sqlite3
import uuid
from datetime import datetime, timezone

def init_database():
    with sqlite3.connect('primamedia.db') as con:
        cur = con.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS primamedia (
            guid TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            url TEXT UNIQUE NOT NULL,
            published_at TEXT,
            comments_count INTEGER DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            rating INTEGER DEFAULT 0
        )''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_url ON primamedia(url)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_created ON primamedia(created_at_utc)')
        con.commit()
    print(" База primamedia.db с таблицей primamedia создана!")

def save_to_primamedia(article_data):
    with sqlite3.connect('primamedia.db') as con:
        cur = con.cursor()
        guid = str(uuid.uuid4())
        created_at_utc = datetime.now(timezone.utc).isoformat()
        cur.execute('''INSERT OR IGNORE INTO primamedia 
                       (guid, title, description, url, published_at, 
                        comments_count, created_at_utc, rating)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                    (guid,
                     article_data['title'][:255],
                     article_data['description'],
                     article_data['url'],
                     article_data.get('published_at'),
                     article_data.get('comments_count', 0),
                     created_at_utc,
                     article_data.get('rating', 0)))
        con.commit()
        return guid

def primamedia_url_exists(url):
    with sqlite3.connect('primamedia.db') as con:
        cur = con.cursor()
        cur.execute("SELECT 1 FROM primamedia WHERE url=?", (url,))
        return cur.fetchone() is not None

def get_primamedia_stats():
    with sqlite3.connect('primamedia.db') as con:
        cur = con.cursor()
        total = cur.execute("SELECT COUNT(*) FROM primamedia").fetchone()[0]
        avg_len = cur.execute("SELECT AVG(LENGTH(description)) FROM primamedia").fetchone()[0]
        return total, int(avg_len or 0)
