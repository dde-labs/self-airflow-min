import sqlite3
from contextlib import closing


conn = sqlite3.connect('./data/metadata.db')
conn.execute('PRAGMA page_size = 4096;')
conn.execute('PRAGMA cache_size = 10000;')
conn.execute("PRAGMA locking_mode = 'EXCLUSIVE';")
conn.execute("PRAGMA synchronous = 'NORMAL';")
conn.execute("PRAGMA journal_mode = 'WAL'")
conn.execute('PRAGMA cache_size = 5000;')


if __name__ == '__main__':
    with closing(conn.cursor()) as cursor:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        print(cursor.fetchall())

    conn.close()
