from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from queue import Queue

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

session_queue = Queue()
internal: Path = Path(__file__).parent.parent / "logs/internal/db"
internal.mkdir(parents=True, exist_ok=True)


class DatabaseManageException(Exception): ...


engine = create_engine(
    (
        f"sqlite:///{Path(__file__).parent.parent.resolve()}"
        f"/logs/internal/watermark.sqlite"
    ),
    pool_pre_ping=True,
    connect_args={"check_same_thread": True},
)
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    bind=engine,
)
BaseWatermark = declarative_base(bind=engine, name="BaseWatermark")


@event.listens_for(engine, "connect")
def set_pragma(dbapi_connection, connection_record):
    """Read more:"""
    cursor = dbapi_connection.cursor()

    cursor.execute("PRAGMA synchronous = 'OFF';")
    cursor.execute("PRAGMA journal_mode = 'WAL';")
    cursor.execute("PRAGMA locking_mode = 'NORMAL';")

    # NOTE: The best practice
    # cursor.execute("PRAGMA synchronous = 'NORMAL';")
    # cursor.execute("PRAGMA journal_mode = 'WAL';")
    # cursor.execute("PRAGMA locking_mode = 'EXCLUSIVE';")

    # NOTE: Low ACID
    # cursor.execute("PRAGMA synchronous = 'OFF';")
    # cursor.execute("PRAGMA journal_mode = 'OFF';")
    # cursor.execute("PRAGMA locking_mode = 'NORMAL';")

    # cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA foreign_keys = OFF")

    cursor.execute("PRAGMA page_size = 4096;")
    cursor.execute("PRAGMA cache_size = 10000;")
    cursor.execute("PRAGMA temp_store = 'MEMORY';")
    cursor.execute("PRAGMA busy_timeout = 150000;")
    cursor.close()


engine_log = create_engine(
    (
        f"sqlite:///{Path(__file__).parent.parent.resolve()}"
        f"/logs/internal/logging.sqlite"
    ),
    pool_pre_ping=True,
    connect_args={"check_same_thread": True},
)
SessionLocalLog = sessionmaker(
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    bind=engine_log,
)
BaseLog = declarative_base(bind=engine_log, name="BaseLog")


@event.listens_for(engine_log, "connect")
def set_log_pragma(dbapi_connection, connection_record):
    """Read more:"""
    # if type(dbapi_connection) is sqlite3.Connection:
    cursor = dbapi_connection.cursor()

    # NOTE: ref https://forum.qt.io/topic/139657/multithreading-with-sqlite/7
    cursor.execute("PRAGMA synchronous = 'OFF';")
    cursor.execute("PRAGMA journal_mode = 'WAL';")
    cursor.execute("PRAGMA locking_mode = 'NORMAL';")

    # NOTE: The best practice
    # cursor.execute("PRAGMA synchronous = 'NORMAL';")
    # cursor.execute("PRAGMA journal_mode = 'WAL';")
    # cursor.execute("PRAGMA locking_mode = 'EXCLUSIVE';")

    # NOTE: Low ACID
    # cursor.execute("PRAGMA synchronous = 'OFF';")
    # cursor.execute("PRAGMA journal_mode = 'OFF';")
    # cursor.execute("PRAGMA locking_mode = 'NORMAL';")

    # cursor.execute("PRAGMA foreign_keys = ON")
    cursor.execute("PRAGMA foreign_keys = OFF")
    cursor.execute("PRAGMA page_size = 4096;")
    cursor.execute("PRAGMA cache_size = 10000;")
    cursor.execute("PRAGMA temp_store = 'MEMORY';")
    cursor.execute("PRAGMA busy_timeout = 150000;")
    cursor.close()

    # (
    #     internal
    #     / (
    #         f"db_{datetime.now():%Y%m%d%H%M%S%f}.{threading.get_ident()}"
    #         f".connect.log"
    #     )
    # ).touch()


@event.listens_for(engine_log, "close")
def set_log_pragma_disconnect(dbapi_connection, connection_record):
    if type(dbapi_connection) is sqlite3.Connection:
        # (
        #     internal
        #     / (
        #         f"db_{datetime.now():%Y%m%d%H%M%S%f}.{threading.get_ident()}"
        #         f".close.log"
        #     )
        # ).touch()
        pass


@event.listens_for(engine_log, "before_cursor_execute")
def before_cursor_log_execute(
    conn, cursor, statement, parameters, context, executemany
):
    fname: str = (
        f"db_{datetime.now():%Y%m%d%H%M%S%f}.{threading.get_ident()}"
        f".cursor_exec.log"
    )
    conn.info.setdefault("query_start_time", []).append(time.time())
    conn.info.setdefault("names", []).append(fname)
    with (internal / fname).open(mode="w") as f:
        f.write(statement)


@event.listens_for(engine_log, "after_cursor_execute")
def after_cursor_log_execute(
    conn, cursor, statement, parameters, context, executemany
):
    fname = conn.info["names"].pop(-1)
    total = time.time() - conn.info["query_start_time"].pop(-1)
    with (internal / fname).open(mode="a") as f:
        f.write(f"\nQuery Complete! Total Time: {total}")


class DBSessionManager:
    def __init__(self):
        self._engine: Engine | None = None
        self._sessionmaker: sessionmaker | None = None
        self.engine_use = False

    def init(self, host: str):
        self._engine = create_engine(
            host,
            echo=False,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False, "timeout": 40},
        )
        self._sessionmaker = sessionmaker(
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
            bind=self._engine,
        )

    def is_used(self) -> bool:
        return self.engine_use

    def close(self):
        if self._engine is None:
            raise DatabaseManageException(
                "DatabaseSessionManager is not initialized"
            )
        self._engine.dispose()
        self.engine_use = False

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        if self._engine is None:
            raise DatabaseManageException(
                "DatabaseSessionManager is not initialized"
            )

        while not self.engine_use:
            time.sleep(0.2)

        self.engine_use = True
        with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                # connection.rollback()
                raise
        self.engine_use = False

    @contextmanager
    def session(self) -> Iterator[Session]:
        if self._sessionmaker is None:
            raise DatabaseManageException(
                "DatabaseSessionManager is not initialized"
            )

        session = self._sessionmaker()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


session_watermark = DBSessionManager()
session_log = DBSessionManager()


@contextmanager
def get_session() -> Iterator[Session]:
    """Get the metadata database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def get_session_log() -> Iterator[Session]:
    """Get the logging database session."""
    db = SessionLocalLog()
    try:
        yield db
    finally:
        db.close()
