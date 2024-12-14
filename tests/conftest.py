import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from plugins.db import BaseLog, BaseWatermark

# NOTE: Show all deprecation warnings
os.environ["SQLALCHEMY_WARN_20"] = "1"

engine = create_engine(
    f"sqlite:///{Path(__file__).parent.resolve()}/watermark.test.sqlite",
    pool_pre_ping=True,
    connect_args={"check_same_thread": True},
)
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    bind=engine,
)

engine_log = create_engine(
    f"sqlite:///{Path(__file__).parent.resolve()}/logging.test.sqlite",
    pool_pre_ping=True,
    connect_args={"check_same_thread": True},
)
SessionLocalLog = sessionmaker(
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    bind=engine_log,
)


@pytest.fixture(scope="session")
def get_session() -> Iterator[sessionmaker]:
    if not engine.url.get_backend_name() == "sqlite":
        raise RuntimeError("Use SQLite backend to run tests")

    BaseWatermark.metadata.create_all(bind=engine)
    BaseWatermark.metadata.bind = engine

    BaseLog.metadata.create_all(bind=engine_log)
    BaseLog.metadata.bind = engine_log

    try:
        yield SessionLocal
    finally:
        BaseWatermark.metadata.drop_all(bind=engine)


@pytest.fixture(scope="session")
def get_session_log() -> Iterator[sessionmaker]:
    if not engine_log.url.get_backend_name() == "sqlite":
        raise RuntimeError("Use SQLite backend to run tests")

    BaseLog.metadata.create_all(bind=engine_log)
    BaseLog.metadata.bind = engine_log

    try:
        yield SessionLocalLog
    finally:
        BaseLog.metadata.drop_all(bind=engine_log)


@pytest.fixture(scope="function")
def session(get_session) -> Iterator[Session]:
    s = get_session()
    try:
        yield s
    finally:
        s.close()
