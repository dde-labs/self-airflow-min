from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Literal

from sqlalchemy import desc, insert, select, update
from sqlalchemy.sql import and_, func, null, true

import plugins.metadata.models as models
from plugins.db import engine, engine_log, get_session, get_session_log
from plugins.metadata.schemas import (
    Process,
    ProcessLog,
    Stream,
    StreamLog,
    StreamWatermark,
)
from plugins.utils import get_dt_now, tz

logger = logging.getLogger("airflow.task")


def initial_database():
    """Initialize watermark and logging database"""
    models.BaseWatermark.metadata.create_all(bind=engine)

    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode = 'WAL';")
        conn.exec_driver_sql("PRAGMA foreign_keys = ON")
        conn.exec_driver_sql("PRAGMA page_size = 4096;")
        conn.exec_driver_sql("PRAGMA cache_size = 10000;")
        conn.exec_driver_sql("PRAGMA locking_mode = 'EXCLUSIVE';")
        # conn.exec_driver_sql("PRAGMA locking_mode = 'NORMAL';")
        conn.exec_driver_sql("PRAGMA synchronous = 'NORMAL';")
        conn.exec_driver_sql("PRAGMA temp_store = 'MEMORY';")
        conn.exec_driver_sql("PRAGMA busy_timeout = 5000;")

    models.BaseLog.metadata.create_all(bind=engine_log)

    with engine_log.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode = 'WAL';")
        conn.exec_driver_sql("PRAGMA foreign_keys = ON")
        conn.exec_driver_sql("PRAGMA page_size = 4096;")
        conn.exec_driver_sql("PRAGMA cache_size = 10000;")
        conn.exec_driver_sql("PRAGMA locking_mode = 'EXCLUSIVE';")
        # conn.exec_driver_sql("PRAGMA locking_mode = 'NORMAL';")
        conn.exec_driver_sql("PRAGMA synchronous = 'NORMAL';")
        conn.exec_driver_sql("PRAGMA temp_store = 'MEMORY';")
        conn.exec_driver_sql("PRAGMA busy_timeout = 5000;")


def get_watermark(
    stream: Stream,
    auto_create: bool = False,
) -> StreamWatermark:
    """Get Watermark of Stream for tracking the latest date that this stream
    start to run for ingest data from source to target.

    :param stream: A stream schema object.
    :param auto_create: An auto create flag.
    :rtype: StreamWatermark
    """
    # NOTE: Import is exception object inside this func because of lazy calling.
    from airflow.exceptions import AirflowFailException

    with get_session() as session:
        db_stream = (
            session.query(models.Streams)
            .filter(
                models.Streams.strem_nm == stream.strem_nm,
                models.Streams.act_f == true(),
            )
            .first()
        )
        if db_stream:
            return StreamWatermark.model_validate(obj=db_stream)

        if not auto_create:
            raise AirflowFailException(
                f"Stream watermark of {stream.strem_nm} does not create."
            )

    with get_session() as session:

        # NOTE: Stream watermark does not exists.
        this_date: datetime = datetime.now(tz=tz).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(days=-1)

        # NOTE: Create pair of asat_dt and calc_asat_dt
        asat_date: datetime = stream.get_asat_dt(this_date, prev=True)
        nxt_asat_dt: datetime = stream.get_asat_dt(asat_date)

        prv_calc_asat_dt: datetime = stream.calc_asat_dt(asat_date)
        calc_asat_dt: datetime = stream.calc_asat_dt(nxt_asat_dt)

        db_stream = models.Streams(
            strem_nm=stream.strem_nm,
            asat_dt=asat_date,
            nxt_asat_dt=nxt_asat_dt,
            prv_calc_asat_dt=prv_calc_asat_dt,
            calc_asat_dt=calc_asat_dt,
            # NOTE: close this stream because it will open in the next step
            strem_f=True,
            act_f=True,
        )
        session.add(db_stream)
        session.commit()
        session.refresh(db_stream)

    return StreamWatermark.model_validate(obj=db_stream)


def open_watermark(stream: Stream) -> None:
    """Mark false on the stream flag column of the table steam watermark.

    :param stream: A stream schema object.
    """
    with get_session() as session:
        session.execute(
            update(models.Streams)
            .where(models.Streams.strem_nm == stream.strem_nm)
            .values(
                {
                    "strem_f": False,
                    "updt_dttm": get_dt_now(only_date=False),
                    "updt_by": "AIRFLOW",
                }
            )
        )
        session.commit()


def shift_watermark(
    stream: Stream,
    watermark: StreamWatermark,
    mode: Literal["N", "F"],
) -> StreamWatermark:
    """Shift the stream watermark to the next running date with settlement
    logic.

    :param stream: A stream schema object.
    :param watermark: A stream watermark schema object.
    :param mode: A running mode. It allow this mode to be only N or F.
    :rtype: StreamWatermark
    """
    # NOTE: Create base updated attributes.
    updated: dict[str, Any] = {
        "strem_f": True,
        "updt_dttm": get_dt_now(only_date=False),
        "updt_by": "AIRFLOW",
    }

    # NOTE: If running mode be normal, it will shift the asat_dt.
    if mode == "N":
        next_date: datetime = stream.get_asat_dt(watermark.nxt_asat_dt)
        updated.update(
            {
                "asat_dt": watermark.nxt_asat_dt,
                "nxt_asat_dt": next_date,
                "prv_calc_asat_dt": watermark.calc_asat_dt,
                "calc_asat_dt": stream.calc_asat_dt(watermark.nxt_asat_dt),
            }
        )

    with get_session() as session:
        session.execute(
            update(models.Streams)
            .where(models.Streams.strem_nm == stream.strem_nm)
            .values(updated)
        )
        session.commit()

        db_stream = (
            session.query(models.Streams).filter(
                models.Streams.strem_nm == stream.strem_nm
            )
        ).first()

    return StreamWatermark.model_validate(obj=db_stream)


def check_log_stream_remark(
    strem_id: int,
    asat_dt: datetime,
    stream: Stream,
):
    with get_session_log() as session:
        db_stream_log = session.execute(
            select(models.StreamLogs).where(
                models.StreamLogs.strem_nm == stream.strem_nm,
                models.StreamLogs.strem_id == strem_id,
                models.StreamLogs.asat_dt == asat_dt,
            )
        ).scalar_one_or_none()

    # NOTE: Chain error when stream failed multiple-tasks
    if db_stream_log and db_stream_log.st == "FAILED":
        return db_stream_log.rmrk or "Failed with no log"
    return None


def update_log_stream(
    strem_id: int,
    asat_dt: datetime,
    stream: Stream,
    update_values: dict[str, Any] | None = None,
    *,
    refresh: bool = False,
) -> StreamLog | None:
    """Update value to existing log data on the stream logging table."""
    update_values: dict[str, Any] = update_values or {}

    if update_values.get("rmrk"):
        update_values["rmrk"] = (
            select(
                (
                    func.coalesce(models.StreamLogs.rmrk, "")
                    + f"||{update_values.get('rmrk')}"
                ),
            )
            .where(
                models.StreamLogs.strem_nm == stream.strem_nm,
                models.StreamLogs.strem_id == strem_id,
                models.StreamLogs.asat_dt == asat_dt,
            )
            .scalar_subquery()
        )

    with get_session_log() as session:
        session.execute(
            update(models.StreamLogs)
            .where(
                models.StreamLogs.strem_nm == stream.strem_nm,
                models.StreamLogs.strem_id == strem_id,
                models.StreamLogs.asat_dt == asat_dt,
            )
            .values(
                {
                    "end_dttm": null(),
                    "st": "SUCCESS",
                    "rmrk": null(),
                    "updt_dttm": get_dt_now(only_date=False),
                    "updt_by": "AIRFLOW",
                }
                | update_values
            )
        )
        session.commit()

    if not refresh:
        return None

    with get_session_log() as session:
        db_stream = (
            session.query(models.StreamLogs).filter(
                models.StreamLogs.strem_nm == stream.strem_nm,
                models.StreamLogs.strem_id == strem_id,
                models.StreamLogs.asat_dt == asat_dt,
            )
        ).first()

    return StreamLog.model_validate(obj=db_stream)


def finish_log_stream(
    strem_id: int,
    asat_dt: datetime,
    stream: Stream,
    update_values: dict[str, Any] | None = None,
):
    update_values: dict[str, Any] = update_values or {}
    return update_log_stream(
        strem_id=strem_id,
        asat_dt=asat_dt,
        stream=stream,
        update_values={
            "end_dttm": get_dt_now(only_date=False),
        }
        | update_values,
    )


def create_log_stream(
    airflow_run_id: str,
    stream: Stream,
    watermark: StreamWatermark,
) -> StreamLog:
    """Update the Stream logging."""
    with get_session_log() as session:
        sub_query = (
            select(
                func.coalesce(func.max(models.StreamLogs.strem_id), 0)
                .op("+")(1)
                .label("strem_id")
            )
            .where(models.StreamLogs.strem_nm == stream.strem_nm)
            .scalar_subquery()
        )
        stmt = insert(models.StreamLogs).values(
            strem_nm=stream.strem_nm,
            strem_id=sub_query,
            airflow_run_id=airflow_run_id,
            asat_dt=watermark.asat_dt,
            strt_dttm=get_dt_now(only_date=False),
            end_dttm=null(),
            st="START",
            rmrk=null(),
            updt_dttm=get_dt_now(only_date=False),
            updt_by="AIRFLOW",
        )
        session.execute(stmt)
        session.commit()

    with get_session_log() as session:
        strem_id: int = (
            session.query(
                func.coalesce(func.max(models.StreamLogs.strem_id), 0).label(
                    "strem_id"
                )
            )
            .filter(
                models.StreamLogs.strem_nm == stream.strem_nm,
                models.StreamLogs.airflow_run_id == airflow_run_id,
            )
            .first()
        )["strem_id"]

        logger.info(f"Start this stream with ID: {strem_id}({type(strem_id)})")

        db_stream = session.execute(
            select(models.StreamLogs)
            .where(
                models.StreamLogs.strem_nm == stream.strem_nm,
                models.StreamLogs.strem_id == strem_id,
            )
            .limit(1)
        ).scalar_one()

    return StreamLog.model_validate(obj=db_stream)


def update_log_process(
    ld_id: int,
    asat_dt: datetime,
    process: Process,
    update_values: dict[str, Any] | None = None,
    *,
    refresh: bool = False,
) -> ProcessLog:
    update_values: dict[str, Any] = update_values or {}
    with get_session_log() as session:
        session.execute(
            update(models.ProcessLogs)
            .where(
                models.ProcessLogs.prcs_nm == process.prcs_nm,
                models.ProcessLogs.ld_id == ld_id,
                models.ProcessLogs.asat_dt == asat_dt,
            )
            .values(
                {
                    "end_dttm": null(),
                    "cnt_rec_cntl": null(),
                    "cnt_rec_src": null(),
                    "cnt_rec_tgt": null(),
                    "cnt_fi_src": null(),
                    "cnt_fi_tgt": null(),
                    "st": "SUCCESS",
                    "rmrk": null(),
                    "fail_airflow_run_id": null(),
                    "updt_dttm": get_dt_now(only_date=False),
                    "updt_by": "AIRFLOW",
                }
                | update_values
            )
        )
        session.commit()

    if not refresh:
        return

    with get_session_log() as session:
        db_process = (
            session.query(models.ProcessLogs).filter(
                models.ProcessLogs.prcs_nm == process.prcs_nm,
                models.ProcessLogs.ld_id == ld_id,
                models.ProcessLogs.asat_dt == asat_dt,
            )
        ).first()

    return ProcessLog.model_validate(obj=db_process)


def finish_log_process(
    ld_id: int,
    asat_dt: datetime,
    process: Process,
    update_values: dict[str, Any] | None = None,
) -> ProcessLog:
    update_values: dict[str, Any] = update_values or {}
    return update_log_process(
        ld_id=ld_id,
        asat_dt=asat_dt,
        process=process,
        update_values={
            "end_dttm": get_dt_now(only_date=False),
        }
        | update_values,
    )


def create_log_process(
    airflow_run_id: str,
    asat_dt: datetime,
    strem_id: int,
    process: Process,
) -> ProcessLog:
    with get_session_log() as session:
        sub_query = (
            select(
                func.coalesce(func.max(models.ProcessLogs.ld_id), 0)
                .op("+")(1)
                .label("ld_id")
            )
            .where(models.ProcessLogs.prcs_nm == process.prcs_nm)
            .scalar_subquery()
        )
        stmt = insert(models.ProcessLogs).values(
            prcs_nm=process.prcs_nm,
            ld_id=sub_query,
            strem_id=strem_id,
            airflow_run_id=airflow_run_id,
            asat_dt=asat_dt,
            strt_dttm=get_dt_now(only_date=False),
            end_dttm=null(),
            cnt_rec_cntl=null(),
            cnt_rec_src=null(),
            cnt_rec_tgt=null(),
            cnt_fi_src=null(),
            cnt_fi_tgt=null(),
            st="START",
            rmrk=null(),
            fail_airflow_run_id=null(),
            updt_dttm=get_dt_now(only_date=False),
            updt_by="AIRFLOW",
        )
        session.execute(stmt)
        session.commit()

    # TODO: Start new session with read-only mode
    with get_session_log() as session:
        ld_id: int = (
            session.query(
                func.coalesce(func.max(models.ProcessLogs.ld_id), 0).label(
                    "ld_id"
                )
            )
            .filter(
                models.ProcessLogs.prcs_nm == process.prcs_nm,
                models.ProcessLogs.airflow_run_id == airflow_run_id,
            )
            .first()
        )["ld_id"]

        logger.info(f"Start this process with {ld_id}({type(ld_id)})")

        db_process = session.execute(
            select(models.ProcessLogs)
            .where(
                models.ProcessLogs.prcs_nm == process.prcs_nm,
                models.ProcessLogs.ld_id == ld_id,
            )
            .limit(1)
        ).scalar_one()

    return ProcessLog.model_validate(obj=db_process)


def check_process_deps(
    stream: Stream,
    process: Process,
    asat_dt: datetime,
) -> list[ProcessLog]:
    """Check process dependency allow current process running."""

    # NOTE: Get the active process dependency.
    with get_session() as session:
        prcs_deps: list[models.ProcessDeps] = (
            session.query(models.ProcessDeps)
            .where(
                models.ProcessDeps.prcs_nm == process.prcs_nm,
                models.ProcessDeps.act_f == true(),
            )
            .all()
        )

    if len(prcs_deps) == 0:
        return []

    dpnd_asat_dt: datetime = asat_dt + timedelta(days=stream.dpnd_stlmt_dt + 1)
    dpnd_prcs_nms: list[str] = [p.dpnd_prcs_nm for p in prcs_deps]

    # NOTE: Start Getting latest status with dependency settlement date.
    with get_session_log() as session:
        subquery = (
            select(
                models.ProcessLogs.prcs_nm,
                func.max(models.ProcessLogs.ld_id).label("max_ld_id"),
            )
            .where(
                models.ProcessLogs.prcs_nm.in_(dpnd_prcs_nms),
                models.ProcessLogs.asat_dt == dpnd_asat_dt,
            )
            .group_by(models.ProcessLogs.prcs_nm)
        ).subquery()

        statement = (
            select(models.ProcessLogs).join(
                subquery,
                and_(
                    models.ProcessLogs.prcs_nm == subquery.c.prcs_nm,
                    models.ProcessLogs.ld_id == subquery.c.max_ld_id,
                ),
            )
            # NOTE: remove this condition because I cannot use left join in orm
            #   statement.
            # ---
            # .where(
            #     or_(
            #         models.ProcessLogs.st.notin_(["SUCCESS", "SKIPPED"]),
            #         models.ProcessLogs.st == null(),
            #     )
            # )
        )
        prcs_logs_mapping: dict[str, models.ProcessLogs] = {
            pl.prcs_nm: pl for pl in session.execute(statement).scalars().all()
        }

    rs: list[ProcessLog] = []
    for prcs in dpnd_prcs_nms:
        if prcs in prcs_logs_mapping:
            prcs_log: ProcessLog = prcs_logs_mapping[prcs]
            if prcs_log.st is None or prcs_log.st not in ("SUCCESS", "SKIPPED"):
                rs.append(ProcessLog.model_validate(prcs_logs_mapping[prcs]))
            else:
                # NOTE: Skip to making failed if status is success or skipped.
                continue
        else:
            rs.append(
                ProcessLog.model_validate(
                    {
                        "prcs_nm": prcs,
                        "ld_id": 0,
                        "strem_id": 0,
                        "airflow_run_id": "",
                        "asat_dt": dpnd_asat_dt,
                        "strt_dttm": dpnd_asat_dt,
                        "st": "FAILED",
                        "rmrk": "Does not run this process yet.",
                        "updt_dttm": get_dt_now(only_date=False),
                    }
                )
            )
    return rs


def get_process_log_from_stream(
    strem_id: int,
    asat_dt: datetime,
) -> list[ProcessLog]:
    with get_session_log() as session:
        db_prcs_logs: list[models.ProcessLogs] = list(
            session.execute(
                select(models.ProcessLogs)
                .where(
                    models.ProcessLogs.strem_id == strem_id,
                    models.ProcessLogs.asat_dt == asat_dt,
                )
                .order_by(desc(models.ProcessLogs.strt_dttm))
            )
            .scalars()
            .all()
        )

    return [
        ProcessLog.model_validate(db_prcs_log) for db_prcs_log in db_prcs_logs
    ]
