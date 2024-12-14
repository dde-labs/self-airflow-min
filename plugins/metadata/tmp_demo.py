import time
from datetime import datetime, timedelta

from pydantic import TypeAdapter
from sqlalchemy import insert, select, update
from sqlalchemy.sql import and_, func, null, or_

import plugins.metadata.models as models
import plugins.metadata.schemas as schemas
from plugins.metadata.services import engine, get_session, get_session_log
from plugins.utils import get_dt_now


def create_stream(name: str):
    with get_session() as db:
        db_stream = models.Streams(
            strem_nm=name,
            asat_dt=datetime(2024, 8, 30, 1, 1),
            nxt_asat_dt=datetime(2024, 8, 30, 1, 1),
            prv_calc_asat_dt=datetime(2024, 8, 30, 1, 1),
            calc_asat_dt=datetime(2024, 8, 30, 1, 1),
            strem_f=False,
        )
        db.add(db_stream)
        db.commit()
        db.refresh(db_stream)
    return db_stream


def read_stream(name: str):
    with get_session() as db:
        return (
            db.query(models.Streams)
            .filter(models.Streams.strem_nm == name)
            .first()
        )


def read_stream_with_select(name: str):
    with get_session() as db:
        return db.execute(
            select(models.Streams)
            .filter(models.Streams.strem_nm == name)
            .limit(1)
        ).scalar_one()


def read_streams(skip: int = 0, limit: int = 100):
    with get_session() as db:
        return db.query(models.Streams).offset(skip).limit(limit).all()


def update_stream(name: str):
    with get_session() as db:
        (
            db.query(models.Streams)
            .filter(models.Streams.strem_nm == name)
            .update(
                {
                    "asat_dt": datetime.now(),
                    "nxt_asat_dt": datetime.now(),
                    "prv_calc_asat_dt": datetime.now(),
                    "calc_asat_dt": datetime.now(),
                    "strem_f": True,
                }
            )
        )
        db.commit()


def update_stream_with_returning(name: str):
    with get_session() as db:
        db.execute(
            update(models.Streams)
            .where(models.Streams.strem_nm == name)
            .values(
                {
                    "asat_dt": datetime.now(),
                    "nxt_asat_dt": datetime.now(),
                    "prv_calc_asat_dt": datetime.now(),
                    "calc_asat_dt": datetime.now(),
                    "strem_f": True,
                }
            )
        )
        # db.commit()
        # db_stream = db.execute(
        #     select(models.Streams)
        #     .where(models.Streams.strem_nm == name)
        # ).scalar()

        db_stream = (
            db.query(models.Streams).filter(models.Streams.strem_nm == name)
        ).first()

    print(schemas.StreamWatermark.model_validate(obj=db_stream))


StreamWatermarks = TypeAdapter(list[schemas.StreamWatermark])


def create_stream_log():
    with get_session_log() as db:
        db_stream_log = models.StreamLogs(
            strem_nm="S_AD_D",
            strem_id=1,
            airflow_run_id="",
            asat_dt=datetime.now(),
            strt_dttm=datetime.now(),
            end_dttm=datetime.now(),
            st="START",
        )
        db.add(db_stream_log)
        db.commit()
        db.refresh(db_stream_log)
    return db_stream_log


def create_max_stream_id_log():
    with get_session_log() as session:
        strem_id: int = (
            session.query(
                func.coalesce(func.max(models.StreamLogs.strem_id), 0).label(
                    "strem_id"
                )
            )
            .filter(models.StreamLogs.strem_nm == "S_AD_D_TMP")
            .first()
        )["strem_id"] + 1

        db_stream = models.StreamLogs(
            strem_nm="S_AD_D_TMP",
            strem_id=strem_id,
            airflow_run_id="demo",
            asat_dt=get_dt_now(),
            strt_dttm=get_dt_now(only_date=False),
            end_dttm=null(),
            st="START",
            rmrk=null(),
            updt_dttm=get_dt_now(only_date=False),
            updt_by="AIRFLOW",
        )
        session.add(db_stream)
        session.commit()
        session.refresh(db_stream)

    stream_log = schemas.StreamLog.model_validate(obj=db_stream)
    print(stream_log)


def create_max_stream_id_log_insert():
    with get_session_log() as session:
        sub_query = (
            select(
                func.coalesce(func.max(models.StreamLogs.strem_id), 0)
                .op("+")(1)
                .label("strem_id")
            )
            .where(models.StreamLogs.strem_nm == "S_AD_D_TMP")
            .scalar_subquery()
        )
        stmt = insert(models.StreamLogs).values(
            strem_nm="S_AD_D_TMP",
            strem_id=sub_query,
            airflow_run_id="demo",
            asat_dt=get_dt_now(),
            strt_dttm=get_dt_now(only_date=False),
            end_dttm=null(),
            st="START",
            rmrk=null(),
            updt_dttm=get_dt_now(only_date=False),
            updt_by="AIRFLOW",
        )
        print(stmt)
        print(time.time())
        try:
            session.execute(stmt)
            session.commit()
        finally:
            print(time.time())
        # session.refresh(db_stream)

    # stream_log = schemas.StreamLog.model_validate(obj=db_stream)
    # print(stream_log)


def delete_stream_log(name: str):
    with get_session_log() as db:
        db.query(models.StreamLogs).filter_by(strem_nm=name).delete()
        db.commit()


def get_deps_process_log():
    dpnd_prcs_nms = ["P_AD_API_01_D", "P_AD_GBQ_TABLE_01_DD"]
    dpnd_asat_dr = datetime(2024, 9, 13) + timedelta(days=-1 + 1)

    with get_session_log() as session:
        subquery = (
            select(
                models.ProcessLogs.prcs_nm,
                func.max(models.ProcessLogs.ld_id).label("max_ld_id"),
            )
            .where(
                models.ProcessLogs.prcs_nm.in_(dpnd_prcs_nms),
                models.ProcessLogs.asat_dt == dpnd_asat_dr,
            )
            .group_by(models.ProcessLogs.prcs_nm)
        ).subquery()

        statement = (
            select(models.ProcessLogs)
            .join(
                subquery,
                and_(
                    models.ProcessLogs.prcs_nm == subquery.c.prcs_nm,
                    models.ProcessLogs.ld_id == subquery.c.max_ld_id,
                ),
            )
            .where(
                or_(
                    models.ProcessLogs.st.notin_(["SUCCESS", "SKIPPED"]),
                    models.ProcessLogs.st == null(),
                )
            )
        )
        print(statement)
        prcs_logs_mapping: dict[str, models.ProcessLogs] = {
            pl.prcs_nm: pl for pl in session.execute(statement).scalars().all()
        }

    print(prcs_logs_mapping)


def set_database_setting():
    with engine.connect() as conn:
        rs = conn.execute("PRAGMA locking_mode")
        print(rs.fetchone())

        # conn.exec_driver_sql("PRAGMA locking_mode = 'EXCLUSIVE';")
        # print(conn.execute('PRAGMA locking_mode').fetchone())
        print(conn.execute("PRAGMA journal_mode").fetchone())
        print(conn.execute("PRAGMA synchronous").fetchone())
        print(conn.execute("PRAGMA temp_store").fetchone())
        print(conn.execute("PRAGMA busy_timeout").fetchone())

        # conn.exec_driver_sql("PRAGMA busy_timeout = 5000;")
        # print(conn.execute('PRAGMA busy_timeout').fetchone())


def get_detail_if_database_locked():
    with get_session() as session:
        session.commit()


def update_stream_rmrk():
    with get_session_log() as session:
        update_values = {
            "rmrk": "suffix",
            "st": "FAILED",
        }
        subquery = (
            select(
                func.coalesce(models.StreamLogs.rmrk, "")
                + f"||{update_values.get('rmrk')}",
            )
            .where(
                models.StreamLogs.strem_nm == "S_AD_D_TMP",
                models.StreamLogs.strem_id == 1,
            )
            .scalar_subquery()
        )

        update_values["rmrk"] = subquery

        stmt = (
            update(models.StreamLogs)
            .where(
                models.StreamLogs.strem_nm == "S_AD_D_TMP",
                models.StreamLogs.strem_id == 1,
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
        print(stmt)
        session.execute(stmt)
        session.commit()


if __name__ == "__main__":
    # create_stream(name='S_AD_TMP')
    # ---
    # rs = read_stream(name='S_AD_D')
    # print(schemas.StreamWatermark.model_validate(rs))
    # ---
    # rs = read_streams()
    # print(StreamWatermarks.validate_python(rs))
    # ---
    # update_stream(name='S_AD_D')
    # ---
    # create_stream_log()
    # ---
    # delete_stream_log(name='S_AD_D')
    # ---
    # create_max_stream_id_log()
    # ---
    # update_stream_with_returning(name='S_AD_TMP')
    # ---
    # get_deps_process_log()
    # ---
    # rs = read_stream_with_select(name="S_AD_D")
    # print(rs)
    # ---
    # set_database_setting()
    # ---
    # create_max_stream_id_log_insert()
    # ---
    update_stream_rmrk()
