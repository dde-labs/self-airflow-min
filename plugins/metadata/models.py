from __future__ import annotations

from sqlalchemy import Column
from sqlalchemy.types import Boolean, DateTime, Integer, String

from plugins.db import BaseLog, BaseWatermark
from plugins.utils import get_dt_now


class Streams(BaseWatermark):
    __tablename__ = "streams"
    __table_args__ = {"extend_existing": True}

    strem_nm = Column(String, primary_key=True)
    asat_dt = Column(DateTime, index=False)
    nxt_asat_dt = Column(DateTime, index=False)
    prv_calc_asat_dt = Column(DateTime, index=False)
    calc_asat_dt = Column(DateTime, index=False)
    strem_f = Column(Boolean, default=True)
    act_f = Column(Boolean, default=True)
    updt_dttm = Column(DateTime, default=get_dt_now)
    updt_by = Column(String, default="INITIAL")


class ProcessDeps(BaseWatermark):
    __tablename__ = "process_deps"
    __table_args__ = {"extend_existing": True}

    prcs_nm = Column(String, primary_key=True)
    dpnd_prcs_nm = Column(String, primary_key=True)
    act_f = Column(Boolean, default=True)
    updt_dttm = Column(DateTime, default=get_dt_now)
    updt_by = Column(String, default="INITIAL")


class StreamLogs(BaseLog):
    __tablename__ = "stream_logs"
    __table_args__ = {"extend_existing": True}

    strem_nm = Column(String(128), primary_key=True)
    strem_id = Column(Integer, primary_key=True)
    airflow_run_id = Column(String(128))
    asat_dt = Column(DateTime, nullable=False)
    strt_dttm = Column(DateTime, nullable=False)
    end_dttm = Column(DateTime, nullable=True)
    st = Column(String, nullable=False)
    rmrk = Column(String, nullable=True)
    updt_dttm = Column(DateTime, default=get_dt_now)
    updt_by = Column(String, default="INITIAL")


class ProcessLogs(BaseLog):
    __tablename__ = "process_logs"
    __table_args__ = {"extend_existing": True}

    prcs_nm = Column(String(128), primary_key=True)
    ld_id = Column(Integer, primary_key=True)
    strem_id = Column(Integer, nullable=False)
    airflow_run_id = Column(String(128), nullable=False)
    asat_dt = Column(DateTime, nullable=False)
    strt_dttm = Column(DateTime, nullable=False)
    end_dttm = Column(DateTime, nullable=True)
    cnt_rec_cntl = Column(Integer, default=None, nullable=True)
    cnt_rec_src = Column(Integer, default=None, nullable=True)
    cnt_rec_tgt = Column(Integer, default=None, nullable=True)
    cnt_fi_src = Column(Integer, default=None, nullable=True)
    cnt_fi_tgt = Column(Integer, default=None, nullable=True)
    st = Column(String, nullable=False)
    rmrk = Column(String, nullable=True)
    fail_airflow_run_id = Column(String, default=None, nullable=True)
    updt_dttm = Column(DateTime, default=get_dt_now)
    updt_by = Column(String, default="INITIAL")
