from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


StatusLiteral = Literal['SUCCESS', 'FAILED', 'START', 'SKIPPED']


class StreamWatermark(BaseModel):
    name: str = Field(description="Stream name")
    asat_dt: datetime
    nxt_asat_dt: datetime
    prv_asat_dt: datetime
    calc_asat_dt: datetime
    strem_f: bool = Field(default=True)


class StreamLog:
    name: str = Field(description="Stream name")
    strem_id: int = Field(description="")
    airflow_run_id: str
    asat_dt: datetime
    strt_dttm: datetime
    end_dttm: datetime
    st: StatusLiteral
    rmrk: Optional[str] = Field(
        default=None, description="Remark for this logging"
    )
    updt_dttm: datetime
    updt_by: str = Field(default='MANUAL')


class ProcessLog:
    name: str = Field(description="Process name")
    ld_id: int = Field(description="")
    strem_id: int = Field(description="")
    airflow_run_id: str
    asat_dt: datetime
    strt_dttm: datetime
    end_dttm: datetime

    cnt_rec_cntl: Optional[int] = None
    cnt_rec_src: Optional[int] = None
    cnt_rec_tgt: Optional[int] = None
    cnt_fi_src: Optional[int] = None
    cnt_fi_tgt: Optional[int] = None

    st: StatusLiteral
    rmrk: Optional[str] = Field(
        default=None, description="Remark for this logging"
    )
    fail_airflow_run_id: Optional[str] = None
    updt_dttm: datetime
    updt_by: str = Field(default='MANUAL')
