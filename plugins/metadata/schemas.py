from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import Literal, Optional, Self, Any

import yaml
from yaml import CSafeLoader
from pydantic import BaseModel, Field, ConfigDict
from pydantic.functional_validators import field_validator

from plugins.utils import (
    add_date_with_freq, calc_data_date_with_freq, get_dt_now, tz
)


RootPath: Path = Path(__file__).parent.parent.parent
FreqLiteral = Literal["D", "W", "M", "Q", "Y"]
TierLiteral = Literal["CURATED", "MDL", "DMT"]
LoadTypeLiteral = Literal["F", "T", "D", "I", "SCD_D", "SCD_F", "SCD_T"]
FileFormatLiteral = Literal["csv", "json", "parquet", "xlsx"]

FreqMapping: dict[str, str] = {
    "D": "days",
    "W": "weeks",
    "M": "months",
    "Q": "quarters",
    "Y": "years",
}


class Stream(BaseModel):
    """Stream Config Data Model"""
    strem_nm: Optional[str] = Field(
        default=None,
        description="Stream name",
    )
    tier: TierLiteral = Field(description="Tier of stream")
    stlmt_dt: int = Field(gt=-10, le=10, description='Settlement date')
    dpnd_stlmt_dt: int = Field(
        gt=-10,
        le=10,
        description='Dependency settlement date',
    )
    feq: FreqLiteral = Field(description='Running Frequency')
    data_feq: FreqLiteral = Field(description='Data Frequency')

    @classmethod
    def from_conf(cls, name: str, filepath: str | None = None) -> Self:
        """Get the data from the yaml config file.
        """
        from airflow.exceptions import AirflowFailException

        conf: Path = RootPath / (filepath or 'dags/conf/streams.yaml')
        with conf.open(mode='r', encoding='utf-8') as f:
            data: dict[str, Any] = (
                yaml.load(f, CSafeLoader)[name]
            )

        # NOTE: Try to parsing metadata to Stream Pydantic model.
        try:
            rs = cls.model_validate(obj=data)
            rs.set_name(name=name)
        except Exception as err:
            raise AirflowFailException(
                f"Stream config data does not valid: {err}"
            )
        return rs

    def set_name(self, name: str) -> Self:
        """Set stream name attribute on this stream instance.

        :param name: A stream name.
        """
        self.__dict__['strem_nm']: str = name
        return self

    def get_asat_dt(self, start_date: datetime, prev: bool = False) -> datetime:
        """Return ``asat_dt`` value that use for running datetime."""
        return add_date_with_freq(start_date, freq=self.feq, prev=prev)

    def calc_asat_dt(self, asat_date: datetime) -> datetime:
        """Return ``calc_asat_dt`` value that use for running datetime.
        This method should receive an asat_dt for calculate this logic.
        """
        return calc_data_date_with_freq(asat_date, freq=self.data_feq)


class ConnConfData(BaseModel):
    """Connection Config Data Model."""
    conn_id: str = Field(description="Airflow connection ID")
    service: str = Field(description="Connection service type")


class SysConfData(BaseModel):
    """System Config Data Model."""
    sys_nm: str = Field(description="System Name")
    cntnr: str = Field(description="Container Name")
    pth: str = Field(description="Path Name")


class ExtraConfData(BaseModel):
    """Extras Config Data Model. Any fields on this model should be set
    default value because it will use all Process Config Data Model.
    """
    lcm_archive: bool = Field(default=False, description="")
    recv_email: Optional[str] = Field(default=None, description="")


class FileConfData(BaseModel):
    """File Config Data Model."""
    fi_pth: str = Field(
        description='File path',
    )
    fi_nm: str = Field(
        description='File name',
    )
    file_format: FileFormatLiteral = Field(
        description='File format',
    )

    cntl_fi_nm: Optional[str] = Field(
        default=None, description='Control File Name'
    )
    hdr_f: bool = Field(default=True, description='Header Flag')
    dlm_strng: Optional[str] = Field(
        default=None, description='Delimiter String value'
    )
    quote_chr: Optional[str] = Field(
        default=None, description='Quote character'
    )
    escp_chr: Optional[str] = Field(
        default=None, description='Escape character'
    )
    encdng: Optional[str] = Field(default=None, description='Encoding')
    chk_nul_cols: Optional[str] = Field(
        default=None, description='Check Null Value Columns'
    )
    chk_pk_cols: Optional[str] = Field(
        default=None, description='Check Primary property Column'
    )
    schm_fi: Optional[str] = Field(
        default=None,
        description='JSON Schema file name',
    )
    sht_nm: Optional[str] = Field(
        default=None,
        description='Sheet Name for the Excel file format',
    )
    m_f: bool = Field(default=True, description='Mandatory Flag')

    def get_formatted_path(self, asat_dt: datetime) -> str:
        return asat_dt.strftime(f"{self.fi_pth}/{self.fi_nm}")


class Process(BaseModel):
    """Process Config Data Model."""
    prcs_nm: Optional[str] = Field(default=None, description="Process name")

    # Optional Config data
    fi: Optional[FileConfData] = Field(default=None)
    extras: ExtraConfData = Field(default_factory=ExtraConfData)

    # Source Config
    conn: ConnConfData
    sys: Optional[SysConfData] = Field(default=None)
    src_schm_nm: Optional[str] = Field(
        default=None, description="Source schema name"
    )
    src_tbl: Optional[str] = Field(
        default=None, description="Source table name"
    )
    bus_date_field: Optional[str] = Field(
        default=None, description="Business data fields"
    )
    chk_pk_cols: Optional[str] = Field(default=None)
    cus_qry: Optional[str] = Field(default=None)

    # Target Config
    tgt_conn: Optional[ConnConfData] = Field(default=None)
    tgt_sys: Optional[SysConfData] = Field(default=None)
    tgt_schm_nm: Optional[str] = Field(
        default=None, description="Target schema name"
    )
    tgt_tbl: Optional[str] = Field(
        default=None, description="Target table name"
    )

    # Common fields for process
    prcs_typ: int = Field(
        description=(
            "Process type that use to dynamic routing in process common dag."
        ),
    )
    prcs_load_typ: LoadTypeLiteral = Field(
        description="Process loading strategy type such as T, D, I, F, or SCD.",
    )

    dist_typ: Optional[str] = Field(default=None)
    tbl_strctr_opt: Optional[str] = Field(default=None)
    hash_col: Optional[str] = Field(default=None)
    skip_chk_f: bool = Field(default=True)
    adb_nm: Optional[str] = Field(default=None)
    adb_parm: Optional[str] = Field(default=None)
    byps_f: bool = Field(default=True)

    @classmethod
    def from_conf(cls, name: str, filepath: str | None = None) -> Self:
        from airflow.exceptions import AirflowFailException

        conf: Path = RootPath / (filepath or 'dags/conf/processes.yaml')
        with conf.open(mode='r', encoding='utf-8') as f:
            data: dict[str, Any] = (
                yaml.load(f, CSafeLoader)[name]
            )

        # NOTE: Try to parsing metadata to Stream Pydantic model.
        try:
            rs = cls.model_validate(obj=data)
            rs.set_name(name=name)
        except Exception as err:
            raise AirflowFailException(
                f"Stream config data does not valid: {err}"
            )
        return rs

    def set_name(self, name: str) -> Self:
        self.__dict__['prcs_nm']: str = name
        return self


StatusLiteral = Literal['SUCCESS', 'FAILED', 'START', 'SKIPPED']


class StreamWatermark(BaseModel):
    """Streams watermark schemas"""
    model_config = ConfigDict(from_attributes=True)

    strem_nm: str = Field(description="Stream name")
    asat_dt: datetime = Field(description="Current running date")
    nxt_asat_dt: datetime = Field(description="Next running date")
    prv_calc_asat_dt: datetime = Field(
        description="Previous calculation data date"
    )
    calc_asat_dt: datetime = Field(description="Calculation data date")
    strem_f: bool = Field(default=True)
    act_f: bool = Field(default=True)
    updt_dttm: datetime
    updt_by: str

    @field_validator(
        'asat_dt',
        'nxt_asat_dt',
        'prv_calc_asat_dt',
        'calc_asat_dt',
        mode='after',
    )
    def replace_tz_on_date(cls, value: datetime):
        return value.replace(tzinfo=tz)


class ProcessDeps(BaseModel):
    """Streams watermark schemas"""
    model_config = ConfigDict(from_attributes=True)

    prcs_nm: str = Field(description="Process name")
    dpnd_prcs_nm: str = Field(description="Dependency process name")
    act_f: bool = Field(description="Dependency active flag")
    updt_dttm: datetime = Field(default_factory=get_dt_now)
    updt_by: str = Field(default='MANUAL')


class StreamLog(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    strem_nm: str = Field(description="Stream name")
    strem_id: int = Field(description="Stream loading ID")
    airflow_run_id: str
    asat_dt: datetime
    strt_dttm: datetime
    end_dttm: Optional[datetime] = Field(default=None)
    st: StatusLiteral
    rmrk: Optional[str] = Field(
        default=None, description="Remark for this logging"
    )
    updt_dttm: datetime = Field(default_factory=get_dt_now)
    updt_by: str = Field(default='MANUAL')


class ProcessLog(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    prcs_nm: str = Field(description="Process name")
    ld_id: int = Field(description="")
    strem_id: int = Field(description="")
    airflow_run_id: str

    asat_dt: datetime
    strt_dttm: datetime
    end_dttm: Optional[datetime] = Field(default=None)

    cnt_rec_cntl: Optional[int] = Field(
        default=None, description="Count record number of "
    )
    cnt_rec_src: Optional[int] = Field(
        default=None, description="Count record number of "
    )
    cnt_rec_tgt: Optional[int] = Field(
        default=None, description="Count record number of "
    )
    cnt_fi_src: Optional[int] = Field(
        default=None, description="Count record number of "
    )
    cnt_fi_tgt: Optional[int] = Field(
        default=None, description="Count record number of "
    )

    st: StatusLiteral
    rmrk: Optional[str] = Field(
        default=None, description="Remark for this logging"
    )
    fail_airflow_run_id: Optional[str] = None
    updt_dttm: datetime
    updt_by: str = Field(default='MANUAL')
