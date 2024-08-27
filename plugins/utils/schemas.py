from datetime import datetime, timedelta
from typing import Literal, Optional

from pydantic import BaseModel, Field


FreqLiteral = Literal["D", "W", "M", "Q", "Y"]
TierLiteral = Literal["CURATED", "MDL", "DMT"]
LoadTypeLiteral = Literal["F", "T", "D", "I", "SCD_D", "SCD_F", "SCD_T"]

FreqMapping: dict[str, str] = {
    "D": "days",
    "W": "weeks",
    "M": "months",
    "Q": "quarters",
    "Y": "years",
}


class StreamConfData(BaseModel):
    tier: TierLiteral = Field(description="Tier of this stream")
    stlmt_dt: int = Field(description='Settlement Date')
    dpnd_stlmt_dt: int = Field(description='Dependency Settlement Date')
    feq: FreqLiteral = Field(description='Running Frequency')
    data_feq: FreqLiteral = Field(description='Data Frequency')

    def get_asat_dt(self, start_date: datetime) -> datetime:
        return (
            start_date.replace(second=0, microsecond=0)
            + timedelta(days=self.stlmt_dt)
        )

    def get_calc_dt(self, start_date: datetime) -> datetime:
        ...


class ConnConfData(BaseModel):
    conn_id: str = Field(description="Airflow connection ID")
    service: str = Field(description="Connection service type")


class SysConfData(BaseModel):
    sys_nm: str = Field(description="System Name")
    cntnr: str = Field(description="Container Name")
    pth: str = Field(description="Path Name")


class ExtraConfData(BaseModel):
    lcm_archive: bool = Field(default=False, description="")
    recv_email: Optional[str] = Field(default=None, description="")


class FileConfData(BaseModel):
    fi_pth: str = Field(description='File Path')
    fi_nm: str = Field(description='File Name')
    file_format: str = Field(description='File Format')

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
        description='Sheet Name for the Excel file format'
    )
    m_f: bool = Field(default=True, description='Mandatory Flag')


class ProcessConfData(BaseModel):
    fi: Optional[FileConfData] = Field(default=None)
    extras: ExtraConfData = Field(default_factory=ExtraConfData)

    # Source Config
    conn: ConnConfData
    sys: Optional[SysConfData] = Field(default=None)
    src_schm_nm: Optional[str] = Field(default=None)
    src_tbl: Optional[str] = Field(default=None)
    bus_date_field: Optional[str] = Field(default=None)
    chk_pk_cols: Optional[str] = Field(default=None)
    cus_qry: Optional[str] = Field(default=None)

    # Target Config
    tgt_conn: Optional[ConnConfData] = Field(default=None)
    tgt_sys: Optional[SysConfData] = Field(default=None)
    tgt_schm_nm: Optional[str] = Field(default=None)
    tgt_tbl: Optional[str] = Field(default=None)

    # Common fields for process
    prcs_typ: int = Field(description="")
    prcs_load_typ: LoadTypeLiteral = Field(description="")
    dist_typ: Optional[str] = Field(default=None)
    tbl_strctr_opt: Optional[str] = Field(default=None)
    hash_col: Optional[str] = Field(default=None)
    skip_chk_f: bool = Field(default=True)
    adb_nm: Optional[str] = Field(default=None)
    adb_parm: Optional[str] = Field(default=None)
    byps_f: bool = Field(default=True)