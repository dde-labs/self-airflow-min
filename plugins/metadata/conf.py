from datetime import datetime
from typing import Literal, Optional, Self

from dateutil import relativedelta
from pydantic import BaseModel, Field


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



class StreamConfData(BaseModel):
    """Stream Config Data Model"""
    name: Optional[str] = Field(default=None, description="Stream name")
    tier: TierLiteral = Field(description="Tier of this stream")
    stlmt_dt: int = Field(description='Settlement Date')
    dpnd_stlmt_dt: int = Field(description='Dependency Settlement Date')
    feq: FreqLiteral = Field(description='Running Frequency')
    data_feq: FreqLiteral = Field(description='Data Frequency')

    def set_name(self, name: str) -> Self:
        self.__dict__['name']: str = name
        return self

    def get_asat_dt(self, start_date: datetime) -> datetime:
        """Return ``asat_dt`` value that use for running datetime."""
        return (
            start_date.replace(second=0, microsecond=0)
            + relativedelta.relativedelta(
                **{FreqMapping[self.feq]: self.stlmt_dt}
            )
        )

    def get_calc_asat_dt(self, start_date: datetime) -> datetime:
        """Return ``calc_asat_dt`` value that use for running datetime."""


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


class ProcessConfData(BaseModel):
    """Process Config Data Model."""
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
    bus_date_field: Optional[str] = Field(default=None)
    chk_pk_cols: Optional[str] = Field(default=None)
    cus_qry: Optional[str] = Field(default=None)

    # Target Config
    tgt_conn: Optional[ConnConfData] = Field(default=None)
    tgt_sys: Optional[SysConfData] = Field(default=None)
    tgt_schm_nm: Optional[str] = Field(default=None)
    tgt_tbl: Optional[str] = Field(default=None)

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
