from datetime import datetime

from plugins.metadata.services import ProcessLog
from plugins.utils import get_dt_now


def test_schemas_process_log():
    pl = ProcessLog.model_validate(
        {
            "prcs_nm": "P_TESTING_PROCESS_D_99",
            "ld_id": 1,
            "strem_id": 1,
            "airflow_run_id": "dummy_airflow_running_id",
            "asat_dt": datetime(2024, 9, 15),
            "strt_dttm": get_dt_now(only_date=False),
            "end_dttm": None,
            "cnt_rec_cntl": None,
            "cnt_rec_src": None,
            "cnt_rec_tgt": None,
            "cnt_fi_src": None,
            "cnt_fi_tgt": None,
            "st": "START",
            "rmrk": None,
            "fail_airflow_run_id": None,
            "updt_dttm": get_dt_now(only_date=False),
            "updt_by": "AIRFLOW",
        }
    )

    assert pl.st == "START"
