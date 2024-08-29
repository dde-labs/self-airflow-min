from plugins.metadata.conf import ProcessConfData, StreamConfData


def test_schemas_process_conf_data():
    process_conf_data: ProcessConfData = ProcessConfData.model_validate(
        obj={
            "conn": {
                "conn_id": "s3_conn_id",
                "service": "s3",
            },
            "prcs_typ": 1,
            "prcs_load_typ": "D",
            "sys": {
                "sys_nm": "PP",
                "cntnr": "curated",
                "pth": "pp",
            },
        }
    )
    print(process_conf_data)


def test_schemas_stream_conf_data():
    stream_conf_data: StreamConfData = StreamConfData.model_validate(
        obj={
            "tier": "CURATED",
            "stlmt_dt": -1,
            "dpnd_stlmt_dt": -1,
            "feq": "D",
            "data_feq": "D",
        }
    )
    print(stream_conf_data)
