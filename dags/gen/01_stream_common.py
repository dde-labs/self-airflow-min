import json
import logging

import pendulum as pm

from airflow.decorators import dag, task
from plugins.models.request_model import Request


def clean_request(request: Request) -> dict:
    """Cleans the data by removing negative values. Returns a list of cleaned
    dictionaries.
    """
    cleaned_dict = {
        "columns": request.input_data.columns,
        "index": request.input_data.index,
        "data": [
            max(item, 0)
            for items in request.input_data.data
            for item in items
        ]
    }
    return cleaned_dict


@dag(
    schedule="@daily",
    start_date=pm.datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["example"],
)
def etl_pipeline():
    """# ETL pipeline
    """

    @task()
    def load_data() -> list[Request]:
        """Loads json requests using the Request model and returns a list of
        Request objects.
        """

        sample_request_1 = '''{
            "input_data": {
                "columns": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22],
                "index": [0, 1],
                "data": [
                    [20000,2,2,1,24,2,2,-1,-1,-2,-2,3913,3102,689,0,0,0,0,689,0,0,0,0],
                    [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10, 9, 8]
                ]
            }
        }'''

        sample_request_2 = '''{
            "input_data": {
                "columns": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22],
                "index": [0, 1],
                "data": [
                    [20005, 7, 7, 6, 29, 7, 7, 4, 4, 3, 3, 3918, 3107, 694, 5, 5, 5, 5, 694, 5, 5, 5, 5],
                    [5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 9, 8, 7, 6, 5, 4, 3]
                ]
            }
        }'''

        ordered_data = []

        for item in [sample_request_1, sample_request_2]:
            ordered_dict = json.loads(item)
            ordered_data.append(ordered_dict)

        requests: list[Request] = [Request(**item) for item in ordered_data]
        return requests

    @task()
    def clean_data(order_data: list[Request]) -> list[dict]:
        """
        Cleans the data and returns a list of dictionaries.
        """
        cleaned_requests = []

        for request in order_data:
            cleaned_dict = clean_request(request)
            cleaned_requests.append(cleaned_dict)

        return cleaned_requests

    @task()
    def prepare_requests(cleaned_requests: list[dict]) -> list[str]:
        """
        Uploads the requests to Azure Blob. Returns a list of request locations.
        """
        request_locations = []
        # blob_connection = WasbHook(wasb_conn_id="connection_id_blob")
        for item in cleaned_requests:
            # blob_connection.load_string(
            #     item,
            #     'azureml',
            #     f"request_sample_{pm.now().timestamp()}.json"
            # )
            logging.info(f"Start loading ... {item}")
            request_locations.append(
                f"request_sample_{pm.now().timestamp()}.json"
            )
        return request_locations

    raw_data = load_data()
    cleaned_data = clean_data(raw_data)
    prepare_requests(cleaned_data)


etl_pipeline()
