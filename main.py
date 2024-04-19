import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import gspread
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

DATASET_ID = "bigquery-public-data.google_analytics_sample"


def get_data_from_single_table(client: bigquery.Client, table_id: str):
    print(table_id)
    query = (
        f"SELECT date, device.browser, device.operatingSystem, "
        f"geoNetwork.continent, geoNetwork.country "
        f"FROM `{DATASET_ID}.{table_id}`"
    )
    query_job = client.query(query)
    query_result = query_job.result()

    print(query_result.total_rows)


def main():
    credentials = service_account.Credentials.from_service_account_file(
        "credentials.json"
    )
    client = bigquery.Client(credentials=credentials)
    dataset = client.get_dataset(DATASET_ID)
    tables = list(client.list_tables(dataset))

    tasks = []
    for table in tables[:90]:
        tasks.append(
            threading.Thread(
                target=get_data_from_single_table,
                args=(client, table.table_id),
            )
        )
        tasks[-1].start()
    for task in tasks:
        task.join()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    print("Elapsed:", end - start)
