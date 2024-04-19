import os
import threading
import time

import gspread
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

DATASET_ID = "bigquery-public-data.google_analytics_sample"


def get_data_from_single_table(
    client: bigquery.Client, table_id: str, dataframes: list
):
    query = (
        f"SELECT date, device.browser, device.operatingSystem, "
        f"geoNetwork.continent, geoNetwork.country "
        f"FROM `{DATASET_ID}.{table_id}`"
    )
    query_job = client.query(query)
    df = query_job.to_dataframe()
    dataframes.append(df)


def get_dataframe(
    client: bigquery.Client, tables: list, amount_of_days: int
) -> pd.DataFrame:
    tasks = []
    dataframes = []
    for table in tables[:amount_of_days]:
        tasks.append(
            threading.Thread(
                target=get_data_from_single_table,
                args=(client, table.table_id, dataframes),
            )
        )
        tasks[-1].start()
    for task in tasks:
        task.join()

    df = pd.concat(dataframes, ignore_index=True)

    # Added a month column to aggregate data by months
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.month

    return df


def main(amount_of_days: int):
    credentials = service_account.Credentials.from_service_account_file(
        "credentials.json"
    )
    client = bigquery.Client(credentials=credentials)
    dataset = client.get_dataset(DATASET_ID)
    tables = list(client.list_tables(dataset))

    result_df = get_dataframe(client, tables, amount_of_days)

    gc = gspread.service_account(filename="credentials.json")
    sh = gc.create("new_age_level2")
    sh.share(os.environ.get("EMAIL"), perm_type="user", role="writer")

    for column in result_df.columns:
        if column != "date":
            aggregated_df = result_df.groupby(column).size().reset_index(name="count")

            worksheet = sh.add_worksheet(
                title=f"Aggregated {column}", rows=1000, cols=5
            )
            worksheet.append_rows(
                [[column, f"Items per {column}"]], value_input_option="RAW"
            )
            worksheet.append_rows(
                aggregated_df.values.tolist(), value_input_option="RAW"
            )

    print("Check your email")


if __name__ == "__main__":
    start = time.perf_counter()
    main(amount_of_days=200)
    end = time.perf_counter()
    print("Elapsed:", end - start)
