from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("credentials.json")


client = bigquery.Client(credentials=credentials)

# Perform a query.
QUERY = (
    "SELECT date, device.browser, device.operatingSystem, geoNetwork. continent, geoNetwork. country "
    "FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` "
    # "GROUP BY browser"
)
# query_job = client.query(QUERY)  # API request
#
# query_result = query_job.result()
# # df = query_result.to_dataframe()
# df = pd.DataFrame(query_result.fetchall())
df = client.query_and_wait(QUERY).to_dataframe()

print(df)


