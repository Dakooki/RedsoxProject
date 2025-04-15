import pandas as pd
from google.cloud import bigquery, storage

# Set your GCP project and dataset
project_id = "redsox-t-challenge"
dataset_id = "redsox_stg_dataset"
bucket_name = "rs_bucket_challenge"
folder = "staging_files"

schema_game_post_wide = [
    bigquery.SchemaField("gameId", "STRING"),
    bigquery.SchemaField("seasonId", "STRING"),
    bigquery.SchemaField("seasonType", "STRING"),
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("startTime", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("gameStatus", "STRING"),
    bigquery.SchemaField("attendance", "INTEGER"),
    bigquery.SchemaField("dayNight", "STRING"),
    bigquery.SchemaField("duration", "STRING"),
    bigquery.SchemaField("durationMinutes", "INTEGER"),
    bigquery.SchemaField("awayTeamId", "STRING"),
    bigquery.SchemaField("awayTeamName", "STRING"),
    bigquery.SchemaField("homeTeamId", "STRING"),
    bigquery.SchemaField("homeTeamName", "STRING"),
    bigquery.SchemaField("venueId", "STRING"),
    bigquery.SchemaField("venueName", "STRING"),
    bigquery.SchemaField("venueSurface", "STRING"),
    bigquery.SchemaField("venueCapacity", "INTEGER"),
    bigquery.SchemaField("venueCity", "STRING"),
    bigquery.SchemaField("venueState", "STRING"),
    bigquery.SchemaField("venueZip", "STRING"),
    bigquery.SchemaField("venueMarket", "STRING"),
    bigquery.SchemaField("homeFinalRuns", "INTEGER"),
    bigquery.SchemaField("homeFinalHits", "INTEGER"),
    bigquery.SchemaField("homeFinalErrors", "INTEGER"),
    bigquery.SchemaField("awayFinalRuns", "INTEGER"),
    bigquery.SchemaField("inningHalf", "STRING"),
    bigquery.SchemaField("inningEventType", "STRING"),
    bigquery.SchemaField("inningHalfEventSequenceNumber", "INTEGER"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP"),
]

schema_game_wide = schema_game_post_wide

""" schema_schedules = [
    bigquery.SchemaField("gameId", "STRING"),
    bigquery.SchemaField("gameNumber", "STRING"),
    bigquery.SchemaField("seasonId", "STRING"),
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("dayNight", "STRING"),
    bigquery.SchemaField("duration", "STRING"),
    bigquery.SchemaField("duration_minutes", "INTEGER"),
    bigquery.SchemaField("homeTeamId", "STRING"),
    bigquery.SchemaField("homeTeamName", "STRING"),
    bigquery.SchemaField("awayTeamId", "STRING"),
    bigquery.SchemaField("awayTeamName", "STRING"),
    bigquery.SchemaField("startTime", "STRING"),
    bigquery.SchemaField("attendance", "INTEGER"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP"),
]
 """
staging_files = {
    "staging_files/staged_games_post_wide.csv": "stg_game_post_wide",
    "staging_files/staged_games_wide.csv": "stg_game_wide",
   ## "staging_files/staged_schedules.csv": "stg_schedules"
}

#  Initialize BigQuery client
client = bigquery.Client(project=project_id)

#  Initialize GCS client for file verification
def check_gcs_file(bucket_name, file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    if blob.exists():
        print(f"✅ File {file_path} exists in GCS!")
        return True
    else:
        print(f"❌ File {file_path} does not exist in GCS.")
        return False


for file_path, table_name in staging_files.items():

    # Check if the file exists in GCS before loading
    if not check_gcs_file(bucket_name, file_path):
        continue  # Skip the file if it does not exist in GCS

    # Prepare BigQuery URI and table ID
    uri = f"gs://{bucket_name}/{file_path}"
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    # Select the correct schema for the table
    if table_name == "stg_game_post_wide":
        schema = schema_game_post_wide
    elif table_name == "stg_game_wide":
        schema = schema_game_wide
    #elif table_name == "stg_schedules":
     #   schema = schema_schedules

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1
    )

    # Attempt to load the data into BigQuery
    try:
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to finish
        print(f"✅ Loaded {file_path} into {table_id}")
    
    except Exception as e:
        print(f"❌ Error loading {file_path} into {table_id}: {e}")
 


