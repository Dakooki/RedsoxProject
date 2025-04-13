from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPICallError

def export_table_to_gcs():
    try:
        client = bigquery.Client()

        project_id = "redsox-t-challenge"
        dataset_id = "redsox_transformed_dataset"
        table_id = "tr_game_wide"
        destination_uri = ""
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        print(f"Starting export from table: {table_ref}")
        print(f"Exporting to: {destination_uri}")

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            job_config=bigquery.job.ExtractJobConfig(
                destination_format=bigquery.DestinationFormat.CSV,
                field_delimiter=",",
                print_header=True
            ),
            location="US"
        )

        extract_job.result()  # Wait for completion
        print("✅ Export successful!")

    except NotFound as e:
        print(f"❌ Table not found: {e}")
    except GoogleAPICallError as e:
        print(f"❌ API error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    export_table_to_gcs()
