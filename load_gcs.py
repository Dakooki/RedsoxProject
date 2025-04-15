from google.cloud import storage
import os

staging_dir = 'path/baseball_project/staging/'
bucket_name = 'rs_bucket_challenge'  # ✅ Replace with your actual bucket name
destination_folder = 'staging_files'  # Optional: GCS folder prefix

files_to_upload = [
    'staged_games_wide.csv',
    'staged_games_post_wide.csv',
    'staged_schedules.csv'
]

def upload_files_to_gcs(local_dir, bucket_name, files, destination_prefix=''):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for filename in files:
        local_path = os.path.join(local_dir, filename)
        blob_path = f"{destination_prefix}/{filename}" if destination_prefix else filename
        blob = bucket.blob(blob_path)

        blob.upload_from_filename(local_path)
        print(f"✅ Uploaded {local_path} to gs://{bucket_name}/{blob_path}")

upload_files_to_gcs(staging_dir, bucket_name, files_to_upload, destination_folder)
