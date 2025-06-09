import csv
import tempfile
from google.cloud import storage, bigquery

EXPECTED_COLUMNS = 41
PROJECT_ID = "opproject-459908"
DATASET = "sns_meltwater"
TABLE = "MeltWater"

def fix_csv_columns(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            if len(row) < EXPECTED_COLUMNS:
                row += [''] * (EXPECTED_COLUMNS - len(row))
            elif len(row) > EXPECTED_COLUMNS:
                row = row[:EXPECTED_COLUMNS]
            writer.writerow(row)

def upload_to_bigquery(gcs_uri):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        null_marker="",
        write_disposition="WRITE_APPEND"
    )
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()

def main(event, context):
    file_name = event['name']
    bucket_name = event['bucket']
    
    if not file_name.endswith('.csv'):
        print(f"⏭️ スキップ: {file_name} はCSVではありません")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_in, \
         tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_out:

        blob.download_to_filename(temp_in.name)
        fix_csv_columns(temp_in.name, temp_out.name)

        fixed_blob_path = f"fixed/{file_name}"
        fixed_blob = bucket.blob(fixed_blob_path)
        fixed_blob.upload_from_filename(temp_out.name)

        gcs_uri = f"gs://{bucket_name}/{fixed_blob_path}"
        upload_to_bigquery(gcs_uri)
        print(f"✅ BigQueryに追記完了: {gcs_uri}")
