import csv
import os
import re
from google.cloud import storage, bigquery

PROJECT = "opproject-459908"
DATASET = "SNS"
TABLE = "MeltWater"

# --- CSVの列数を揃える関数（カンマ区切り） ---
def ensure_fixed_column_count(input_path, output_path, expected_cols=42):
    with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile, delimiter=',')  # ✅ カンマ区切り
        writer = csv.writer(outfile, delimiter=',')  # ✅ カンマ区切り
        for row in reader:
            if not row or all(cell.strip() == '' for cell in row):
                continue
            if len(row) < expected_cols:
                row += [''] * (expected_cols - len(row))
            elif len(row) > expected_cols:
                row = row[:expected_cols]
            writer.writerow(row)

# --- メイン関数（Cloud Function） ---
def main(event, context):
    file_data = event
    file_name = file_data['name']
    bucket_name = file_data['bucket']

    if not file_name.endswith('.csv'):
        print(f"⏭️ スキップ: {file_name} はCSVではありません")
        return

    if re.search(r'(^|/)fixed(/|$)', file_name):
        print(f"⏭️ fixed フォルダ内のファイルは無視します: {file_name}")
        return

    print(f"📥 GCSからCSVを取得: {bucket_name}/{file_name}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    local_path = f"/tmp/{os.path.basename(file_name)}"
    fixed_path = f"/tmp/fixed_{os.path.basename(file_name)}"

    blob.download_to_filename(local_path)

    ensure_fixed_column_count(local_path, fixed_path, expected_cols=42)

    fixed_blob = bucket.blob(f"fixed/{os.path.basename(file_name)}")
    fixed_blob.upload_from_filename(fixed_path)

    upload_to_bigquery(f"gs://{bucket_name}/fixed/{os.path.basename(file_name)}")

# --- BigQueryにロードする処理 ---
def upload_to_bigquery(gcs_uri):
    client = bigquery.Client()
    table_id = f"{PROJECT}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        field_delimiter=',',  # ✅ カンマ区切りを明示
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("documentID", "STRING"),
            bigquery.SchemaField("URL", "STRING"),
            bigquery.SchemaField("search_keyword", "STRING"),
            bigquery.SchemaField("keyword", "STRING"),
            bigquery.SchemaField("information_type", "STRING"),
            bigquery.SchemaField("source_type", "STRING"),
            bigquery.SchemaField("source_name", "STRING"),
            bigquery.SchemaField("source_name_url", "STRING"),
            bigquery.SchemaField("Content Type", "STRING"),
            bigquery.SchemaField("author_name", "STRING"),
            bigquery.SchemaField("author_handlename", "STRING"),
            bigquery.SchemaField("Content_tytle", "STRING"),
            bigquery.SchemaField("opening_sentence", "STRING"),
            bigquery.SchemaField("text", "STRING"),
            bigquery.SchemaField("image_url", "STRING"),
            bigquery.SchemaField("hashtag", "STRING"),
            bigquery.SchemaField("link", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("Place_of_submission", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("language", "STRING"),
            bigquery.SchemaField("centiment", "STRING"),
            bigquery.SchemaField("text_keyword", "STRING"),
            bigquery.SchemaField("reach", "INTEGER"),
            bigquery.SchemaField("AVE", "FLOAT"),
            bigquery.SchemaField("Social_Echo", "STRING"),
            bigquery.SchemaField("Editorial_Echo", "STRING"),
            bigquery.SchemaField("engagement", "INTEGER"),
            bigquery.SchemaField("share", "STRING"),
            bigquery.SchemaField("Quotes", "STRING"),
            bigquery.SchemaField("like", "INTEGER"),
            bigquery.SchemaField("reply", "INTEGER"),
            bigquery.SchemaField("repost", "STRING"),
            bigquery.SchemaField("comment", "STRING"),
            bigquery.SchemaField("reaction", "INTEGER"),
            bigquery.SchemaField("impression", "STRING"),
            bigquery.SchemaField("comparison_of_impression", "STRING"),
            bigquery.SchemaField("documenttag", "STRING"),
            bigquery.SchemaField("custom_category", "STRING"),
        ]
    )

    try:
        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        print(f"▶️ BigQuery LoadJob started: {load_job.job_id}")
        load_job.result()
        print(f"✅ BigQuery 取込成功: {table_id}")
    except Exception as e:
        print(f"❌ BigQuery 取込失敗: {e}")
        raise
