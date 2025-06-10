import csv
import os
import re
from google.cloud import storage, bigquery

# 環境変数で指定してもよい
PROJECT = "opproject-459908"
DATASET = "SNS"
TABLE = "MeltWater"


# --- CSVの列数を揃える関数（空行スキップ付き） ---
def ensure_fixed_column_count(input_path, output_path, expected_cols=42):
    with open(input_path, 'r', encoding='utf-8') as infile, open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            # ✅ 空行またはすべて空の行をスキップ
            if not row or all(cell.strip() == '' for cell in row):
                continue
            # 列数を調整
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

    # ✅ fixed/ フォルダに含まれていたら処理しない（再帰防止）
    if re.search(r'(^|/)fixed(/|$)', file_name):
        print(f"⏭️ fixed フォルダ内のファイルは無視します: {file_name}")
        return

    print(f"📥 GCSからCSVを取得: {bucket_name}/{file_name}")

    # GCSクライアントの初期化
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # 一時保存パス
    local_path = f"/tmp/{os.path.basename(file_name)}"
    fixed_path = f"/tmp/fixed_{os.path.basename(file_name)}"

    # ダウンロード
    blob.download_to_filename(local_path)

    # 列数を揃える
    ensure_fixed_column_count(local_path, fixed_path, expected_cols=42)

    # 整形ファイルを別GCSパスへ再アップロード
    fixed_blob = bucket.blob(f"fixed/{os.path.basename(file_name)}")
    fixed_blob.upload_from_filename(fixed_path)

    # BigQueryにロード
    upload_to_bigquery(f"gs://{bucket_name}/fixed/{os.path.basename(file_name)}")

# --- BigQueryにロードする処理 ---
def upload_to_bigquery(gcs_uri):
    client = bigquery.Client()
    table_id = f"{PROJECT}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        encoding="UTF-8",
    )

    try:
        load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        print(f"▶️ BigQuery LoadJob started: {load_job.job_id}")
        load_job.result()  # ✅ ここで失敗すると例外になる
        print(f"✅ BigQuery 取込成功: {table_id}")
    except Exception as e:
        print(f"❌ BigQuery 取込失敗: {e}")
        raise  # エラーをCloud Functionsのログに残す
