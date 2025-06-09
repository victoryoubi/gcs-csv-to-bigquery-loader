# gcs-csv-to-bigquery-loader

GCS に CSV をアップロードすると、Cloud Function が自動で起動して：

1. CSV の列数を 41 列に整形（不足分は空欄で補う）
2. 整形済みの CSV を GCS 内 `/fixed/` に保存
3. BigQuery テーブル `sns_meltwater.MeltWater` に追記

## 🔧 使用技術
- Cloud Functions (Python 3.10)
- Google Cloud Storage
- BigQuery

## 🚀 デプロイ手順

```bash
gcloud functions deploy csv_to_bq_loader \
  --runtime python310 \
  --trigger-resource YOUR_BUCKET_NAME \
  --trigger-event google.storage.object.finalize \
  --entry-point main \
  --region asia-northeast1 \
  --memory 512MB \
  --timeout 60s \
  --source .
