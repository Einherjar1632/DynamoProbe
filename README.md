# DynamoDB Test Project

このプロジェクトは、DynamoDBの能力を検証するためのPythonスクリプトです。

## 機能

1. DynamoDBテーブルの作成
2. ダミーデータの挿入（10,000件）
3. クエリの実行（パーティションキー、GSI、全レコード検索）
4. テーブルの削除

## セットアップ

1. 必要なパッケージをインストールします：

```bash
pip install -r requirements.txt
```

2. 環境変数を設定します：

```bash
export AWS_ACCESS_KEY_ID=<your_access_key_id>
export AWS_SECRET_ACCESS_KEY=<your_secret_access_key>
export AWS_DEFAULT_REGION=<your_region>
```