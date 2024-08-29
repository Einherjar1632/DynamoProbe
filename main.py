from dotenv import load_dotenv
import os

load_dotenv()  # .envファイルから環境変数を読込

if not all([os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY'), os.getenv('AWS_DEFAULT_REGION')]):
    raise ValueError("AWS認証情報が正しく設定されていません。.envファイルを確認してください。")

from src.table_operations import create_main_table, create_transaction_table, delete_main_table, delete_transaction_table
from src.data_operations import insert_main_data, query_main_table, insert_transaction_data, test_transaction_isolation
from src.config import TABLE_NAME, TRANSACTION_TEST_TABLE_NAME

def main():
    # メインテーブルの作成と操作
    #main_table = create_main_table()
    #insert_main_data(main_table)
    #query_main_table(main_table)

    # トランザクションテストテーブルの作成と操作
    transaction_table = create_transaction_table()
    insert_transaction_data(transaction_table)
    test_transaction_isolation(transaction_table)

    # クリーンアップ（オプション）
    # delete_main_table()
    # delete_transaction_table()

if __name__ == "__main__":
    main()