from dotenv import load_dotenv
import os

load_dotenv()  # .envファイルから環境変数を読込

if not all([os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY'), os.getenv('AWS_DEFAULT_REGION')]):
    raise ValueError("AWS認証情報が正しく設定されていません。.envファイルを確認してください。")

from src.table_operations import create_table, delete_table
from src.data_operations import insert_dummy_data, query_table

def main():
    try:
        # delete_table()
        table = create_table()
        # insert_dummy_data(table)
        query_table(table)
    finally:
        print("プログラム終了")

if __name__ == "__main__":
    main()