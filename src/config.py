# DynamoDB設定
TABLE_NAME = 'EmployeeTable'
TRANSACTION_TEST_TABLE_NAME = 'TransactionTable'
REGION_NAME = 'ap-northeast-1'

# テーブル設定
PARTITION_KEY = 'employee_id'
GSI_KEY = 'email'
GSI_NAME = 'EmailIndex'

# プロビジョニング設定
MIN_CAPACITY = 1
MAX_CAPACITY = 10

# データ生成設定
NUM_RECORDS = 1000  # または必要な数
