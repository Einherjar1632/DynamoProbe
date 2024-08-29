# AWS設定
REGION_NAME = 'ap-northeast-1'

# テーブル設定
TABLE_NAME = 'EmployeeTable'
PARTITION_KEY = 'employee_id'
GSI_KEY = 'email'
GSI_NAME = 'EmailIndex'

# トランザクションテーブル設定
TRANSACTION_TEST_TABLE_NAME = 'TransactionTable'

# プロビジョニング設定
MIN_CAPACITY = 1
MAX_CAPACITY = 25

# データ生成設定
NUM_RECORDS = 1000
