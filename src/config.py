# DynamoDB設定
TABLE_NAME = 'EmployeeTable'
REGION_NAME = 'ap-northeast-1'

# テーブル設定
PARTITION_KEY = 'employee_id'
GSI_KEY = 'email'
GSI_NAME = 'EmailIndex'

# プロビジョニング設定
MIN_CAPACITY = 1
MAX_CAPACITY = 25

# データ生成設定
NUM_RECORDS = 100000