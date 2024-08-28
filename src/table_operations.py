import boto3
from botocore.exceptions import ClientError
from src.config import TABLE_NAME, REGION_NAME, PARTITION_KEY, GSI_KEY, GSI_NAME, MIN_CAPACITY, MAX_CAPACITY

dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)
client = boto3.client('dynamodb', region_name=REGION_NAME)
application_autoscaling = boto3.client('application-autoscaling', region_name=REGION_NAME)

def create_table():
    try:
        # テーブルが存在するか確認
        existing_table = dynamodb.Table(TABLE_NAME)
        existing_table.load()
        print(f"テーブル {TABLE_NAME} は既に存在します。")
        return existing_table
    except client.exceptions.ResourceNotFoundException:
        # テーブルが存在しない場合、新しく作成
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {'AttributeName': PARTITION_KEY, 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': PARTITION_KEY, 'AttributeType': 'S'},
                {'AttributeName': GSI_KEY, 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': GSI_NAME,
                    'KeySchema': [
                        {'AttributeName': GSI_KEY, 'KeyType': 'HASH'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': MIN_CAPACITY, 'WriteCapacityUnits': MIN_CAPACITY}
                }
            ],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={'ReadCapacityUnits': MIN_CAPACITY, 'WriteCapacityUnits': MIN_CAPACITY}
        )

        print(f"テーブル {TABLE_NAME} を作成しました。")
        
        # テーブルが作成されるのを待つ
        table.meta.client.get_waiter('table_exists').wait(TableName=TABLE_NAME)
        
        # Auto Scalingの設定
        setup_auto_scaling(TABLE_NAME)
        
        return table

def setup_auto_scaling(table_name):
    # 書き込みキャパシティのAuto Scaling設定
    register_scalable_target(table_name, 'table', 'WriteCapacityUnits')
    register_scalable_target(table_name, 'index', 'WriteCapacityUnits', GSI_NAME)
    
    # 読み取りキャパシティのAuto Scaling設定
    register_scalable_target(table_name, 'table', 'ReadCapacityUnits')
    register_scalable_target(table_name, 'index', 'ReadCapacityUnits', GSI_NAME)
    
    # スケーリングポリシーの設定
    put_scaling_policy(table_name, 'table', 'WriteCapacityUnits')
    put_scaling_policy(table_name, 'index', 'WriteCapacityUnits', GSI_NAME)
    put_scaling_policy(table_name, 'table', 'ReadCapacityUnits')
    put_scaling_policy(table_name, 'index', 'ReadCapacityUnits', GSI_NAME)

def register_scalable_target(table_name, dimension, scaling_type, index_name=None):
    resource_id = f"table/{table_name}"
    if index_name:
        resource_id += f"/index/{index_name}"
    
    application_autoscaling.register_scalable_target(
        ServiceNamespace='dynamodb',
        ResourceId=resource_id,
        ScalableDimension=f'dynamodb:{dimension}:{scaling_type}',
        MinCapacity=MIN_CAPACITY,
        MaxCapacity=MAX_CAPACITY
    )

def put_scaling_policy(table_name, dimension, scaling_type, index_name=None):
    resource_id = f"table/{table_name}"
    if index_name:
        resource_id += f"/index/{index_name}"
    
    metric_type = 'WriteCapacityUtilization' if scaling_type == 'WriteCapacityUnits' else 'ReadCapacityUtilization'
    
    application_autoscaling.put_scaling_policy(
        PolicyName=f'{table_name}-{scaling_type}-scaling-policy',
        ServiceNamespace='dynamodb',
        ResourceId=resource_id,
        ScalableDimension=f'dynamodb:{dimension}:{scaling_type}',
        PolicyType='TargetTrackingScaling',
        TargetTrackingScalingPolicyConfiguration={
            'TargetValue': 70.0,
            'PredefinedMetricSpecification': {
                'PredefinedMetricType': f'DynamoDB{metric_type}'
            },
            'ScaleOutCooldown': 60,
            'ScaleInCooldown': 60
        }
    )

def delete_table():
    try:
        table = dynamodb.Table(TABLE_NAME)
        table.delete()
        print(f"テーブル {TABLE_NAME} を削除しました。")
        table.meta.client.get_waiter('table_not_exists').wait(TableName=TABLE_NAME)
    except client.exceptions.ResourceNotFoundException:
        print(f"テーブル {TABLE_NAME} は存在しません。削除をスキップします。")