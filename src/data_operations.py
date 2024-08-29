import time
import uuid
import random
from faker import Faker
from boto3.dynamodb.conditions import Key
from src.config import TABLE_NAME, NUM_RECORDS, GSI_NAME
from src.table_operations import dynamodb

fake = Faker('ja_JP')

def generate_kana_name():
    katakana = "アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン"
    return ''.join(random.choice(katakana) for _ in range(random.randint(2, 5)))

def insert_dummy_data(table):
    start_time = time.time()
    
    with table.batch_writer() as batch:
        for i in range(NUM_RECORDS):
            item = {
                'employee_id': str(uuid.uuid4()),
                'company_name': fake.company(),
                'employment_type': fake.random_element(elements=('正社員', '契約社員', 'パート')),
                'hire_date': fake.date_between(start_date='-10y', end_date='today').isoformat(),
                'resignation_date': fake.date_between(start_date='today', end_date='+5y').isoformat() if fake.boolean(chance_of_getting_true=20) else None,
                'department': fake.job(),
                'position': fake.job(),
                'last_name': fake.last_name(),
                'first_name': fake.first_name(),
                'last_name_kana': generate_kana_name(),
                'first_name_kana': generate_kana_name(),
                'birth_date': fake.date_of_birth(minimum_age=20, maximum_age=60).isoformat(),
                'gender': fake.random_element(elements=('男性', '女性')),
                'email': fake.email(),
                'profile_image': fake.image_url(),
                'linkage_category': fake.random_element(elements=('カテゴリA', 'カテゴリB', 'カテゴリC')),
                'remarks': fake.text(max_nb_chars=100),
                'is_deleted': fake.boolean(chance_of_getting_true=5),
                'created_by': fake.name(),
                'created_at': fake.date_time_this_year().isoformat(),
                'updated_by': fake.name(),
                'updated_at': fake.date_time_this_year().isoformat()
            }
            batch.put_item(Item=item)
            
            print(f"レコード {i+1}/{NUM_RECORDS} を追加しました。")
    
    end_time = time.time()
    print(f"{NUM_RECORDS}件のデータを挿入しました。所要時間: {end_time - start_time:.2f}秒")

def query_table(table):
    # パーティションキーで検索
    start_time = time.time()
    response = table.query(
        KeyConditionExpression=Key('employee_id').eq(table.scan(Limit=1)['Items'][0]['employee_id'])
    )
    end_time = time.time()
    print(f"パーティションキーでの検索結果: {response['Items']}")
    print(f"検索時間: {end_time - start_time:.2f}秒")
    
    # GSIで検索
    start_time = time.time()
    response = table.query(
        IndexName=GSI_NAME,
        KeyConditionExpression=Key('email').eq(table.scan(Limit=1)['Items'][0]['email'])
    )
    end_time = time.time()
    print(f"GSIでの検索結果: {response['Items']}")
    print(f"検索時間: {end_time - start_time:.2f}秒")
    
    # 全レコード検索
    start_time = time.time()
    items = []
    scan_kwargs = {}
    done = False
    while not done:
        response = table.scan(**scan_kwargs)
        items.extend(response.get('Items', []))
        scan_kwargs['ExclusiveStartKey'] = response.get('LastEvaluatedKey')
        done = scan_kwargs['ExclusiveStartKey'] is None
    end_time = time.time()
    print(f"全レコード数: {len(items)}")
    print(f"検索時間: {end_time - start_time:.2f}秒")

    # employment_typeが正社員の人を検索
    start_time = time.time()
    items = []
    scan_kwargs = {
        'FilterExpression': Key('employment_type').eq('正社員')
    }
    done = False
    while not done:
        response = table.scan(**scan_kwargs)
        items.extend(response.get('Items', []))
        scan_kwargs['ExclusiveStartKey'] = response.get('LastEvaluatedKey')
        done = scan_kwargs['ExclusiveStartKey'] is None
    end_time = time.time()
    print(f"キー無し列の検索結果: {len(items)}件")
    print(f"検索時間: {end_time - start_time:.2f}秒")