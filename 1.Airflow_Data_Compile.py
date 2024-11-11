#전체 코드 흐름 
## 1.환경 설정 -> 2.신규 데이터 추출 -> 3.기존 데이터와 비교(추가 데이터 확인) -> 4.S3 및 RDS에 데이터 로드
## RDS : 웹사이트 활용 / S3 : 데이터 백업 및 체크 

from datetime import datetime, timedelta
import pymysql
import json
import boto3
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python import PythonOperator

####################
# 1. 환경설정
# 기본 DAG 설정
default_args = {
    'owner': 'soowon',
    'depends_on_past': False,
    'email': ['suwonleee@naver.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="update_data_pipeline",
    description="ETL Pipeline: Web data extraction -> S3 load -> RDS update",
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 9,21 * * *',  # 하루 두 번 실행
    default_args=default_args,
    catchup=False
)

# SparkSession 전역 설정
spark = SparkSession.builder.appName("WebDataETL").getOrCreate()

def get_s3_client():
    """AWS S3 클라이언트 생성 및 반환"""
    return boto3.client('s3', region_name='ap-northeast-2')

def get_rds_credentials():
    """Secrets Manager에서 RDS 크레덴셜 가져오기"""
    client = boto3.client('secretsmanager', region_name='ap-northeast-2')
    response = client.get_secret_value(SecretId='RDS_Credentials')
    return json.loads(response['SecretString'])

####################
# 2.신규 데이터 추출 
def fetch_web_data(**kwargs):
    """웹 데이터를 추출하여 XCom에 저장"""
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(5)

    base_url = 'http://데이터_체크_링크.com'
    driver.get(base_url)

    product_data = []
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.prod_item.prod_layer"))
        )
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        products = soup.select('li.prod_item.prod_layer')
        for product in products:
            try:
                name = product.select_one('p.prod_name > a').text.strip()
                price = product.select_one('p.price_sect > a').text.strip()
                img_link = product.select_one('div.thumb_image > a > img').get('data-original') or product.select_one('div.thumb_image > a > img').get('src')
                img_url = f"https://{img_link.split('//')[1].split('?')[0]}" if img_link else "N/A"
                detail_link = product.select_one('div.thumb_image > a').get('href', 'N/A')
                detail_spec = product.select_one('div.info > a').text.strip()

                if any(term in name for term in ["중고", "리퍼비시", "재고 없음"]):
                    continue

                product_data.append({
                    "제품모델명": name,
                    "가격": price,
                    "이미지URL": img_url,
                    "상세페이지링크": detail_link,
                    "상세스펙": detail_spec
                })
            except Exception as e:
                logging.error(f"제품 처리 중 에러 발생: {e}")
    except Exception as e:
        logging.error(f"데이터 확인 중 에러 발생: {e}")
    finally:
        driver.quit()

    kwargs['ti'].xcom_push(key='web_data', value=product_data)

####################
# 3.기존 데이터와 비교(추가 데이터 확인) 
def read_data_from_s3(s3_client, bucket_name, file_key):
    """S3에서 기존 데이터를 Spark DataFrame으로 로드.
    S3에 저장된 데이터는 JSON 형식이며 다음 스키마를 따릅니다:
    전체 용량 57.4 GB (2024.04.12)
    - 제품모델명: STRING
    - 가격: STRING
    - 이미지URL: STRING
    - 상세페이지링크: STRING
    - 상세스펙: STRING"""
    try:
        df = spark.read.json(f"s3a://{bucket_name}/{file_key}")
        df = df.repartition(10).cache()
        logging.info("S3에서 데이터를 성공적으로 로드했습니다.")
        return df
    except Exception as e:
        logging.warning(f"S3 데이터 로드 실패: {e}")
        return spark.createDataFrame([], schema=None)

def write_data_to_s3(s3_client, bucket_name, file_key, df):
    """Spark DataFrame을 JSON 형식으로 S3에 저장"""
    try:
        json_data = df.toPandas().to_dict(orient='records')
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(json_data))
        logging.info("S3에 데이터가 성공적으로 업로드되었습니다.")
    except Exception as e:
        logging.error(f"S3 데이터 업로드 실패: {e}")
        raise

####################
# 4.S3 및 RDS에 데이터 로드
def update_rds(new_data):
    """RDS 데이터베이스 업데이트"""
    creds = get_rds_credentials()
    connection = pymysql.connect(
        host=creds['host'],
        user=creds['username'],
        password=creds['password'],
        database=creds['database']
    )
    try:
        with connection.cursor() as cursor:
            for item in new_data:
                sql = """
                INSERT INTO products (제품모델명, 가격, 이미지URL, 상세페이지링크, 상세스펙)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    가격 = VALUES(가격),
                    이미지URL = VALUES(이미지URL),
                    상세페이지링크 = VALUES(상세페이지링크),
                    상세스펙 = VALUES(상세스펙)
                """
                cursor.execute(sql, (item["제품모델명"], item["가격"], item["이미지URL"], item["상세페이지링크"], item["상세스펙"]))
        connection.commit()
        logging.info(f"RDS 업데이트 완료: {len(new_data)}개의 데이터가 처리되었습니다.")
    finally:
        connection.close()

def etl_process_with_rds(**kwargs):
    """ETL 프로세스: S3 데이터 읽기 -> 새로운 데이터 파악 -> S3 및 RDS 업데이트"""
    s3_client = get_s3_client()
    bucket_name = 'gggh-s3-bucket'
    file_key = 'data/products.json'

    parsed_data = kwargs['ti'].xcom_pull(key='web_data')
    existing_df = read_data_from_s3(s3_client, bucket_name, file_key)

    # Spark SQL을 사용해 중복 확인 및 필터링
    new_df = spark.createDataFrame(parsed_data)
    combined_df = existing_df.union(new_df).dropDuplicates(["제품모델명"])
    write_data_to_s3(s3_client, bucket_name, file_key, combined_df)

    new_data = new_df.subtract(existing_df)  # 새로운 데이터만 추출
    if not new_data.isEmpty():
        update_rds(new_data.collect())
    else:
        logging.info("새로운 데이터가 없습니다.")

fetch_task = PythonOperator(
    task_id='fetch_web_data',
    python_callable=fetch_web_data,
    dag=dag,
    provide_context=True
)

etl_task = PythonOperator(
    task_id='etl_process',
    python_callable=etl_process_with_rds,
    dag=dag,
    provide_context=True
)

fetch_task >> etl_task
