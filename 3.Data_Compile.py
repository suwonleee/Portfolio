from datetime import date, datetime, timedelta
import requests
import pandas as pd
import json
import pathlib
import boto3
import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    dag_id="update_data",
    description="Web data scraping and update with Spark and S3",  # 설명
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 9,21 * * *',  # 하루 두 번 실행
    default_args=default_args,
    catchup=False
)

# SparkSession 전역 설정
spark = SparkSession.builder.appName("WebDataETL").getOrCreate()

def get_s3_client():
    """AWS S3 클라이언트 생성 및 반환"""
    return boto3.client('s3', region_name='ap-northeast-2')  # S3 리전 설정

def fetch_web_data() -> list:
    """Selenium을 사용해 웹사이트에서 데이터를 파싱하여 JSON 형식으로 반환"""
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--incognito")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.implicitly_wait(5)

    base_url = 'http://데이터_체크_링크.com'
    driver.get(base_url)
    time.sleep(3)  # 페이지 로드를 위해 지연 추가

    product_data = []
    try:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        products = soup.select('li.prod_item.prod_layer')
        for product in products:
            try:
                name = product.select_one('p.prod_name > a').text.strip()
                price = product.select_one('p.price_sect > a').text.strip()
                img_link = product.select_one('div.thumb_image > a > img').get('data-original') or product.select_one('div.thumb_image > a > img').get('src')
                img_url = f"https://{img_link.split('//')[1].split('?')[0]}" if img_link else "N/A"
                detail_link = product.select_one('div.thumb_image > a').get('href', 'N/A')

                # 데이터 필터링 (중고/재고 없음)
                if any(term in name for term in ["중고", "리퍼비시", "재고 없음"]):
                    continue

                product_data.append({
                    "제품모델명": name,
                    "가격": price,
                    "이미지URL": img_url,
                    "상세페이지링크": detail_link
                })
            except Exception as e:
                logging.error(f"제품 처리 중 에러 발생: {e}")
    except Exception as e:
        logging.error(f"데이터 크롤링 중 에러 발생: {e}")
    finally:
        driver.quit()

    return product_data

def read_data_from_s3(s3_client, bucket_name: str, file_key: str):
    """S3에서 기존 데이터를 읽어 Spark DataFrame으로 반환"""
    try:
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        json_content = s3_object['Body'].read().decode('utf-8')
        json_data = json.loads(json_content)
        df = spark.createDataFrame(json_data)
        logging.info("S3에서 데이터를 성공적으로 로드했습니다.")
        return df
    except s3_client.exceptions.NoSuchKey:
        logging.warning("S3에 기존 데이터가 없습니다. 빈 DataFrame을 반환합니다.")
        return spark.createDataFrame([], schema=None)
    except Exception as e:
        logging.error(f"S3 데이터 읽기에 실패했습니다: {e}")
        raise

def write_data_to_s3(s3_client, bucket_name: str, file_key: str, df):
    """Spark DataFrame을 S3에 JSON 형식으로 저장"""
    try:
        json_data = df.toPandas().to_dict(orient='records')
        json_buffer = json.dumps(json_data)
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=json_buffer)
        logging.info("S3에 데이터가 성공적으로 업로드되었습니다.")
    except Exception as e:
        logging.error(f"S3 데이터 업로드에 실패했습니다: {e}")
        raise

def etl_process():
    """ETL 프로세스: 웹 데이터 파싱 -> S3 데이터 읽기 -> 비교 및 업데이트"""
    s3_client = get_s3_client()
    bucket_name = 'gggh-s3-bucket'
    file_key = 'data/products.json'

    # 데이터 수집 및 처리
    parsed_data = fetch_web_data()
    logging.info(f"총 {len(parsed_data)}개의 제품 데이터를 수집했습니다.")

    existing_df = read_data_from_s3(s3_client, bucket_name, file_key)

    # 중복 제거: 제품 모델명을 기준으로
    existing_names = {row["제품모델명"] for row in existing_df.collect()}
    new_data = [item for item in parsed_data if item["제품모델명"] not in existing_names]

    if new_data:
        new_df = spark.createDataFrame(new_data)
        combined_df = existing_df.union(new_df).dropDuplicates(["제품모델명"])
        write_data_to_s3(s3_client, bucket_name, file_key, combined_df)
        logging.info(f"S3 데이터가 업데이트되었습니다. 추가된 데이터: {len(new_data)}개")
    else:
        logging.info("S3 데이터에 변경 내용이 없습니다.")

etl_task = PythonOperator(
    task_id='run_etl_process',
    python_callable=etl_process,
    dag=dag
)
