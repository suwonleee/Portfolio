import pandas as pd
import gspread
import chromedriver_autoinstaller
import random
import urllib.parse

from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import NoSuchElementException, TimeoutException, UnexpectedAlertPresentException, NoAlertPresentException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import date
from gspread_formatting import * 

# * 코드 개요
# [ 1.기존 데이터 연결 -> 2.데이터 추출 ->  3.프로세싱 및 확인 자동화 -> 4.조건별 데이터 추가]

###############################################
# 1.기존 데이터 연결
# 서비스 계정 키 파일 경로
SERVICE_ACCOUNT_FILE = r'C:\\Users\\Woowahan\\python_test\\2.automation_project\\testproject-429723-326248195827.json'

# Google Sheets API 인증
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://spreadsheets.google.com/feeds',
          'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
client = gspread.authorize(creds)

# 구글 시트 열기
sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/세부_시트_주소")
worksheet = sheet.worksheet("data")

# G열의 모든 URL과 F열의 값을 가져오기
urls = worksheet.col_values(7)[1:]  # G2셀부터 끝까지 G열 (URL)
f_values = worksheet.col_values(6)[1:]  # F2셀부터 끝까지 F열 (메시지에 포함할 값)
f_titles = worksheet.col_values(3)[1:]  # F2셀부터 끝까지 F열 (메시지에 포함할 값)


###############################################
# 2.데이터 추출
# Chrome 옵션 설정
options = webdriver.ChromeOptions()
options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
options.add_experimental_option('useAutomationExtension', False)
options.add_argument("--disable-backgrounding-occluded-windows")
userAgent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
options.add_argument('user-agent=' + userAgent)
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-popup-blocking')
#페이지 로딩을 최소화 하기 위해, 멀티 페이지 로딩
options.page_load_strategy = 'eager'

# ChromeDriver 자동 설치 및 설정
path = chromedriver_autoinstaller.install()
webdriver_service = Service(path)

# 한번에 여러개의 사이트를 접속하여 로딩 시간 단축
def open_multiple_windows(start_row, num_windows=10):
    drivers = []
    # 역순으로 N+10에서 N까지 열기
    for i in range(start_row + num_windows - 2, start_row - 3, -1):  # N+10에서 N까지
        if i < 0 or i >= len(urls):
            continue
        url = urls[i]
        
        # 새로운 Chrome 창을 열고 URL을 로드
        driver = webdriver.Chrome(service=webdriver_service, options=options)
        driver.get(url)
        print(f"Opened URL in window for row {i + 2}")
        
        drivers.append(driver)

    return drivers

# 판매처별 사이트 팝업을 감지하고 닫는 함수
def close_popup(driver, seller):
    try:
        if seller == '사이트1':
            # 팝업 닫기 버튼 대기 후 클릭
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div/div/div[2]/div/div/button')))
            close_button = driver.find_element(By.XPATH, '/html/body/div[2]/div/div/div[2]/div/div/button')
            close_button.click()
            print("사이트1 팝업을 닫았습니다.")
        if seller == '사이트2':
            # 팝업 닫기 버튼 대기 후 클릭
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="name"]/div/div[2]/a[2]')))
            close_button = driver.find_element(By.XPATH, '//*[@id="name"]/div/div[2]/a[2]')
            close_button.click()
            print("사이트2 팝업을 닫았습니다.")
        else:
            # 기본 팝업 닫기 위치 (다른 판매처에 적용할 수 있는 공통 클래스명 또는 셀렉터 추가 가능)
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.popup-close-button')))
            close_button = driver.find_element(By.CSS_SELECTOR, '.popup-close-button')
            close_button.click()
            print("기본 팝업을 닫았습니다.")
    except (TimeoutException, NoSuchElementException):
        print(f"팝업이 나타나지 않았습니다. 그대로 진행합니다.")


###############################################
# 3.프로세싱 및 확인 자동화
def process_windows(drivers, start_row, seller, num_windows=10):
    # 질문은 정순으로 처리
    for index in range(start_row, start_row + num_windows):
        if index >= len(f_values):
            break
        product_price = f_values[index - 2]
        product_title = f_titles[index - 2]
        
        # 판매처에 따른 XPath 동적 변경
        if seller == '사이트1':
            price_xpath = '//*[@id="content"]/div[2]/div[1]/div[2]/div[6]/div/strong'
            shipping_xpath = '//*[@id="content"]/div[2]/div[1]/div[2]/div[5]/div[1]/dl/dd'
        elif seller == '사이트2':
            price_xpath = '//*[@id="stickyTopParent"]/div[2]/div[1]/div/div/div/span'
            shipping_xpath = '//*[@id="stickyTopParent"]/div[2]/div[3]/div[1]/div/div[2]/div/div[2]/button'
        elif seller == '사이트3':
            price_xpath = '//*[@id="itemcase_basic"]/div[1]/div[2]/span[3]/strong'
            shipping_xpath = '//*[@id="container"]/div[3]/div[2]/div[2]/ul/li[1]/div[2]/div[1]/div[2]/div[2]/span[1]'
        #... 사이트4, 사이트5, ...
        else:
            # 특정 사이트의 요소를 찾고 텍스트가 "기본 사이트"인지 확인
            try:
                naver_element = drivers.find_element(By.XPATH, '//*[@id="header"]/div/div[1]/div[1]/a[2]/span')
                if naver_element.text == '기본 사이트':
                    print(f"기본 사이트 상품을 처리합니다. 판매처: {seller}")
                    price_xpath = '//*[@id="content"]/div/div[2]/div[2]/fieldset/div[1]/div[2]/div/strong/span[2]'  
                    shipping_xpath = '//*[@id="content"]/div/div[2]/div[2]/fieldset/div[1]/div[2]/div/div/span' 
                else:
                    print(f"{index}번째 데이터에서 지원되지 않는 판매처입니다: {seller}")
                    continue
            except NoSuchElementException:
                print(f"판매처가 지원되지 않음: {seller}")
                continue
            
        print(f'{seller} 상품입니다.')
        price_element = drivers.find_element(By.XPATH, price_xpath)
        shipping_element = drivers.find_element(By.XPATH, shipping_xpath)
        
        # 가격 텍스트 전처리
        price_text = int(price_element.text.replace(',', '').replace('원', ''))

        # shipping_element.text를 공백으로 분리하여 리스트로 저장
        shipping_text = shipping_element.text
        elements = shipping_element.text.split()
        
        # 숫자 + 원 형식이 있는지 확인 / 무료배송이 있는 경우 처리
        shipping_price = None
        if '무료배송' in shipping_text or '무료' in shipping_text:
            print('무료배송이어서 숫자 0을 넣습니다.')
            shipping_price = 0
        else:
            for element in elements:
                if re.match(r'^\d{1,3}(,\d{3})*(원)?$', element) or re.match(r'^\d+(원)?$', element):
                    # '원'과 ','를 제거한 후 int로 변환
                    shipping_price = int(element.replace(',', '').replace('원', ''))
                    break  # 첫 번째로 일치하는 값을 찾으면 종료
            else:
                shipping_price = 0

        # 실제 판매 가격 계산
        sell_price = int(price_text + shipping_price)
        print(f"판매처 실제 가격: {sell_price}")

###############################################
# 4.조건별 데이터 추가
        # 실제 판매 가격과 시트 최저가 비교
        if sell_price == int(product_price):
            # 가격이 같으면 기존 값을 그대로 데이터에 입력하고 다음 행으로 자동 진행
            worksheet.update_cell(index, 9, product_price)
            print(f"'{product_price}'원이 정확합니다. 그대로 입력하고 다음 행으로 이동합니다.")
        # 다를 경우 스탭 분이 확인할 수 있도록 입력 창 출력
        else:
            # 가격이 다르면 사용자 입력을 기다림
            user_input = input(f"---------------- \n{product_price} 원 \n \n '{product_title} 제품명의 // 가격이 {product_price}'원이 맞습니까? 에러로 인해 다음 URL로 이동하려면 엔터키를 눌러주시고, 가격을 수정하려면 숫자를 입력하세요: ")
            
            # 입력값 처리
            try:
                new_value = float(user_input)
                worksheet.update_cell(index, 9, new_value)  # 시트에 입력값 업데이트 (행 번호 그대로 사용)
            except ValueError:
                #사이트 에러의 경우 공백 입력 시 다음 사이트 진행
                if user_input.lower() == '':
                    worksheet.update_cell(index, 9, "")  # 빈 값으로 업데이트
                    print(f"입력하지 않고 다음 행으로 이동합니다.")
                #숫자나 공백이 아닌 다른 키 입력 시 재요청
                else:
                    print("잘못된 입력입니다. 수정할 숫자를 입력하거나 공백 상태에서 엔터키를 누르세요.")
                    process_windows(drivers, index)  # 잘못된 입력이면 다시 시도
        # 창을 닫을 때 예외 처리 및 팝업 처리
        try:
            # 입력이 끝나면 가장 최근에 열린 창부터 닫기
            drivers.pop().close()  # pop()으로 가장 최근에 열린 창을 닫음
            print(f"창 {index}을 닫았습니다.")  # 실제 행 번호 출력
        except UnexpectedAlertPresentException as e:
            # 팝업(Alert)이 발생한 경우 처리
            print(f"Unexpected alert detected: {e.alert_text}")
            try:
                # 팝업이 뜰 경우 닫기
                close_popup(drivers, seller)
                print("Alert closed successfully.")
            except NoAlertPresentException:
                print("No alert found.")

# 시작할 행을 사용자로부터 입력받기
while True:
    try:
        start_row = int(input("몇 행부터 시작하시겠습니까? (숫자를 입력하세요): "))
        if start_row >= 2:
            break
        else:
            print("2 이상의 숫자를 입력하세요.")
    except ValueError:
        print("올바른 숫자를 입력하세요.")

# 무한 루프로 계속해서 10개씩 처리
while True:
    drivers = open_multiple_windows(start_row)
    process_windows(drivers, start_row)
    
    # 다음 10개로 넘어가기 위해 start_row 업데이트
    start_row += 10
    
    # 모든 데이터를 처리한 경우 반복 종료
    if start_row > len(urls):
        print("모든 데이터를 처리했습니다.")
        break
    
    # 사용자가 종료하지 않는 이상 계속 처리
    print(f"{start_row}번째부터 다음 10개의 데이터를 처리합니다...")