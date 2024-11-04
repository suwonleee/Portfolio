import subprocess
from ppadb.client import Client as AdbClient
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import gspread
import re
from oauth2client.service_account import ServiceAccountCredentials
from slack_sdk.web import WebClient
import traceback
import pandas as pd
import requests 

# * 코드 개요
# [ 1.안드로이드 앱 연결 -> 2.데이터 추출 ->  3.프로세싱 -> 4.적재 및 연동 ]

#ADB 작업, 데이터 수집, Google Sheets 연동을 하나의 클래스에 캡슐화
class ADBDeviceInspector:
###############################################
# 1.안드로이드 앱 연결
    def __init__(self):
        self.client = None  # ADB 클라이언트 초기화
        self.device = None  # 연결된 디바이스 객체
        self.button_bounds = []  # 버튼 좌표 저장
        self.index_0_texts = []  # 인덱스 0 텍스트 저장
        global product_name  # 전역 변수로 상품명 선언

    def start_adb_server(self):
        # ADB 서버 시작
        result = subprocess.run(['adb', 'start-server'], capture_output=True, text=True)
        if result.returncode != 0:
            print("Failed to start ADB server")  # 서버 시작 실패 메시지
            print(result.stderr)
        else:
            print("ADB server started")  # 서버 시작 성공 메시지
            print(result.stdout)

    def connect_device(self):
        # ADB 클라이언트 생성 및 디바이스 연결
        self.client = AdbClient()
        devices = self.client.devices()
        if len(devices) == 0:
            print("No devices connected")  # 디바이스 연결 실패 메시지
            return False
        self.device = devices[0]
        print(f"Connected to device: {self.device}")  # 디바이스 연결 성공 메시지
        return True
###############################################
# 2.데이터 추출
    def get_screen_xml(self):
        # 디바이스에서 UI XML 덤프 가져오기
        if not self.device:
            print("No device connected")  # 디바이스 연결 안 된 경우
            return None
        self.device.shell("uiautomator dump /sdcard/ui_dump.xml")
        self.device.pull("/sdcard/ui_dump.xml", "ui_dump.xml")
        with open("ui_dump.xml", "r", encoding="utf-8") as file:
            xml_dump = file.read()
        return xml_dump
###############################################
# 3.프로세싱
    @staticmethod
    def contains_price(text):
        # 텍스트에 가격 정보 포함 여부 확인
        return bool(re.search(r'\d+원', text))

    #필요한 텍스트 부분 파악 후 추출 
    def filter_and_sort_texts(self, texts):
        # 짝수 인덱스는 상품명, 홀수 인덱스는 가격으로 필터링 및 정렬
        filtered = []
        i = len(texts) - 1
        while i >= 0:
            split_text = texts[i].split()[0]
            price_matches = re.findall(r'\d+,?\d*원', split_text)
            if price_matches:
                filtered.append((texts[i-1], texts[i]))
            i -= 2
        return [item for pair in filtered[::-1] for item in pair]  # 원래 순서로 정렬

    #전체 데이터에서 필요한 정보 추출
    def extract_info(self, xml_dump, option_type):
        # XML에 화면 정보 저장
        soup = BeautifulSoup(xml_dump, 'xml')
        products = []
        current_date = datetime.now().strftime("%Y-%m-%d")
        nodes = soup.find_all('node')
        text_nodes = soup.find_all(attrs={"text": True})
        excluded_phrases = ["수량내리기", "수량올리기", "재입고 알림", "장바구니 담기", '담기', '나우배달']
        filtered_texts = []
        append_sold_out = False

        # 텍스트 필터링 및 품절 여부 확인
        for node in text_nodes:
            text = node.get("text")
            #품절이 되었다면 이름에 "품절" 추가하여 분류
            if text:
                if text.strip() == "(품절)":
                    append_sold_out = True
                elif not any(phrase in text for phrase in excluded_phrases) and not text.isdigit():
                    text = text.replace("*", "")
                    if append_sold_out:
                        filtered_texts.append(f"(품절) {text}")
                        append_sold_out = False
                    else:
                        filtered_texts.append(text)

        # 필터링된 텍스트를 정렬
        filtered_texts = self.filter_and_sort_texts(filtered_texts)
        is_option = len(filtered_texts) > 2
        #옵션 값이 있다면 별도 옵션 데이터 추가
        if option_type == '':
            option_type = '옵션' if is_option else ''

        # 상품 정보를 백엔드에서 원하는 형태로 딕셔너리 생성
        for i in range(0, len(filtered_texts), 2):
            detail_product_name = filtered_texts[i]
            price_text = filtered_texts[i + 1] if i + 1 < len(filtered_texts) else None
            price1, price2, discount_rate = self.extract_prices_and_discount(price_text)
            products.append({
                '날짜': current_date,
                '상품명': product_name,
                '옵션 상품명': detail_product_name,
                '옵션별 가격': price2 if price2 else price1,
                '구분': option_type,
                '가격1': price1,
                '할인율': discount_rate,
                '가격2': price2,
            })
        return products

    def extract_prices_and_discount(self, price_text):
        # 가격 및 할인율 데이터 추출
        if not price_text:
            return None, None, None
        prices = price_text.split('원')
        prices = [price for price in prices if price.strip()]
        
        #불필요한 텍스트 제거 후 원하는 형식 추가
        if len(prices) >= 2:
            price1 = prices[1].strip()
            price2 = prices[0].strip()
            try:
                price1_value = float(price1.replace(',', ''))
                price2_value = float(price2.replace(',', ''))
                discount_rate = f"{int((price1_value - price2_value) / price1_value * 100)}%"
            except ValueError:
                discount_rate = None
            return price1, price2, discount_rate
        elif len(prices) == 1:
            return prices[0].strip(), None, None
        return None, None, None

    def get_button_bounds(self, xml_dump):
        # XML 덤프에서 버튼의 좌표 추출
        soup = BeautifulSoup(xml_dump, 'xml')
        nodes = soup.find_all('node', {'class': 'android.widget.Button'})
        bounds_and_texts = []
        for node in nodes:
            text = node.get('text')
            if text in ['담기', '재입고 알림']:
                bound_str = node.get('bounds')
                if bound_str:
                    match = re.findall(r'\d+', bound_str)
                    if len(match) == 4:
                        x = (int(match[0]) + int(match[2])) // 2
                        y = (int(match[1]) + int(match[3])) // 2
                        bounds_and_texts.append(((x, y), text))
        return bounds_and_texts

    #클릭 활용 함수
    def click_button(self, x, y):
        # 디바이스에서 특정 좌표 클릭
        self.device.shell(f'input tap {x} {y}')
        time.sleep(2)  # 대기 시간
        
    #스크롤 활용 함수
    def swipe(self, direction='down', distance_fraction=1):
        # 스크롤 다운
        self.device.shell(f"input tap 5 1211")
        time.sleep(0.1)
        if direction == 'down':
            start_x, start_y, end_x, end_y = 294, 2008, 294, 1511
        else:
            start_x, start_y, end_x, end_y = 294, 1001, 294, 1908
        duration = int(500 * distance_fraction)
        self.device.shell(f"input swipe {start_x} {start_y} {end_x} {end_y} {duration}")
        time.sleep(0.1)

    #데이터 청크 단위로 분할 
    def chunked(self, data, chunk_size):
        # 데이터를 청크 단위로 나눔
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
    
    #기존 데이터 불러오기
    def load_google_sheet_data(self):
        # Google Sheets에서 데이터 로드
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://spreadsheets.google.com/feeds', 
                  'https://www.googleapis.com/auth/drive']
        SERVICE_ACCOUNT_FILE = r'C:\Users\project\python_test\2.ADB\testproject-429723-326248195827.json'
        SPREADSHEET_ID2 = '스프레드시트_ID_수정'
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
        client = gspread.authorize(creds)
        spreadsheet_copy = client.open_by_key(SPREADSHEET_ID2)
        sheet = spreadsheet_copy.worksheet('data')
        all_values = sheet.get_all_values()
        headers = all_values[3][2:10]
        data = [row[2:10] for row in all_values[4:]]
        return pd.DataFrame(data, columns=headers)

    def inspect_device(self):
        # 디바이스 상태 확인 및 데이터 수집
        global product_name
        self.start_adb_server()
        time.sleep(2)
        if self.connect_device():
            try:
                # ADB 내 키보드 활용을 위한 외부 apk 연결
                apk_path = 'C:\\Users\\project\\python_test\\2.ADB\\ADBKeyBoard.apk'
                subprocess.run(['adb', 'install', apk_path])
                original_keyboard = self.get_current_keyboard()
                self.set_keyboard('com.android.adbkeyboard/.AdbIME')
                df = self.load_google_sheet_data()
                # 필터링 및 새 데이터 수집
                #외부 키보드를 가져와서 활용(adb는 한글 키보드 활용 x)
                subprocess.run(['adb', 'install', apk_path])

                original_keyboard = self.get_current_keyboard()

                self.set_keyboard('com.android.adbkeyboard/.AdbIME')

                # df, df_total = self.load_google_sheet_data()
                df = self.load_google_sheet_data()
                
                # 어제와 오늘 날짜 데이터 필터링
                current_date = datetime.now().strftime("%Y-%m-%d")
                yesterday_date = (datetime.now() - timedelta(1)).strftime("%Y-%m-%d")

                df_today = df[df['날짜'] == current_date]
                df_yesterday = df[df['날짜'] == yesterday_date]

                # 오늘 새롭게 추가된 데이터 (NEW 데이터)
                new_data = df_today[~df_today['상품명'].isin(df_yesterday['상품명'])]

                # 다시 추가된 데이터 (리로드 데이터)
                reload_data = df_today[df_today['상품명'].isin(df_yesterday['상품명']) & ~df_yesterday['상품명'].isin(df_today['상품명'])]

                # 세부 옵션 값 확인이 필요한 이름 조건 설정 
                ### 아래 코드처럼 조건에 따라 확인 여부 설정시, 작업 시간 60% 단축 가능
                name_patterns = [ '옵션', r'택\d+', r'\d+종', r'\d+송이', '선택', '골라담기', '~', r'BEST\d+', r'BEST\s*\d+', '/']
                product_names = df_today['상품명'].dropna().tolist()
                total_products = len(product_names)

                all_products = []
                for index, product_name in enumerate(product_names):
                    if not product_name.strip():
                        continue

                    # 진행 상황 출력
                    progress = (index + 1) / total_products * 100
                    print(f"Processing {index + 1}/{total_products} ({progress:.2f}%) - {product_name}")

                    # 패턴 매칭 확인
                    pattern_found = any(re.search(pattern, product_name) for pattern in name_patterns)
                    
                    if not pattern_found:
                        # 패턴이 없는 경우 해당 df의 행을 그대로 가져와서 적재
                        matching_row = df_today[df_today['상품명'] == product_name].iloc[0]
                        # print(matching_row)
                        product_info = {
                            '날짜': matching_row['날짜'],
                            '상품명': matching_row['상품명'],
                            '옵션 상품명': matching_row.get('옵션 상품명', '').replace('나우배달', ''),
                            '옵션별 가격': matching_row.get('옵션별 가격', ''),
                            '구분': matching_row.get('구분', '').replace('나우배달', ''),
                            '가격1': matching_row.get('가격1', ''),
                            '할인율': matching_row.get('할인율', ''),
                            '가격2': matching_row.get('가격2', '')
                        }
                        all_products.append(product_info)
                        print(product_info)
                        continue

                    ###2. 만약 상품명에 name_patterns에 있다면 옵션 값 확인이 필요. -> 검색 창 검색 
                    sanitized_product_name = self.sanitize_input(product_name)

                    self.click_button(634, 156)  # 검색 창의 좌표 클릭
                    time.sleep(0.2)

                    self.click_button(756, 156)  # [732,132][780,180]의 정가운데 좌표
                    time.sleep(0.2)

                    self.send_text_via_adb_keyboard(sanitized_product_name)
                    time.sleep(0.2)
                    
                    #엔터 키 누르기
                    self.device.shell('input keyevent 66')
                    time.sleep(1)
                    self.swipe(direction='down', distance_fraction=1)  # 스크롤 다운 추가
                    time.sleep(0.2)

                    # 최신 화면의 XML 덤프를 추출
                    xml_dump = self.get_screen_xml()
                    soup = BeautifulSoup(xml_dump, 'xml')
                    # '담기' 또는 '재입고 알림' 버튼의 좌표를 저장
                    button_bounds_and_texts = self.get_button_bounds(xml_dump)
                    self.button_bounds.extend([bt[0] for bt in button_bounds_and_texts])
                    button_texts = [bt[1] for bt in button_bounds_and_texts]
                    
                    # 인덱스 0번의 텍스트와 TextView index 3 텍스트를 추출 후 저장
                    index_0_texts, text_view_texts = self.get_index_0_text_and_bounds(xml_dump)
                    
                    # 인덱스 i를 미리 초기화
                    i = -1
                    x, y = None, None
                    
                    # product_name이 text_view_texts에 들어있는 경우 해당 인덱스의 좌표를 출력
                    for i, text_view_text in reversed(list(enumerate(text_view_texts))):
                        if product_name in text_view_text:
                            if i < len(self.button_bounds):
                                x, y = self.button_bounds[i]

                    text_nodes = soup.find_all(attrs={"text": True})
                    if xml_dump:
                        filtered_texts = []
                        #필요 없는 텍스트(정보) 제외
                        excluded_phrases = ["수량내리기", "수량올리기", "(품절)", "재입고 알림", "장바구니 담기"]

                        for node in text_nodes:
                            text = node.get("text")
                            if text and not any(phrase in text for phrase in excluded_phrases):
                                # 숫자만 혼자 적혀있는 경우 제외
                                if not text.isdigit():
                                    filtered_texts.append(text)

                        # '조건값A' 이후의 인덱스들을 가져오기
                        if '조건값A' in filtered_texts:
                            now_index = filtered_texts.index('조건값A')
                            filtered_texts = filtered_texts[now_index + 1:]

                    # 버튼에 '조건값B' 가 있는 경우 
                    if i < len(button_texts) and button_texts[i] == '조건값B':
                        self.click_button(x, y)  # 첫 번째 상품 클릭 좌표
                        time.sleep(1.2)
                        detail_xml = self.get_screen_xml()
                        soup = BeautifulSoup(detail_xml, 'xml')
                        
                        # 조건 확인 및 option_type 업데이트
                        option_type = '옵션'
                        
                        # 옵션 창이 켜졌다면 정보 추출
                        if '장바구니 담기' in detail_xml:
                            details = self.extract_info(detail_xml, option_type)
                            all_products.extend(details)
                            time.sleep(2)
                            
                        # 옵션 창이 잘못해서 켜졌다면 정확한 버튼으로 유도하여 정보 추출
                        if '구매하기' in detail_xml:
                            print('구매하기 버튼을 잘못눌렀습니다.')
                            self.click_button(540, 1974)  # 상세 페이지 내 구매하기 버튼 좌표
                            time.sleep(1.2)
                        
                            detail_xml = self.get_screen_xml()
                            option_type = '옵션'
                            details = self.extract_info(detail_xml, option_type)
                            all_products.extend(details)
                            self.device.shell('input keyevent 4')  # 뒤로 가기
                            time.sleep(2)

                    # 버튼에 '재입고 알림'이 있는 경우 
                    else:
                        print('재입고 알림의 경우입니다.')
                        self.click_button(x, y + 100)  # y좌표를 해당 ui에 맞게 클릭
                        time.sleep(2.4)
                        
                        self.click_button(540, 1974)  # 상세 페이지 내 구매하기 버튼 좌표
                        time.sleep(1.2)
                    
                        detail_xml = self.get_screen_xml()
                        option_type = '옵션'
                        details = self.extract_info(detail_xml, option_type)
                        all_products.extend(details)
                        self.device.shell('input keyevent 4')  # 뒤로 가기 키 입력
                        time.sleep(2)
                    time.sleep(1)
                self.set_keyboard(original_keyboard)
                self.append_to_google_sheet(all_products)
            except Exception as e:
                # Slack 오류 메시지 전송
                slack_token = '슬랙 토큰 수정'
                client = WebClient(token=slack_token)
                error_message = f"오류 발생: {traceback.format_exc()}"
                client.chat_postMessage(channel='price_channel', text=error_message)
            finally:
                print("세션 종료, 불필요한 데이터를 정리합니다.")
                self.device.shell('am force-stop io.appium.uiautomator2.server')

    def click_saved_buttons(self):
        # 저장된 버튼 좌표 클릭
        for x, y in self.button_bounds:
            self.click_button(x, y)
            
###############################################
# 4. 데이터 적재 및 연동
def append_to_google_sheet(self, products):
    # Google Sheets에 데이터 추가
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
              'https://spreadsheets.google.com/feeds', 
              'https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = r'C:\Users\project\python_test\2.ADB\testproject-429723-326248195827.json'
    SPREADSHEET_ID = '스프레드시트_ID_수정'
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet('옵션')
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title='옵션', rows="1000", cols="20")

    data_to_add = [[p['날짜'], p['상품명'], p['옵션 상품명'], p['옵션별 가격'], p['구분'], p['가격1'], p['할인율'], p['가격2']] for p in products]
    next_row = len(sheet.col_values(3)) + 1
    for data_chunk in self.chunked(data_to_add, 50):  # 50개씩 처리
        sheet.append_rows(data_chunk)
        time.sleep(1)
        next_row += len(data_chunk)  # 다음 청크의 시작 행

    # 데이터 백엔드 API에 전송
    try:
        api_url = "https://api-endpoint.url/data 수정"  # 백엔드 API URL
        headers = {
            "Content-Type": "application/json",
            "Authorization": "인증-api-token 수정"  # 인증 토큰
        }
        payload = {"products": products}
        response = requests.post(api_url, json=payload, headers=headers)

        if response.status_code == 200:
            print("데이터 백엔드 API 전송 성공")
            # 성공적인 전송 후 Slack 메시지 전송
            slack_token = '슬랙 토큰 수정'
            slack_client = WebClient(token=slack_token)
            slack_client.chat_postMessage(channel='price_channel', text='데이터 적재 및 API 전송 성공')
        else:
            print(f"API 전송 실패: {response.status_code}, {response.text}")
            # 실패 시 Slack 메시지 전송
            slack_client = WebClient(token=slack_token)
            slack_client.chat_postMessage(channel='price_channel', text=f"데이터 적재는 성공했지만 API 전송 실패: {response.status_code}")
    except Exception as e:
        print(f"API 전송 중 오류 발생: {str(e)}")
        # 예외 처리 시 Slack 메시지 전송
        slack_client = WebClient(token=slack_token)
        slack_client.chat_postMessage(channel='price_channel', text=f"API 전송 중 오류 발생: {str(e)}")
    
if __name__ == "__main__":
    inspector = ADBDeviceInspector()
    inspector.inspect_device()
    inspector.click_saved_buttons()
