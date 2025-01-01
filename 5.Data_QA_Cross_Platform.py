from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
import pymysql

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller

# 1.크로스 브라우저 세팅
# Chrome 옵션 설정
def get_chrome_options():
    options = webdriver.ChromeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument('--disable-popup-blocking')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36')
    options.page_load_strategy = 'eager'
    return options

# Edge 옵션 설정
def get_edge_options():
    options = webdriver.EdgeOptions()
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--disable-popup-blocking")
    options.add_argument('--no-sandbox')
    return options
#...

# Firefox 옵션 설정
def get_firefox_options():
    options = webdriver.FirefoxOptions()
    options.set_preference("dom.webdriver.enabled", False)
    options.set_preference("useAutomationExtension", False)
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--headless")  # 필요 시 헤드리스 모드
    return options

# 브라우저 선택 및 드라이버 실행
def get_webdriver(browser_name):
    if browser_name == "chrome":
        chromedriver_autoinstaller.install()  # Chrome 드라이버 자동 설치
        return webdriver.Chrome(options=get_chrome_options())
    elif browser_name == "firefox":
        geckodriver_autoinstaller.install()  # Firefox 드라이버 자동 설치
        return webdriver.Firefox(service=FirefoxService(), options=get_firefox_options())
    elif browser_name == "edge":
        edgedriver_autoinstaller.install()  # Edge 드라이버 자동 설치
        return webdriver.Edge(service=EdgeService(), options=get_edge_options())
    else:
        raise ValueError(f"지원하지 않는 브라우저: {browser_name}")

# ChromeDriver 자동 설치 및 설정
path = chromedriver_autoinstaller.install()
webdriver_service = Service(path)

# 서비스 계정 키 파일 경로
SERVICE_ACCOUNT_FILE = r'json 파일 경로'

# 2.구글 시트 세팅
# Google Sheets API 연결 설정
def get_google_sheets_service():
# Google Sheets API 인증
  SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://spreadsheets.google.com/feeds',
          'https://www.googleapis.com/auth/drive']    
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    return service

# 3.데이터 정합성 체크가 필요한 조건 읽어오기
def get_user_input(browser_name):
    SPREADSHEET_ID = "시트_주소_입력"
    sheet_name_map = {
        "chrome": "data_qa_chrome",
        "firefox": "data_qa_firefox",
        "edge": "data_qa_edge"
    }
    # 브라우저 이름으로 시트 이름 매핑
    sheet_name = sheet_name_map.get(browser_name.lower())
    RANGE_NAME = f"{sheet_name}!B7:N7"  # 동적으로 시트 이름 설정
    service = get_google_sheets_service()

    # Google Sheets 데이터 읽기
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])

    if not values or not values[0]:
        raise ValueError("구글 시트에 데이터가 없습니다.")

    # Google Sheets 데이터를 딕셔너리로 변환
    keys = [
        "price_limit", "plays_games", "game_specs", "does_design_work", 
        "design_specs", "works_outside", "weight_limit", "uses_3d_software", 
        "preferred_game_specs", "screen_preference", "min_screen_size", 
        "does_programming", "programming_type"
    ]
    user_input = {key: int(value) if key != "price_limit" else float(value) for key, value in zip(keys, values[0])}
    return user_input

# Google Sheets에 데이터 업데이트
def update_google_sheets(spreadsheet_id, cell_range, value):
    service = get_google_sheets_service()
    body = {
        "values": [[value]]
    }
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=cell_range,
        valueInputOption="RAW",
        body=body
    ).execute()

# 4.RDS와 웹 실제 데이터 정합성 비교
def compare_data(rds_data, web_data):
    mismatched_data = []
    for rds_title, web_title in zip(rds_data, web_data):
        if rds_title != web_title:
            mismatched_data.append({"rds": rds_title, "web": web_title})
    return mismatched_data

# 데이터 정합성 비교 및 결과 기록
def compare_data_and_record(spreadsheet_id, row_number, rds_data, web_data):
    mismatched_data = compare_data(rds_data, web_data)
    
    # 결과에 따라 Google Sheets 업데이트
    if mismatched_data:
        print("불일치 데이터가 발견되었습니다.")
        for mismatch in mismatched_data:
            print(f"RDS: {mismatch['rds']} | 웹: {mismatch['web']}")
        record_result = "불일치"
    else:
        print("모든 데이터가 일치합니다.")
        record_result = "일치"
    
    # O열에 결과 기록
    cell_range = f"check_data!O{row_number}"  # N 다음 열 (O열)
    update_google_sheets(spreadsheet_id, cell_range, record_result)

    return mismatched_data

# 5. RDS 데이터 로드
def check_rds_data(user_input):
    connection = pymysql.connect(
        host="rds-hostname",
        user="username",
        password="password",
        database="database_name"
    )
    query = """
    SELECT product_title FROM products
    WHERE price <= %(price_limit)s
    AND plays_games = %(plays_games)s
    AND game_specs = %(game_specs)s
    AND does_design_work = %(does_design_work)s
    AND design_specs = %(design_specs)s
    AND works_outside = %(works_outside)s
    AND weight_limit = %(weight_limit)s
    AND uses_3d_software = %(uses_3d_software)s
    AND preferred_game_specs = %(preferred_game_specs)s
    AND screen_preference = %(screen_preference)s
    AND min_screen_size = %(min_screen_size)s
    AND does_programming = %(does_programming)s
    AND programming_type = %(programming_type)s
    """
    with connection.cursor() as cursor:
        cursor.execute(query, user_input)
        rds_data = [row[0] for row in cursor.fetchall()]
    connection.close()
    return rds_data

# 6. 웹 데이터 로드
def check_web_data(user_input, browser_name="chrome"):
    base_url = (
      "https://example.info/?isButton=false&recordsNo1={price_limit}&recordsNo2={plays_games}&recordsNo3={game_specs}&recordsNo4={does_design_work}&recordsNo5={design_specs}&recordsNo6={works_outside}&recordsNo7={weight_limit}&recordsNo8={uses_3d_software}&recordsNo9={preferred_game_specs}&recordsNo10={screen_preference}&recordsNo11={min_screen_size}&recordsNo12={does_programming}&recordsNo13={programming_type}"
    ).format(**user_input)

    # 브라우저 조건에 맞춰 실행
    driver = get_webdriver(browser_name)
    driver.get(base_url)

    # 웹 데이터 추출
    web_data = []
    try:
        # 페이지 로딩 대기
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#root > div > div"))
        )

        # 상품 데이터를 반복적으로 추출
        product_index = 5  # 첫 번째 상품이 위치한 div의 인덱스
        max_products = 20  # 최대 가져올 상품 개수 (필요 시 조정)

        for i in range(product_index, product_index + max_products):
            try:
                selector = f"#root > div > div > div:nth-child(2) > div > div:nth-child({i}) > div.MuiCardContent-root.css-1bihocf > div.MuiBox-root.css-pct6nk > div.MuiBox-root.css-dvxtzn > div.MuiTypography-root.MuiTypography-h5.css-vpy7xc"
                product_title = driver.find_element(By.CSS_SELECTOR, selector).text
                web_data.append(product_title)
            except Exception as e:
                continue  # 다음 상품으로 진행
    except Exception as e:
        print(f"웹 데이터 가져오는 중 오류 발생: {e}")
    finally:
        driver.quit()
    return web_data

# 실행 흐름
if __name__ == "__main__":
    try:
        # 확인할 사용자 입력값 생성
        user_input = get_user_input()
        print("사용자 입력값:", user_input)

        # RDS 데이터 가져오기
        rds_data = check_rds_data(user_input)
        print("RDS 데이터:", rds_data)

        # 크로스 브라우저 테스트
        for browser in ["chrome", "firefox", "edge"]:
            print(f"{browser} 브라우저에서 웹 데이터 가져오기 실행")
            web_data = check_web_data(user_input, browser_name=browser)
            print(f"{browser} 브라우저 웹 데이터: {web_data}")

        # Google Sheets에 비교 결과 기록
        SPREADSHEET_ID = "시트_주소_입력"
        ROW_NUMBER = 7  # 예시는 시트 7행 기준
        compare_data_and_record(SPREADSHEET_ID, ROW_NUMBER, rds_data, web_data)

    except Exception as e:
        print(f"오류 발생: {e}")
