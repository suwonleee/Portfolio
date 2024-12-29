package test;

import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.Collections;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.appium.java_client.AppiumDriver;
import io.appium.java_client.android.AndroidDriver;
import io.appium.java_client.android.AndroidElement;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

// 1. 구글 시트 연결
public class GoogleSheetsUtil {
    private static Sheets sheetsService;

    public static Sheets getSheetsService() throws Exception {
        if (sheetsService == null) {
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("src/main/resources/credentials.json"))
                    .createScoped(Collections.singletonList(SheetsScopes.SPREADSHEETS));
            sheetsService = new Sheets.Builder(new com.google.api.client.http.javanet.NetHttpTransport(),
                                                com.google.api.client.json.JsonFactory.getDefaultInstance(),
                                                new HttpCredentialsAdapter(credentials))
                    .setApplicationName("구글 시트 API에 연결했습니다.")
                    .build();
        }
        return sheetsService;
    }

    public static void updateCell(String spreadsheetId, String range, String value) throws Exception {
        ValueRange body = new ValueRange()
                .setValues(Collections.singletonList(Collections.singletonList(value)));
        getSheetsService().spreadsheets().values()
                .update(spreadsheetId, range, body)
                .setValueInputOption("RAW")
                .execute();
    }
}

// 2. 테스트 실행
public class AutoTest {
    public static AndroidDriver<AndroidElement> driver;
    public static DesiredCapabilities capabilities = new DesiredCapabilities();
    private List<Product> productList = new ArrayList<>();
    private String category = "서비스_카테고리_입력"; // Default category

    @BeforeClass
    public void setUp() throws Exception {
        capabilities.setCapability("appPackage", "com.dbs.baemin.m2");
        capabilities.setCapability("appActivity", "com.dbs.baemin.m2.a_new_presentation.start.AppStarterActivity");
        capabilities.setCapability("deviceName", "phone");
        capabilities.setCapability("udid", "핸드폰_아이디_입력");
        capabilities.setCapability("unicodeKeyboard", "true");
        capabilities.setCapability("resetKeyboard", "true");
        capabilities.setCapability("automationName", "UiAutomator2");

        System.out.println("Appium 서버에 연결 중입니다...");
        driver = new AndroidDriver<>(new URL("http://127.0.0.1:포트번호_입력"), capabilities);
        driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
        System.out.println("Appium 서버에 연결했습니다.");
    }

    @Test
    public void loginTest() throws Exception {
        WebDriverWait wait = new WebDriverWait(driver, 20);

        // A. 팝업 처리
        // 확인 버튼 클릭 (첫 화면에서 나오는 팝업 처리)
        dismissPopups();

        // B. 회원 정보로 이동
        // 메인 화면으로 진입 후 로그인 과정 수행
        WebElement myBaeminButton = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("com.dbs.baemin.m2:id/mybaemin")));
        myBaeminButton.click();

        // C. 로그인 과정 진행
        try {
            WebElement idField = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("com.dbs.baemin.m2:id/etId")));
            idField.sendKeys("아이디_입력");

            WebElement passwordField = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("com.dbs.baemin.m2:id/etPassword")));
            passwordField.sendKeys("비밀번호_입력");

            WebElement loginButton = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("com.dbs.baemin.m2:id/loginButton")));
            loginButton.click();

            // Google Sheets에 성공(True) 기록
            GoogleSheetsUtil.updateCell("시트_링크_입력", "test!D2", "True"); // D열에 기록
        } catch (Exception e) {
            System.out.println("로그인 필드가 없어서 로그인 과정을 건너뜁니다.");

            // Google Sheets에 실패(False) 기록
            GoogleSheetsUtil.updateCell("시트_링크_입력", "test!D2", "False"); // D열에 기록
        }

        // D. 신규 서비스 접근
        WebElement homeButton = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id("com.dbs.baemin.m2:id/home")));
        homeButton.click();
        waitForPageLoad(); // 홈 페이지 로딩 대기

        try {
            // 신규 서비스 버튼 클릭 (최대 3번 시도)
            clickElementWithRetries(By.id("com.dbs.baemin.m2:id/신규_서비스_버튼"), 3);
            waitForPageLoad(); // 페이지 로딩 대기

            // Google Sheets에 성공(True) 기록
            GoogleSheetsUtil.updateCell("시트_링크_입력", "test!E2", "True"); // E열에 기록
        } catch (Exception e) {
            System.out.println("신규 서비스 버튼을 클릭하지 못 했습니다.");
            // Google Sheets에 실패(False) 기록
            GoogleSheetsUtil.updateCell("시트_링크_입력", "test!E2", "False"); // E열에 기록
        }

        // 웹 뷰 컨텍스트로 전환
        switchToWebViewContext();

        // E. 특정 카테고리 접근
        try {
            // 특정 카테고리 버튼 클릭
            clickElementByText("서비스_카테고리명");
            waitForPageLoad(); // 페이지 로딩 대기

            // Google Sheets에 성공(True) 기록
            GoogleSheetsUtil.updateCell("시트_링크_입력", "test!F2", "True"); // F열에 기록
        } catch (Exception e) {
            System.out.println("카테고리 버튼을 클릭하지 못 했습니다.");
            // Google Sheets에 실패(False) 기록
            GoogleSheetsUtil.updateCell("시트_링크_입력", "test!F2", "False"); // F열에 기록
        }

        waitForPageLoad(); // 다음 페이지 로딩 대기

        // 추가로 3초 대기
        Thread.sleep(3000);

        // F. 제품 데이터 수집
        try {
            // 제품 데이터를 수집합니다.
            collectProductData();

            // Google Sheets에 성공(True) 기록
            GoogleSheetsUtil.updateCell("시트_링크_입력", "test!G2", "True"); // G열에 기록
        } catch (Exception e) {
            System.out.println("데이터를 수집하지 못 했습니다.");
            // Google Sheets에 실패(False) 기록
            GoogleSheetsUtil.updateCell("시트_링크_입력", "test!G2", "False"); // G열에 기록
        }

        // 데이터 저장
        saveDataToExcel();
    }

    //화면 내 상품 정보 추출 및 전처리
    private void collectProductData() throws InterruptedException {
        switchToWebViewContext();
        WebDriverWait wait = new WebDriverWait(driver, 20);

        // 페이지 로딩 대기
        waitForPageLoad();

        WebElement totalProductsElement;
        try {
            totalProductsElement = wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//android.widget.TextView[contains(@text, '총')]")));
        } catch (Exception e) {
            System.out.println("전체 상품을 발견할 수 없습니다.");
            saveDataToExcel(); // 수집된 데이터를 저장하고 종료
            return;
        }

        int totalProducts = Integer.parseInt(totalProductsElement.getText().replaceAll("[^0-9]", ""));
        System.out.println("전체 상품 개수: " + totalProducts);
        int productsCollected = 0;

        while (productsCollected < totalProducts) {
            // 제품 요소 다시 가져오기
            List<AndroidElement> productElements = driver.findElements(By.xpath("//android.view.View[contains(@content-desc, '담기') or contains(@content-desc, '배민배달')]"));
            System.out.println("발견된 상품 개수: " + productElements.size());

            boolean newProductFound = false;

            for (AndroidElement productElement : productElements) {
                String contentDesc = productElement.getAttribute("content-desc");
                System.out.println("Product content description: " + contentDesc);

                // '담기' 또는 '배민배달' 제거
                contentDesc = contentDesc.replace("담기", "").replace("배민배달", "").trim();

                // 제품 정보 파싱
                String title = "";
                String price = "";
                boolean isBaeminMark = false;

                try {
                    // 가격 추출
                    String[] parts = contentDesc.split(" ");
                    for (String part : parts) {
                        if (part.contains("원")) {
                            price = part;
                            break;
                        }
                    }

                    // 가격을 제외한 부분을 제목으로 설정
                    title = contentDesc.split(price)[0].trim();

                    // 'baemin mark' 여부 확인
                    if (contentDesc.contains("baemin mark")) {
                        isBaeminMark = true;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    saveDataToExcel(); // 수집된 데이터를 저장하고 종료
                    return;
                }

                // 새로운 제품인 경우 리스트에 추가
                if (!isProductExists(title, price)) {
                    System.out.println("Title: " + title);
                    System.out.println("Price: " + price);
                    System.out.println("Baemin Mark: " + isBaeminMark);

                    productList.add(new Product(title, price, isBaeminMark, category));
                    productsCollected++;
                    newProductFound = true;

                    // 새로운 제품을 찾을 때마다 엑셀에 저장
                    saveDataToExcel();
                }

                if (productsCollected >= totalProducts) {
                    break;
                }
            }

            // 스크롤 다운
            if (!newProductFound) {
                scrollDown(); // 스크롤 동작
                Thread.sleep(3000); // 스크롤 후 3초 대기

                // 스크롤 후 제품 요소 다시 가져오기
                productElements = driver.findElements(By.xpath("//android.view.View[contains(@content-desc, '담기') or contains(@content-desc, '배민배달')]"));
                System.out.println("Number of product elements found after scroll: " + productElements.size());

                if (productElements.size() == 0) {
                    System.out.println("새로운 제품을 찾지 못했습니다. 다시 시도합니다.");
                    continue;
                }
            }

            // 스크롤 후 총 제품 수 다시 확인
            switchToWebViewContext(); // 웹뷰 컨텍스트로 전환
            try {
                totalProductsElement = wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//android.widget.TextView[contains(@text, '총')]")));
                totalProducts = Integer.parseInt(totalProductsElement.getText().replaceAll("[^0-9]", ""));
                System.out.println("스크롤 후 전체 상품 개수: " + totalProducts);
            } catch (Exception e) {
                System.out.println("스크롤 후 상품이 발견되지 않았습니다.");
                saveDataToExcel(); // 수집된 데이터를 저장하고 종료
                return;
            }
        }

        switchToNativeContext();
    }

    //화면 내 상품이 존재하는지 확인
    private boolean isProductExists(String title, String price) {
        for (Product product : productList) {
            if (product.getTitle().equals(title) && product.getPrice().equals(price)) {
                return true;
            }
        }
        return false;
    }

    //스크롤 내리는 행동
    private void scrollDown() {
        try {
            driver.findElementByAndroidUIAutomator("new UiScrollable(new UiSelector().scrollable(true)).scrollForward();");
        } catch (Exception e) {
            System.out.println("스크롤 실패: " + e.getMessage());
        }
    }

    //화면 내 팝업이 있는 경우 창 닫기 버튼 클릭
    private void dismissPopups() {
        WebDriverWait wait = new WebDriverWait(driver, 10);
        String[] popupIds = {"com.dbs.baemin.m2:id/okButton", "android:id/button1"};

        for (String popupId : popupIds) {
            try {
                WebElement popupButton = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id(popupId)));
                while (popupButton.isDisplayed()) {
                    popupButton.click();
                    popupButton = wait.until(ExpectedConditions.visibilityOfElementLocated(By.id(popupId)));
                }
            } catch (Exception e) {
                System.out.println("팝업이 없습니다.");
            }
        }
    }

    //어플리케이션 화면이 로딩되는 것을 대기
    private void waitForPageLoad() {
        WebDriverWait wait = new WebDriverWait(driver, 20);
        try {
            wait.until(ExpectedConditions.invisibilityOfElementLocated(By.id("com.dbs.baemin.m2:id/loading_indicator"))); // 로딩 인디케이터가 사라질 때까지 대기
        } catch (Exception e) {
            System.out.println("로딩 관련해서 발견되지 않았습니다.");
        }
    }

    //특정 엘리먼트를 n회 클릭 시도
    private void clickElementWithRetries(By locator, int retries) {
        WebDriverWait wait = new WebDriverWait(driver, 10);
        int attempts = 0;
        while (attempts < retries) {
            try {
                WebElement element = wait.until(ExpectedConditions.visibilityOfElementLocated(locator));
                element.click();
                return; // 클릭 성공 시 메서드 종료
            } catch (Exception e) {
                System.out.println("클릭 실패, 재시도: " + (attempts + 1));
                attempts++;
            }
        }
    }

    //엘리먼트의 텍스트 기반으로 클릭
    private void clickElementByText(String text) {
        WebDriverWait wait = new WebDriverWait(driver, 20);
        try {
            WebElement element = wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//android.widget.TextView[@text='" + text + "']")));
            element.click();
        } catch (Exception e) {
            System.out.println(text + " 요소를 찾을 수 없습니다.");
            saveDataToExcel(); // 수집된 데이터를 저장하고 종료
        }
    }

    //웹뷰인 경우 화면 전환
    private void switchToWebViewContext() {
        for (String contextName : driver.getContextHandles()) {
            if (contextName.contains("WEBVIEW")) {
                driver.context(contextName);
                break;
            }
        }
    }

    //네이티브로 화면 전환
    private void switchToNativeContext() {
        driver.context("NATIVE_APP");
    }

    //데이터 추출 후 결과 데이터를 엑셀 파일로 저장
    private void saveDataToExcel() {
        try (XSSFWorkbook workbook = new XSSFWorkbook(); FileOutputStream fileOut = new FileOutputStream("C:\\Users\\Woowahan\\ProductData.xlsx")) {
            Sheet sheet = workbook.createSheet("Product Data");
            int rowNum = 0;

            // 첫 번째 행에 칼럼명 추가
            Row headerRow = sheet.createRow(rowNum++);
            headerRow.createCell(0).setCellValue("title");
            headerRow.createCell(1).setCellValue("price");
            headerRow.createCell(2).setCellValue("baemin mark");
            headerRow.createCell(3).setCellValue("category");

            for (Product product : productList) {
                Row row = sheet.createRow(rowNum++);
                row.createCell(0).setCellValue(product.getTitle());
                row.createCell(1).setCellValue(product.getPrice());
                row.createCell(2).setCellValue(product.isBaeminMark() ? "Yes" : "No");
                row.createCell(3).setCellValue(product.getCategory());
            }
            workbook.write(fileOut);
            System.out.println("Data saved to Excel file successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    

    public class Product {
        private String title;
        private String price;
        private boolean baeminMark;
        private String category;

        public Product(String title, String price, boolean baeminMark, String category) {
            this.title = title;
            this.price = price;
            this.baeminMark = baeminMark;
            this.category = category;
        }

        public String getTitle() {
            return title;
        }

        public String getPrice() {
            return price;
        }

        public boolean isBaeminMark() {
            return baeminMark;
        }

        public String getCategory() {
            return category;
        }
    }

    @AfterClass
    public void end() throws Exception {
        System.out.println("테스트 종료 후 드라이버를 닫습니다.");
        driver.quit();
    }
}