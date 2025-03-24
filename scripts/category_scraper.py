import os
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from pykrx import stock
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import requests

# .env 파일 로드
load_dotenv("db.env")

# 환경 변수에서 데이터베이스 설정 가져오기
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '2561')
DB_NAME = os.getenv('DB_NAME', 'imsolo')

# SQLAlchemy 엔진 생성
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# FastAPI 서버 주소
FASTAPI_BASE_URL = "http://localhost:8000"

# 카테고리별 티커 매핑
category_tickers = {
    "bank": ["323410", "055550", "138930", "086790", "105560", "024110"],
    "bio": ["068270", "207940"],
    "semiconductor": ["005930", "000660"],
    "food": ["003230", "007310", "271560", "097950"],
    "video": ["352820"],
    "car": ["005380", "000270", "012330"],
    "beauty": ["090430", "003350"],
    "travel": ["020560", "003490"],
    "shipbuilding": ["010140", "009540"]
}


def is_holiday_or_weekend():
    """공휴일 또는 주말 여부를 확인"""
    today = datetime.today()
    holidays = [
        datetime(2025, 3, 1),  # 삼일절
        datetime(2025, 5, 5),  # 어린이날
        datetime(2025, 8, 15),  # 광복절
        datetime(2025, 10, 3),  # 개천절
        datetime(2025, 12, 25)  # 크리스마스
    ]
    is_weekend = today.weekday() >= 5
    is_holiday = today.date() in [holiday.date() for holiday in holidays]

    print(f"[{datetime.now()}] 오늘은 주말인가요? {is_weekend}, 공휴일인가요? {is_holiday}")

    return is_weekend or is_holiday


def get_pykrx_data(ticker):
    """PyKRX를 사용하여 과거 데이터를 가져옵니다."""
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

        df = stock.get_market_ohlcv(start_date, end_date, ticker)
        if df.empty:
            print(f"[Error] No data available for {ticker} in PyKRX.")
            return {}

        current_price = df.iloc[-1]['종가']
        yesterday_price = df.iloc[-2]['종가'] if len(df) > 1 else None
        one_month_price = df.iloc[-21]['종가'] if len(df) > 21 else None
        three_month_price = df.iloc[-63]['종가'] if len(df) > 63 else None
        one_year_price = df.iloc[0]['종가'] if len(df) > 0 else None

        def calculate_change(current, past):
            if past is None or current is None:
                return None
            return round(((current - past) / past) * 100, 2)

        return {
            "currentPrice": current_price,
            "yesterdayChange": calculate_change(current_price, yesterday_price),
            "oneMonthChange": calculate_change(current_price, one_month_price),
            "threeMonthChange": calculate_change(current_price, three_month_price),
            "oneYearChange": calculate_change(current_price, one_year_price),
        }
    except Exception as e:
        print(f"[Error] PyKRX for {ticker}: {e}")
        return {}


async def fetch_google_finance_data(session, ticker):
    """Google Finance에서 현재가와 회사명을 비동기로 가져옵니다."""
    url = f"https://www.google.com/finance/quote/{ticker}:KRX"
    headers = {"User-Agent": "Mozilla/5.0"}
    async with session.get(url, headers=headers) as response:
        html = await response.text()
        soup = BeautifulSoup(html, 'html.parser')
        try:
            current_price_element = soup.select_one(".YMlKec.fxKbKc")
            company_name_element = soup.select_one(".zzDege")

            if not current_price_element or not company_name_element:
                raise ValueError("Required elements not found on the page")

            current_price = float(current_price_element.text.strip().replace(',', '').replace('₩', ''))
            company_name = company_name_element.text.strip()

            return {"currentPrice": current_price, "companyName": company_name}
        except Exception as e:
            print(f"[Error] Google Finance for {ticker}: {e}")
            return {"currentPrice": None, "companyName": None}


async def update_stock_data():
    """모든 카테고리 데이터를 업데이트하고 DB에 저장합니다."""
    try:
        # category 테이블 존재 여부 확인 및 생성
        with engine.connect() as connection:
            # 테이블 존재 여부 확인
            result = connection.execute(text("SHOW TABLES LIKE 'category'"))
            table_exists = result.fetchone() is not None

            # 테이블이 없으면 생성
            if not table_exists:
                print("category 테이블이 존재하지 않습니다. 테이블을 생성합니다...")
                connection.execute(text("""
                    CREATE TABLE `category` (
                      `id` bigint(20) NOT NULL AUTO_INCREMENT,
                      `category` varchar(255) NOT NULL,
                      `company_name` varchar(255) NOT NULL,
                      `current_price` double DEFAULT NULL,
                      `one_month_change` double DEFAULT NULL,
                      `one_year_change` double DEFAULT NULL,
                      `three_month_change` double DEFAULT NULL,
                      `ticker` varchar(255) DEFAULT NULL,
                      `yesterday_change` double DEFAULT NULL,
                      `last_updated` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
                      PRIMARY KEY (`id`),
                      UNIQUE KEY `ticker` (`ticker`)
                    ) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
                """))
                connection.commit()
                print("category 테이블이 성공적으로 생성되었습니다.")

        async with aiohttp.ClientSession() as session:
            # 모든 카테고리를 순차적으로 처리
            for category, tickers in category_tickers.items():
                for ticker in tickers:
                    try:
                        print(f"Processing ticker: {ticker} ({category})")

                        # Google Finance와 PyKRX 데이터를 결합
                        google_data = await fetch_google_finance_data(session, ticker)
                        pykrx_data = get_pykrx_data(ticker)

                        # 데이터가 없으면 스킵
                        if not google_data or not pykrx_data:
                            print(f"[Warning] Skipping ticker: {ticker}")
                            continue

                        with engine.connect() as connection:
                            # 기존 데이터 업데이트
                            result = connection.execute(text("""
                                UPDATE category
                                SET company_name = :company_name,
                                    category = :category,
                                    current_price = :current_price,
                                    yesterday_change = :yesterday_change,
                                    one_month_change = :one_month_change,
                                    three_month_change = :three_month_change,
                                    one_year_change = :one_year_change,
                                    last_updated = NOW()
                                WHERE ticker = :ticker
                            """), {
                                "company_name": google_data.get("companyName"),
                                "category": category,
                                "current_price": google_data.get("currentPrice"),
                                "yesterday_change": pykrx_data.get("yesterdayChange"),
                                "one_month_change": pykrx_data.get("oneMonthChange"),
                                "three_month_change": pykrx_data.get("threeMonthChange"),
                                "one_year_change": pykrx_data.get("oneYearChange"),
                                "ticker": ticker
                            })

                            # 데이터가 없으면 새로 삽입
                            if result.rowcount == 0:  # 업데이트된 행이 없으면 삽입
                                print(f"[Info] No existing data for ticker {ticker}, inserting new record.")
                                connection.execute(text("""
                                    INSERT INTO category (ticker, company_name, category, current_price, yesterday_change,
                                                         one_month_change, three_month_change, one_year_change, last_updated)
                                    VALUES (:ticker, :company_name, :category, :current_price, :yesterday_change,
                                           :one_month_change, :three_month_change, :one_year_change, NOW())
                                """), {
                                    "ticker": ticker,
                                    "company_name": google_data.get("companyName"),
                                    "category": category,
                                    "current_price": google_data.get("currentPrice"),
                                    "yesterday_change": pykrx_data.get("yesterdayChange"),
                                    "one_month_change": pykrx_data.get("oneMonthChange"),
                                    "three_month_change": pykrx_data.get("threeMonthChange"),
                                    "one_year_change": pykrx_data.get("oneYearChange")
                                })

                            connection.commit()
                            print(f"[Success] Updated or Inserted ticker: {ticker}")

                    except Exception as e:
                        print(f"[Error] Failed to process ticker {ticker}: {e}")

    except Exception as e:
        print(f"[Error] Database operation failed: {e}")


def update_all_categories(update_type="regular"):
    """FastAPI의 /update_all 엔드포인트 호출"""
    try:
        response = requests.post(f"{FASTAPI_BASE_URL}/update_all", json={"update_type": update_type})
        if response.status_code == 200:
            print(f"[{datetime.now()}] 모든 카테고리 데이터가 성공적으로 업데이트되었습니다. (업데이트 유형: {update_type})")
        else:
            print(f"[{datetime.now()}] 업데이트 실패: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"[{datetime.now()}] FastAPI 호출 중 오류 발생: {e}")
        # FastAPI 호출 실패 시 직접 데이터 업데이트 시도
        print(f"[{datetime.now()}] 직접 데이터 업데이트를 시도합니다.")
        asyncio.run(update_stock_data())


def scheduled_task():
    """정규장 시간 동안만 데이터를 업데이트"""
    try:
        print(f"[{datetime.now()}] 스케줄러 작업 시작.")

        current_time = datetime.now().time()
        market_open = datetime.strptime("09:00", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()

        print(f"[{datetime.now()}] 현재 시간: {current_time}, 정규장 시작 시간: {market_open}, 정규장 종료 시간: {market_close}")

        if market_open <= current_time <= market_close:
            print(f"[{datetime.now()}] 정규장 시간입니다. 데이터를 업데이트합니다.")
            update_all_categories(update_type="regular")  # 정규장 시간에는 실시간 데이터 업데이트
        elif current_time > market_close:
            print(f"[{datetime.now()}] 정규장 시간이 아닙니다. 종가기준으로 데이터를 업데이트합니다.")
            update_all_categories(update_type="closing")  # 정규장 종료 후 종가기준으로 업데이트
        elif current_time < market_open:
            print(f"[{datetime.now()}] 오전 9시 이전입니다. 전일 종가기준으로 데이터를 업데이트합니다.")
            update_all_categories(update_type="previous_closing")  # 오전 9시 이전에는 전일 종가기준으로 업데이트

    except Exception as e:
        print(f"[{datetime.now()}] 스케줄러 작업 중 오류 발생: {e}")


def main():
    """메인 함수: 카테고리 데이터를 업데이트합니다."""
    print(f"[{datetime.now()}] 카테고리 데이터 업데이트 시작...")

    if is_holiday_or_weekend():
        print(f"[{datetime.now()}] 오늘은 주말이나 공휴일입니다. 이전 영업일 데이터 사용.")
        # 이전 영업일 데이터로 업데이트하는 코드 추가
    else:
        scheduled_task()  # 카테고리 데이터 업데이트

    print(f"[{datetime.now()}] 카테고리 데이터 업데이트 완료.")


if __name__ == "__main__":
    print(f"[{datetime.now()}] 카테고리 스케줄러 실행...")
    main()

