import asyncio
import aiohttp
from bs4 import BeautifulSoup
from pykrx import stock
from datetime import datetime, timedelta
import pymysql

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

async def update_stock_data(category=None):
    """특정 카테고리 또는 모든 데이터를 업데이트하고 DB에 저장합니다."""
    try:
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="2561",
            database="imsolo"
        )
        cursor = connection.cursor()

        async with aiohttp.ClientSession() as session:
            # 특정 카테고리가 주어진 경우 해당 카테고리만 처리
            categories = {category: category_tickers[category]} if category else category_tickers

            for category, tickers in categories.items():
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

                        # 기존 데이터 업데이트
                        cursor.execute("""
                            UPDATE stock_data
                            SET company_name = %s,
                                category = %s,
                                current_price = %s,
                                yesterday_change = %s,
                                one_month_change = %s,
                                three_month_change = %s,
                                one_year_change = %s,
                                last_updated = NOW()
                            WHERE ticker = %s
                        """, (
                            google_data.get("companyName"),
                            category,
                            google_data.get("currentPrice"),
                            pykrx_data.get("yesterdayChange"),
                            pykrx_data.get("oneMonthChange"),
                            pykrx_data.get("threeMonthChange"),
                            pykrx_data.get("oneYearChange"),
                            ticker
                        ))

                        # 데이터가 없으면 새로 삽입
                        if cursor.rowcount == 0:  # 업데이트된 행이 없으면 삽입
                            print(f"[Info] No existing data for ticker {ticker}, inserting new record.")
                            cursor.execute("""
                                INSERT INTO stock_data (ticker, company_name, category, current_price, yesterday_change,
                                                        one_month_change, three_month_change, one_year_change, last_updated)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                            """, (
                                ticker,
                                google_data.get("companyName"),
                                category,
                                google_data.get("currentPrice"),
                                pykrx_data.get("yesterdayChange"),
                                pykrx_data.get("oneMonthChange"),
                                pykrx_data.get("threeMonthChange"),
                                pykrx_data.get("oneYearChange")
                            ))

                        print(f"[Success] Updated or Inserted ticker: {ticker}")

                    except Exception as e:
                        print(f"[Error] Failed to process ticker {ticker}: {e}")

            # 커밋은 루프 외부에서 한 번만 실행
            connection.commit()
        connection.close()
    except Exception as e:
        print(f"[Error] Database operation failed: {e}")


if __name__ == "__main__":
    asyncio.run(update_stock_data())
