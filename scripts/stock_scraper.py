import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pykrx import stock

# .env 파일 로드
load_dotenv("db.env")

# 환경 변수
host = os.getenv('HOST')
user = os.getenv('USER')
password = os.getenv('PASSWORD')
database = os.getenv('DATABASE')

# 조회 티커 목록
tickers = [
    ("삼성전자", "005930"),
    ("SK하이닉스", "000660"),
    ("한화시스템", "272210"),
    ("NAVER", "035420"),
    ("현대로템", "064350"),
    ("풍산", "103140"),
    ("현대차", "005380"),
    ("기아", "000270"),
    ("현대모비스", "012330"),
    ("아모레퍼시픽", "090430"),
    ("대한항공", "003490"),
    ("삼성중공업", "010140"),
    ("카카오뱅크", "323410"),
    ("신한지주", "055550"),
    ("KB금융", "105560"),
    ("셀트리온", "068270"),
    ("삼성바이오로직스", "207940"),
    ("삼양식품", "003230"),
    ("CJ제일제당", "097950"),
    ("하이브", "352820"),
]


def get_previous_trading_day(today):
    """주말이나 공휴일인 경우 이전 거래일을 반환합니다."""
    holidays = [
        datetime(2025, 3, 1),  # 삼일절
        datetime(2025, 5, 5),  # 어린이날
        datetime(2025, 8, 15),  # 광복절
        datetime(2025, 10, 3),  # 개천절
        datetime(2025, 12, 25)  # 크리스마스
    ]

    # 오늘이 주말이거나 공휴일인 경우 이전 날짜로 이동
    while today.weekday() >= 5 or today.date() in [holiday.date() for holiday in holidays]:
        today -= timedelta(days=1)

    return today.strftime("%Y%m%d")


def get_stock_data(tickers):
    stock_data = {}
    today = datetime.now()
    end_day = get_previous_trading_day(today)

    for name, ticker in tickers:
        try:
            # 2000년 1월 1일부터 현재까지의 데이터 가져오기
            start_day = "20000101"
            df = stock.get_market_ohlcv(start_day, end_day, ticker)

            if df is not None and not df.empty:
                stock_data[ticker] = df
                print(f"{name}({ticker}) 데이터 가져오기 성공: {len(df)}일치")
            else:
                print(f"{name}({ticker}) 데이터 없음")
        except Exception as e:
            print(f"{name}({ticker}) 데이터 가져오기 실패: {e}")

    return stock_data


def save_to_db():
    """주식 데이터를 데이터베이스에 저장합니다."""
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

    ticker_dict = {ticker: company for company, ticker in tickers}

    # livestock 테이블이 없으면 생성
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS livestock (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE NOT NULL,
                start_price DECIMAL(10, 2) NOT NULL,
                high_price DECIMAL(10, 2) NOT NULL,
                low_price DECIMAL(10, 2) NOT NULL,
                end_price DECIMAL(10, 2) NOT NULL,
                volume BIGINT NOT NULL,
                ticker VARCHAR(10) NOT NULL,
                ticker_name VARCHAR(50) NOT NULL,
                diff_rate DECIMAL(10, 2) NOT NULL,
                UNIQUE KEY (date, ticker)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
        """))

    stock_data = get_stock_data(tickers)

    with engine.connect() as conn:
        for ticker, df in stock_data.items():
            df = df.rename(columns={
                "시가": "start_price",
                "고가": "high_price",
                "저가": "low_price",
                "종가": "end_price",
                "거래량": "volume",
                "등락률": "diff_rate",
            })
            df["ticker"] = ticker
            df["ticker_name"] = ticker_dict.get(ticker)
            df["diff_rate"] = df["diff_rate"].apply(lambda x: 0 if pd.isna(x) else round(x, 2))
            df.reset_index(inplace=True)
            df = df.rename(columns={"날짜": "date"})

            # 각 행을 개별적으로 INSERT IGNORE
            for _, row in df.iterrows():
                query = text("""
                    INSERT IGNORE INTO livestock (date, start_price, high_price, low_price, end_price, volume, ticker, ticker_name, diff_rate)
                    VALUES (:date, :start_price, :high_price, :low_price, :end_price, :volume, :ticker, :ticker_name, :diff_rate)
                """)
                conn.execute(query, row.to_dict())

            conn.commit()  # 변경사항 저장
            print(f"save success: {ticker}")


if __name__ == "__main__":
    save_to_db()
