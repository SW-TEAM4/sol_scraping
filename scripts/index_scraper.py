import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import FinanceDataReader as fdr

# .env 파일 로드
load_dotenv("db.env")

# 환경 변수에서 데이터베이스 설정 가져오기
HOST = os.getenv('HOST')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
DATABASE = os.getenv('DATABASE')

# SQLAlchemy 엔진 생성
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}")

def create_stock_index_table_if_not_exists():
    """stock_index 테이블이 없으면 생성합니다."""
    with engine.connect() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS `stock_index` (
              `id` bigint(20) NOT NULL AUTO_INCREMENT,
              `index_name` varchar(255) NOT NULL,
              `current` decimal(38,2) DEFAULT NULL,
              `change_value` decimal(38,2) DEFAULT NULL,
              `change_percent` decimal(38,2) DEFAULT NULL,
              `date` datetime NOT NULL,
              PRIMARY KEY (`id`),
              UNIQUE KEY `index_name` (`index_name`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
        """))
    print("stock_index 테이블이 확인되었습니다.")


def fetch_market_indices():
    """
    FinanceDataReader를 사용하여 주요 지수 데이터를 가져와 MySQL에 저장.
    """
    try:
        # 테이블 존재 여부 확인 및 생성
        create_stock_index_table_if_not_exists()

        end_date = datetime.now()
        start_date = end_date - timedelta(days=10)  # 최근 10일간 데이터 조회

        # 주요 지수 데이터 가져오기
        indices = {
            "코스피": fdr.DataReader("KS11", start_date, end_date),
            "코스닥": fdr.DataReader("KQ11", start_date, end_date),
            "나스닥 종합": fdr.DataReader("IXIC", start_date, end_date),
            "S&P 500": fdr.DataReader("US500", start_date, end_date),
            "다우존스": fdr.DataReader("DJI", start_date, end_date),
            "미국 USD": fdr.DataReader("USD/KRW", start_date, end_date),
        }

        # SQLAlchemy로 데이터 처리
        with engine.connect() as connection:
            for name, df in indices.items():
                if not df.empty:
                    latest = df.iloc[-1]
                    previous = df.iloc[-2] if len(df) > 1 else latest

                    current = round(latest["Close"], 2)
                    change = round(latest["Close"] - previous["Close"], 2)
                    change_percent = round((change / previous["Close"]) * 100, 2)
                    date = latest.name.strftime("%Y-%m-%d %H:%M:%S")

                    # 먼저 데이터가 존재하는지 확인
                    result = connection.execute(text(
                        "SELECT 1 FROM stock_index WHERE index_name = :name"
                    ), {"name": name})
                    exists = result.fetchone() is not None

                    if exists:
                        # 데이터가 존재하면 업데이트
                        connection.execute(text("""
                            UPDATE stock_index
                            SET current = :current,
                                change_value = :change_value,
                                change_percent = :change_percent,
                                date = :date
                            WHERE index_name = :name
                        """), {
                            "current": current,
                            "change_value": change,
                            "change_percent": change_percent,
                            "date": date,
                            "name": name
                        })
                        print(f"[Success] Updated index: {name}")
                    else:
                        # 데이터가 없으면 삽입
                        connection.execute(text("""
                            INSERT INTO stock_index (index_name, current, change_value, change_percent, date)
                            VALUES (:name, :current, :change_value, :change_percent, :date)
                        """), {
                            "name": name,
                            "current": current,
                            "change_value": change,
                            "change_percent": change_percent,
                            "date": date
                        })
                        print(f"[Success] Inserted new index: {name}")

            connection.commit()
        print(f"Market indices updated successfully at {datetime.now()}")

    except Exception as e:
        print(f"Error updating market indices: {str(e)}")

if __name__ == "__main__":
    print(f"[{datetime.now()}] 지수 데이터 업데이트 시작...")
    fetch_market_indices()
    print(f"[{datetime.now()}] 지수 데이터 업데이트 완료.")
