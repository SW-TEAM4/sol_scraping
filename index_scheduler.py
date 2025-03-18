from datetime import datetime
import FinanceDataReader as fdr
import pymysql
import schedule
import time  # 수정: 잘못된 import 제거
from datetime import timedelta  # 수정: 잘못된 import 제거

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "2561",
    "database": "imsolo",
}

def fetch_market_indices():
    """
    FinanceDataReader를 사용하여 주요 지수 데이터를 가져와 MySQL에 저장.
    """
    try:
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

        # MySQL 연결
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        for name, df in indices.items():
            if not df.empty:
                latest = df.iloc[-1]
                previous = df.iloc[-2] if len(df) > 1 else latest

                current = round(latest["Close"], 2)
                change = round(latest["Close"] - previous["Close"], 2)
                change_percent = round((change / previous["Close"]) * 100, 2)
                date = latest.name.strftime("%Y-%m-%d %H:%M:%S")

                cursor.execute(
                    """
                    INSERT INTO stock_index (index_name, current, change_value, change_percent, date)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        current = VALUES(current),
                        change_value = VALUES(change_value),
                        change_percent = VALUES(change_percent),
                        date = VALUES(date)
                    """,
                    (name, current, change, change_percent, date),
                )

        connection.commit()
        cursor.close()
        connection.close()
        print(f"Market indices updated successfully at {datetime.now()}")

    except Exception as e:
        print(f"Error updating market indices: {str(e)}")


# 스케줄 설정
schedule.every(5).seconds.do(fetch_market_indices)  # 5초마다 업데이트 진행

if __name__ == "__main__":
    print("스케줄러가 시작되었습니다.")
    while True:
        try:
            schedule.run_pending()  # 예약된 작업 실행
            time.sleep(1)  # CPU 과부하 방지를 위해 1초 대기
            print(f"[{datetime.now()}] 스케줄러 대기 중...")  # 디버깅 로그 추가
        except Exception as e:
            print(f"[{datetime.now()}] 메인 루프에서 오류 발생: {e}")
