import schedule
import time
from datetime import datetime
import requests  # FastAPI 엔드포인트 호출을 위한 라이브러리

FASTAPI_BASE_URL = "http://localhost:8000"  # FastAPI 서버 주소

def is_holiday_or_weekend():
    """공휴일 또는 주말 여부를 확인"""
    today = datetime.today()
    holidays = [
        datetime(2025, 3, 1),  # 삼일절
        datetime(2025, 5, 5),  # 어린이날
        datetime(2025, 8, 15), # 광복절
        datetime(2025, 10, 3), # 개천절
        datetime(2025, 12, 25) # 크리스마스
    ]
    is_weekend = today.weekday() >= 5
    is_holiday = today.date() in [holiday.date() for holiday in holidays]

    print(f"[{datetime.now()}] 오늘은 주말인가요? {is_weekend}, 공휴일인가요? {is_holiday}")

    return is_weekend or is_holiday

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

def scheduled_task():
    """정규장 시간 동안만 데이터를 업데이트"""
    try:
        print(f"[{datetime.now()}] 스케줄러 작업 시작.")  # 디버깅 로그 추가

        current_time = datetime.now().time()
        market_open = datetime.strptime("09:00", "%H:%M").time()
        market_close = datetime.strptime("15:30", "%H:%M").time()

        # 디버깅 로그 추가
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


# 스케줄 설정
schedule.every(5).seconds.do(scheduled_task) #5초마다 업데이트 진행

if __name__ == "__main__":
    print("스케줄러가 시작되었습니다.")
    while True:
        try:
            schedule.run_pending()  # 예약된 작업 실행
            time.sleep(1)  # CPU 과부하 방지를 위해 1초 대기
            print(f"[{datetime.now()}] 스케줄러 대기 중...")  # 디버깅 로그 추가
        except Exception as e:
            print(f"[{datetime.now()}] 메인 루프에서 오류 발생: {e}")

