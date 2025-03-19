from datetime import datetime
import pymysql
import requests
from bs4 import BeautifulSoup
import schedule
import time

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "2561",
    "database": "imsolo",
    "charset": "utf8mb4"
}

def save_news_to_db(news_items):
    """스크래핑한 뉴스 데이터를 MySQL에 저장."""
    connection = pymysql.connect(**DB_CONFIG)
    cursor = connection.cursor()

    for item in news_items:
        print("저장할 데이터:", item)  # 디버깅용 로그 추가
        cursor.execute(
            """
            INSERT INTO news (title, summary, link, time_info, publication_date)
            VALUES (%s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                summary = VALUES(summary),
                link = VALUES(link),
                time_info = VALUES(time_info),
                publication_date = NOW()
            """,
            (item["title"], item["summary"], item["link"], item["time_info"]),
        )

    connection.commit()
    cursor.close()
    connection.close()

def scrape_headlines(limit=10):
    """매일경제 증권 최신 뉴스에서 헤드라인과 링크를 스크래핑."""
    url = "https://www.mk.co.kr/news/stock/latest/"
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        news_items = []
        articles = soup.select('li.news_node')[:limit]

        for article in articles:
            link_element = article.select_one('a.news_item')
            link = link_element['href'] if link_element else ""

            title_element = article.select_one('h3.news_ttl')
            title = title_element.text.strip() if title_element else "제목 없음"

            desc_element = article.select_one('.news_desc')
            summary = desc_element.text.strip() if desc_element else ""

            time_element = article.select_one('.time_info')
            time_info = time_element.text.strip() if time_element else ""

            news_items.append({
                'title': title,
                'link': link,
                'summary': summary,
                'time_info': time_info
            })

        save_news_to_db(news_items)
        return news_items
    except Exception as e:
        print(f"헤드라인 스크래핑 중 오류 발생: {e}")
        return []

def fetch_and_store_news():
    """뉴스를 스크래핑하고 MySQL에 저장."""
    print(f"[{datetime.now()}] 뉴스 스크래핑 시작...")
    scrape_headlines(limit=10)
    print(f"[{datetime.now()}] 뉴스 스크래핑 완료.")

# 스케줄 설정: 1시간마다 실행
schedule.every(5).seconds.do(fetch_and_store_news)

if __name__ == "__main__":
    print("뉴스 스케줄러가 시작되었습니다.")
    fetch_and_store_news()  # 첫 실행 시 바로 한 번 실행
    while True:
        try:
            schedule.run_pending()  # 예약된 작업 실행
            time.sleep(1)  # CPU 과부하 방지를 위해 1초 대기
        except Exception as e:
            print(f"[{datetime.now()}] 메인 루프에서 오류 발생: {e}")
