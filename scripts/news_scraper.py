import os
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import logging

# 로그 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# .env 파일 로드
load_dotenv("db.env")

# 환경 변수에서 데이터베이스 설정 가져오기
HOST = os.getenv('HOST')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
DATABASE = os.getenv('DATABASE')

# SQLAlchemy 엔진 생성
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}")

def create_news_table_if_not_exists():
    """news 테이블이 없으면 생성합니다."""
    with engine.connect() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS `news` (
              `id` bigint(20) NOT NULL AUTO_INCREMENT,
              `link` varchar(255) DEFAULT NULL,
              `publication_date` datetime(6) DEFAULT NULL,
              `summary` varchar(255) DEFAULT NULL,
              `time_info` varchar(255) DEFAULT NULL,
              `title` varchar(255) DEFAULT NULL,
              PRIMARY KEY (`id`)
            ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
        """))
    logger.info("news 테이블이 확인되었습니다.")

def scrape_headlines(limit=15):
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

        return news_items
    except Exception as e:
        logger.error(f"헤드라인 스크래핑 중 오류 발생: {e}")
        return []

def save_news_to_db(news_items):
    """스크래핑한 뉴스 데이터를 MySQL에 저장. title과 summary가 모두 동일한 경우 추가하지 않음."""
    with engine.connect() as connection:
        for item in news_items:
            # 동일한 title과 summary가 있는지 먼저 확인
            result = connection.execute(text(
                "SELECT 1 FROM news WHERE title = :title AND summary = :summary LIMIT 1"
            ), {"title": item["title"], "summary": item["summary"]})
            exists = result.fetchone() is not None

            # 동일한 title과 summary가 없는 경우에만 삽입
            if not exists:
                logger.info(f"저장할 데이터: {item['title']}")
                connection.execute(text("""
                    INSERT INTO news (title, summary, link, time_info, publication_date)
                    VALUES (:title, :summary, :link, :time_info, NOW())
                """), item)
            else:
                logger.info(f"중복된 뉴스 발견, 건너뜀: {item['title']}")
        connection.commit()

def main():
    """메인 함수: 뉴스를 스크래핑하고 MySQL에 저장."""
    logger.info(f"뉴스 스크래핑 시작...")
    create_news_table_if_not_exists()
    news_items = scrape_headlines(limit=20)
    save_news_to_db(news_items)
    logger.info(f"뉴스 스크래핑 완료.")

if __name__ == "__main__":
    main()
