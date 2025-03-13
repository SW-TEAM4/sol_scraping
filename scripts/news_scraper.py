import requests
from bs4 import BeautifulSoup


def scrape_headlines(limit=10):
    """매일경제 증권 최신 뉴스에서 헤드라인과 링크를 스크래핑합니다."""
    url = "https://www.mk.co.kr/news/stock/latest/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        news_items = []
        # 매일경제 증권 최신 뉴스 섹션의 뉴스 항목 추출
        articles = soup.select('li.news_node')[:limit]

        for article in articles:
            try:
                # 링크 추출
                link_element = article.select_one('a.news_item')
                link = link_element['href'] if link_element else ""

                # 제목 추출
                title_element = article.select_one('h3.news_ttl')
                title = title_element.text.strip() if title_element else "제목 없음"

                # 이미지 URL 추출
                img_element = article.select_one('.thumb_area img')
                img_url = img_element['src'] if img_element and 'src' in img_element.attrs else ""

                # 요약 추출
                desc_element = article.select_one('.news_desc')
                summary = desc_element.text.strip() if desc_element else ""

                # 시간 정보 추출
                time_element = article.select_one('.time_info')
                time_info = time_element.text.strip() if time_element else ""

                # 날짜 추출
                date_element = article.select_one('.time_area span')
                date = date_element.text.strip().replace('\n', ' ') if date_element else ""

                news_items.append({
                    'title': title,
                    'link': link,
                    'img_url': img_url,
                    'summary': summary,
                    'time_info': time_info,
                    'date': date
                })
            except Exception as e:
                print(f"기사 처리 중 오류 발생: {e}")

        return news_items
    except Exception as e:
        print(f"헤드라인 스크래핑 중 오류 발생: {e}")
        return []


def scrape_article(url):
    """매일경제 특정 기사 URL에서 제목, 내용, 날짜를 스크래핑합니다."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # 기사 제목
        title_element = soup.select_one('.news_ttl')
        title = title_element.text.strip() if title_element else "제목을 찾을 수 없습니다."

        # 기사 내용
        content_element = soup.select_one('#article_body')
        content = content_element.text.strip() if content_element else "내용을 찾을 수 없습니다."

        # 작성 날짜
        date_element = soup.select_one('.time_info time')
        date = date_element.text.strip() if date_element else "날짜 정보 없음"

        # 기자 정보
        author_element = soup.select_one('.author_info .name')
        author = author_element.text.strip() if author_element else "작성자 정보 없음"

        return {
            "title": title,
            "content": content,
            "date": date,
            "author": author,
            "url": url
        }
    except Exception as e:
        print(f"기사 스크래핑 중 오류 발생: {e}")
        return {
            "title": "오류 발생",
            "content": f"기사를 가져오는 중 오류가 발생했습니다: {e}",
            "date": "",
            "author": "",
            "url": url
        }
