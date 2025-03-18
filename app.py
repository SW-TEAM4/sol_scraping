import pymysql
from fastapi import FastAPI, HTTPException, Query
from scripts.finance_scraper import update_stock_data
from scripts.portfolio_scraper import evaluate_portfolio
from scripts.news_scraper import scrape_headlines, scrape_article
from fastapi.middleware.cors import CORSMiddleware
app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 실제 배포 시에는 특정 도메인만 허용하는 것이 좋습니다
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------- Stock 관련 API -------------------
@app.post("/update/{category}")
async def update_category(category: str):
    try:
        await update_stock_data(category=category)
        return {"message": f"{category} data updated successfully"}
    except Exception as e:
        return {"error": str(e)}

@app.post("/update_all")
async def update_all():
    try:
        await update_stock_data()
        return {"message": "All categories updated successfully"}
    except Exception as e:
        return {"error": str(e)}


# 카테고리별 주식 데이터
@app.get("/stocks/category/{category}")
async def get_stocks_by_category(category: str):
    """
    특정 카테고리의 주식 데이터를 반환합니다.
    """
    try:
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="2561",
            database="imsolo"
        )
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # 카테고리에 해당하는 주식 데이터 조회
        cursor.execute("""
            SELECT 
                ticker, 
                company_name as companyName, 
                category, 
                current_price as currentPrice, 
                yesterday_change as yesterdayChange,
                one_month_change as oneMonthChange, 
                three_month_change as threeMonthChange, 
                one_year_change as oneYearChange
            FROM category
            WHERE category = %s
        """, (category,))

        stocks = cursor.fetchall()
        connection.close()

        if not stocks:
            raise HTTPException(status_code=404, detail=f"Category '{category}' not found or no stocks available.")

        return stocks
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"주식 데이터를 가져오는 중 오류 발생: {str(e)}")


# ------------------- Portfolio 관련 API -------------------
@app.get("/portfolio/list")
async def evaluate_portfolio_api():
    """
    포트폴리오 데이터를 평가하고 결과를 JSON 형식으로 반환합니다.
    """
    try:
        result = await evaluate_portfolio()
        if not result:  # 결과가 비어 있는 경우 에러 반환
            raise HTTPException(status_code=404, detail="No portfolio data found.")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------- News 관련 API -------------------
@app.get("/news/headlines")
def get_headlines(limit: int = Query(default=10, description="가져올 기사 수 (기본값: 10)")):
    """매일경제 증권 최신 뉴스의 헤드라인과 링크를 반환합니다."""
    try:
        headlines = scrape_headlines(limit=limit)
        if not headlines:
            raise HTTPException(status_code=404, detail="헤드라인을 찾을 수 없습니다.")
        return {"headlines": headlines}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {str(e)}")

@app.get("/news/article")
def get_article(url: str = Query(default=..., description="기사를 가져올 URL")):
    """매일경제 특정 기사 URL에서 내용을 반환합니다."""
    try:
        article_data = scrape_article(url)
        if article_data["title"] == "오류 발생":
            raise HTTPException(status_code=404, detail="기사 내용을 찾을 수 없습니다.")
        return article_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"서버 내부 오류: {str(e)}")

# ------------------- Market Indices 관련 API -------------------
@app.get("/market/indices")
async def get_market_indices():
    try:
        import FinanceDataReader as fdr
        from datetime import datetime, timedelta
        import pandas as pd

        # 최근 10일간의 데이터를 가져와서 가장 최근 거래일 찾기
        end_date = datetime.now()
        start_date = end_date - timedelta(days=10)  # 10일 전 데이터부터 조회

        # 모든 지수 데이터 가져오기
        kospi_recent = fdr.DataReader('KS11', start_date, end_date)
        kosdaq_recent = fdr.DataReader('KQ11', start_date, end_date)
        dow_recent = fdr.DataReader('DJI', start_date, end_date)
        nasdaq_recent = fdr.DataReader('IXIC', start_date, end_date)
        sp500_recent = fdr.DataReader('US500', start_date, end_date)
        usd_krw_recent = fdr.DataReader('USD/KRW', start_date, end_date)

        if kospi_recent.empty:
            raise HTTPException(status_code=404, detail="최근 거래 데이터를 찾을 수 없습니다.")

        # 각 지수별 최근 거래일과 이전 거래일 찾기
        def get_index_data(df):
            if df.empty:
                return 0, 0, 0, ""

            latest = df.index[-1]
            latest_value = float(df['Close'].iloc[-1])

            if len(df) > 1:
                previous_value = float(df['Close'].iloc[-2])
                change = latest_value - previous_value
                change_pct = (change / previous_value) * 100 if previous_value != 0 else 0
            else:
                change = 0
                change_pct = 0

            latest_date_str = latest.strftime('%Y-%m-%d') if isinstance(latest, datetime) else str(latest).split(' ')[0]

            return latest_value, change, change_pct, latest_date_str

        # 각 지수 데이터 계산
        kospi_value, kospi_change, kospi_change_pct, kospi_date = get_index_data(kospi_recent)
        kosdaq_value, kosdaq_change, kosdaq_change_pct, kosdaq_date = get_index_data(kosdaq_recent)
        dow_value, dow_change, dow_change_pct, dow_date = get_index_data(dow_recent)
        nasdaq_value, nasdaq_change, nasdaq_change_pct, nasdaq_date = get_index_data(nasdaq_recent)
        sp500_value, sp500_change, sp500_change_pct, sp500_date = get_index_data(sp500_recent)
        usd_krw_value, usd_krw_change, usd_krw_change_pct, usd_krw_date = get_index_data(usd_krw_recent)

        return {
            'kospi': {
                'current': round(kospi_value, 2),
                'change': round(kospi_change, 2),
                'changePercent': round(kospi_change_pct, 2),
                'date': kospi_date
            },
            'kosdaq': {
                'current': round(kosdaq_value, 2),
                'change': round(kosdaq_change, 2),
                'changePercent': round(kosdaq_change_pct, 2),
                'date': kosdaq_date
            },
            'dow': {
                'current': round(dow_value, 2),
                'change': round(dow_change, 2),
                'changePercent': round(dow_change_pct, 2),
                'date': dow_date
            },
            'nasdaq': {
                'current': round(nasdaq_value, 2),
                'change': round(nasdaq_change, 2),
                'changePercent': round(nasdaq_change_pct, 2),
                'date': nasdaq_date
            },
            'sp500': {
                'current': round(sp500_value, 2),
                'change': round(sp500_change, 2),
                'changePercent': round(sp500_change_pct, 2),
                'date': sp500_date
            },
            'usdKrw': {
                'current': round(usd_krw_value, 2),
                'change': round(usd_krw_change, 2),
                'changePercent': round(usd_krw_change_pct, 2),
                'date': usd_krw_date
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(traceback.format_exc())  # 상세 오류 로그 출력
        raise HTTPException(status_code=500, detail=f"시장 지수 데이터를 가져오는 중 오류 발생: {str(e)}")
