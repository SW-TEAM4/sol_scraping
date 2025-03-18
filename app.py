import pymysql
from fastapi import FastAPI, HTTPException, Query
from scripts.finance_scraper import update_stock_data
from scripts.portfolio_scraper import evaluate_portfolio
from scripts.news_scraper import scrape_headlines
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



# ------------------- Market Indices 관련 API -------------------
@app.get("/market/indices")
def get_market_indices():
    """
    Market indices 업데이트 상태 확인용 엔드포인트.
    """
    return {"message": "Market indices are being updated every 5 minutes."}