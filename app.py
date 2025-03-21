import pymysql
from fastapi import FastAPI, HTTPException, Query
from scripts.finance_scraper import update_stock_data
from fastapi.middleware.cors import CORSMiddleware

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "2561",
    "database": "imsolo",
    "charset": "utf8mb4"
}
app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 실제 배포 시에는 특정 도메인만 허용하는 것이 좋습니다
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------- Category 관련 API -------------------
@app.post("/update_all")
async def update_all():
    """
    모든 카테고리 데이터를 업데이트합니다.
    """
    try:
        await update_stock_data()  # 모든 카테고리 업데이트
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

# ------------------- News 관련 API -------------------
@app.get("/news/headlines")
def get_headlines(limit: int = Query(default=10, description="가져올 기사 수 (기본값: 10)")):
    """MySQL에서 뉴스 데이터를 가져옵니다."""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        cursor.execute("SELECT * FROM news ORDER BY publication_date DESC LIMIT %s", (limit,))
        headlines = cursor.fetchall()

        print("MySQL에서 가져온 뉴스 데이터:", headlines)  # 디버깅용 로그 추가

        cursor.close()
        connection.close()

        if not headlines:
            raise HTTPException(status_code=404, detail="헤드라인을 찾을 수 없습니다.")

        # 빈 값 처리: 빈 문자열 대신 기본값 설정
        for headline in headlines:
            if not headline["summary"]:
                headline["summary"] = "요약 정보 없음"
            if not headline["link"]:
                headline["link"] = "#"
            if not headline["title"]:
                headline["title"] = "제목 없음"

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