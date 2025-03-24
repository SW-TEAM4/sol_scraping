import os
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from scripts.category_scraper import update_stock_data

# .env 파일 로드
load_dotenv("db.env")

# 환경 변수에서 데이터베이스 설정 가져오기
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', '2561')
DB_NAME = os.getenv('DB_NAME', 'imsolo')

# SQLAlchemy 설정
DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 실제 배포 시에는 특정 도메인만 허용하는 것이 좋습니다
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 의존성 주입을 위한 함수
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


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
async def get_stocks_by_category(category: str, db=Depends(get_db)):
    """
    특정 카테고리의 주식 데이터를 반환합니다.
    """
    try:
        query = text("""
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
            WHERE category = :category
        """)

        result = db.execute(query, {"category": category})
        stocks = [dict(row) for row in result]

        if not stocks:
            raise HTTPException(status_code=404, detail=f"Category '{category}' not found or no stocks available.")

        return stocks
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"주식 데이터를 가져오는 중 오류 발생: {str(e)}")


# ------------------- News 관련 API -------------------
@app.get("/news/headlines")
def get_headlines(limit: int = Query(default=10, description="가져올 기사 수 (기본값: 10)"), db=Depends(get_db)):
    """MySQL에서 뉴스 데이터를 가져옵니다."""
    try:
        query = text("SELECT * FROM news ORDER BY publication_date DESC LIMIT :limit")
        result = db.execute(query, {"limit": limit})
        headlines = [dict(row) for row in result]

        print("MySQL에서 가져온 뉴스 데이터:", headlines)  # 디버깅용 로그 추가

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
def get_market_indices(db=Depends(get_db)):
    """
    Market indices 데이터를 가져옵니다.
    """
    try:
        query = text("SELECT * FROM stock_index ORDER BY index_name")
        result = db.execute(query)
        indices = [dict(row) for row in result]

        if not indices:
            return {"message": "No market indices data available."}

        return {"indices": indices}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"시장 지수 데이터를 가져오는 중 오류 발생: {str(e)}")


# ------------------- Livestock(주식) 관련 API -------------------
@app.get("/stocks/livestock")
def get_livestock_data(ticker: str = None, date: str = None, db=Depends(get_db)):
    """
    주식 데이터를 가져옵니다. 티커와 날짜로 필터링할 수 있습니다.
    """
    try:
        query_parts = ["SELECT * FROM livestock"]
        params = {}

        # 필터 조건 추가
        where_clauses = []
        if ticker:
            where_clauses.append("ticker = :ticker")
            params["ticker"] = ticker

        if date:
            where_clauses.append("date = :date")
            params["date"] = date

        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))

        # 정렬 추가
        query_parts.append("ORDER BY date DESC, ticker")

        # 쿼리 실행
        query = text(" ".join(query_parts))
        result = db.execute(query, params)
        stocks = [dict(row) for row in result]

        if not stocks:
            return {"message": "No stock data available with the given filters."}

        return {"stocks": stocks}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"주식 데이터를 가져오는 중 오류 발생: {str(e)}")


@app.get("/stocks/livestock/latest")
def get_latest_livestock_data(db=Depends(get_db)):
    """
    가장 최근 날짜의 모든 주식 데이터를 가져옵니다.
    """
    try:
        # 가장 최근 날짜 조회
        latest_date_query = text("SELECT MAX(date) as latest_date FROM livestock")
        latest_date_result = db.execute(latest_date_query).fetchone()

        if not latest_date_result or not latest_date_result['latest_date']:
            return {"message": "No stock data available."}

        latest_date = latest_date_result['latest_date']

        # 최근 날짜의 모든 주식 데이터 조회
        query = text("SELECT * FROM livestock WHERE date = :date ORDER BY ticker")
        result = db.execute(query, {"date": latest_date})
        stocks = [dict(row) for row in result]

        return {
            "date": latest_date.strftime("%Y-%m-%d") if hasattr(latest_date, 'strftime') else latest_date,
            "stocks": stocks
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"주식 데이터를 가져오는 중 오류 발생: {str(e)}")


@app.get("/stocks/livestock/tickers")
def get_livestock_tickers(db=Depends(get_db)):
    """
    사용 가능한 모든 주식 티커 목록을 가져옵니다.
    """
    try:
        query = text("SELECT DISTINCT ticker, ticker_name FROM livestock ORDER BY ticker_name")
        result = db.execute(query)
        tickers = [dict(row) for row in result]

        if not tickers:
            return {"message": "No tickers available."}

        return {"tickers": tickers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"티커 목록을 가져오는 중 오류 발생: {str(e)}")

