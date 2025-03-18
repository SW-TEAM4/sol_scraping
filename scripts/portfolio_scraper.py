import pymysql
import requests
from bs4 import BeautifulSoup

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "2561",
    "database": "imsolo",
}

def fetch_closing_price(ticker):
    """
    네이버 금융에서 전일 종가 데이터를 스크래핑합니다.
    """
    try:
        url = f"https://finance.naver.com/item/main.nhn?code={ticker}"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        closing_price_element = soup.select_one(".no_today .blind")
        if closing_price_element:
            return float(closing_price_element.text.replace(",", ""))
        else:
            print(f"Warning: No closing price found for ticker {ticker}")
            return None
    except Exception as e:
        print(f"Error fetching closing price for ticker {ticker}: {e}")
        return None

def update_portfolio_data(data):
    """
    데이터베이스에 평가 금액 및 손익 데이터를 업데이트합니다.
    """
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor() as cursor:
            for item in data:
                query = """
                UPDATE portfolio 
                SET closing_price = %s,
                    evaluation_amount = %s,
                    profit_loss = %s,
                    profit_loss_rate = %s
                WHERE ticker = %s
                """
                cursor.execute(query, (
                    item["closingPrice"],
                    item["evaluationAmount"],
                    item["profitLoss"],
                    item["profitLossRate"],
                    item["ticker"]
                ))
            connection.commit()
    except Exception as e:
        print(f"Error updating portfolio data: {e}")
    finally:
        connection.close()


async def evaluate_portfolio():
    connection = pymysql.connect(**DB_CONFIG)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # 모든 필드를 명시적으로 가져오도록 수정
            query = """
            SELECT 
                id, 
                krw_balance, 
                stock_name, 
                ticker, 
                stock_quantity, 
                average_purchase_price,
                purchase_amount
            FROM portfolio
            """
            cursor.execute(query)
            portfolio_data = cursor.fetchall()

            # 디버깅을 위해 가져온 데이터 출력
            print("데이터베이스에서 가져온 데이터:", portfolio_data)

        # 평가 금액 및 손익 계산
        for item in portfolio_data:
            ticker = item["ticker"]
            try:
                closing_price = fetch_closing_price(ticker)
                if closing_price is not None:
                    item["closingPrice"] = closing_price
                    item["evaluationAmount"] = closing_price * item["stock_quantity"]
                    item["profitLoss"] = item["evaluationAmount"] - item["purchase_amount"]
                    item["profitLossRate"] = (item["profitLoss"] / item["purchase_amount"]) * 100

                    # 필드명 변환 (snake_case를 camelCase로)
                    item["krwBalance"] = item.pop("krw_balance")
                    item["stockName"] = item.pop("stock_name")
                    item["stockQuantity"] = item.pop("stock_quantity")
                    item["averagePurchasePrice"] = item.pop("average_purchase_price")
                    item["purchaseAmount"] = item.pop("purchase_amount")
            except Exception as e:
                print(f"Error processing ticker {ticker}: {e}")

        return portfolio_data
    except Exception as e:
        print(f"Error in evaluate_portfolio: {e}")
        return {"error": str(e)}


