import yfinance as yf
import FinanceDataReader as fdr
import pandas as pd
import requests
import datetime
import os
import warnings

# 경고 메시지 숨김
warnings.filterwarnings('ignore')

# --- 텔레그램 설정 ---
# 발급받은 API 토큰과 Chat ID를 입력하세요.
# (GitHub Actions 사용 시 코드에 직접 적지 않고 Secrets에 등록하는 것이 안전합니다.)
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '8383235460:AAFAdBAFy5dUQE1wqkShqF3X8T9FbIaUJQc')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '1729501017')


def send_telegram_message(message):
    """텔레그램으로 메시지를 전송하는 함수"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"텔레그램 전송 실패: {e}")


def check_moving_averages(ticker, market='KR'):
    """주봉 10, 20, 60 정배열 확인 함수"""
    try:
        # 60주선 계산을 위해 넉넉하게 약 2년(730일)치 일봉 데이터 호출
        start_date = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime('%Y-%m-%d')

        if market == 'KR':
            df = fdr.DataReader(ticker, start_date)
        else:
            df = yf.download(ticker, start=start_date, progress=False)

        if df.empty or len(df) < 150:  # 데이터가 부족한 신규 상장주 제외
            return False

        # 일봉 데이터를 주봉(금요일 기준)으로 변환
        df_weekly = df.resample('W-FRI').agg({'Close': 'last'})

        # 주봉 이동평균선 계산
        df_weekly['MA10'] = df_weekly['Close'].rolling(window=10).mean()
        df_weekly['MA20'] = df_weekly['Close'].rolling(window=20).mean()
        df_weekly['MA60'] = df_weekly['Close'].rolling(window=60).mean()

        # 가장 최근 주간 데이터
        latest = df_weekly.iloc[-1]

        if pd.isna(latest['MA60']):
            return False

        # 10주 > 20주 > 60주 정배열 조건 검사
        if latest['MA10'] > latest['MA20'] > latest['MA60']:
            return True

        return False
    except Exception as e:
        return False


def main():
    print("🚀 주봉 정배열 검색 시작...")

    # 1. 한국 주식 (KOSPI)
    print("🇰🇷 KOSPI 검색 중...")
    kospi = fdr.StockListing('KOSPI')
    kr_aligned = []

    for _, row in kospi.iterrows():
        ticker = row['Code']
        name = row['Name']
        if check_moving_averages(ticker, market='KR'):
            kr_aligned.append(name)

    # 2. 미국 주식 (S&P 500)
    print("🇺🇸 S&P 500 검색 중...")
    sp500 = fdr.StockListing('S&P500')
    us_aligned = []

    for _, row in sp500.iterrows():
        ticker = row['Symbol']
        name = row['Name']
        if check_moving_averages(ticker, market='US'):
            us_aligned.append(f"{name}({ticker})")

    # 3. 텔레그램 메시지 조립 및 전송
    message = "📈 **주간 10/20/60 정배열 포착 리포트**\n\n"

    message += "🇰🇷 **KOSPI**\n"
    message += ", ".join(kr_aligned) if kr_aligned else "포착된 종목이 없습니다."
    message += "\n\n"

    message += "🇺🇸 **S&P 500**\n"
    message += ", ".join(us_aligned) if us_aligned else "포착된 종목이 없습니다."

    print("검색 완료. 텔레그램으로 전송합니다.")
    send_telegram_message(message)


if __name__ == "__main__":
    main()