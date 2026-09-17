import os
import sys

# 🚀 1. 스크립트 실행 시 yfinance 강제 업데이트
print("🔄 yfinance 패키지를 최신 버전으로 업데이트하는 중...")
os.system(f"{sys.executable} -m pip install --upgrade yfinance --quiet")

import yfinance as yf
import FinanceDataReader as fdr
import pandas as pd
import requests
import datetime
import warnings
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 경고 메시지 숨김
warnings.filterwarnings('ignore')

# --- 텔레그램 설정 ---
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '8383235460:AAFAdBAFy5dUQE1wqkShqF3X8T9FbIaUJQc')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '1729501017')

# --- 통신 안정성 및 차단 방지 세션 설정 (User-Agent 추가) ---
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
})
retry = Retry(connect=5, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry, pool_connections=5, pool_maxsize=5)
session.mount('http://', adapter)
session.mount('https://', adapter)


def send_telegram_message(message):
    """텔레그램으로 메시지를 전송하는 함수 (4096자 초과 시 분할 전송)"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    max_length = 4000

    try:
        for i in range(0, len(message), max_length):
            payload = {
                "chat_id": CHAT_ID,
                "text": message[i:i + max_length],
                "parse_mode": "Markdown"
            }
            response = requests.post(url, data=payload, timeout=10)
            response.raise_for_status()
    except Exception as e:
        print(f"❌ 텔레그램 전송 실패: {e}")


def check_ma_logic(df_ticker):
    """(로컬 연산) 주봉 10, 20, 60 정배열 2주 내 진입 여부 판별"""
    try:
        df_weekly = df_ticker.resample('W-FRI').agg({'Close': 'last'}).dropna()

        if len(df_weekly) < 3:
            return False

        df_weekly['MA10'] = df_weekly['Close'].rolling(window=10).mean()
        df_weekly['MA20'] = df_weekly['Close'].rolling(window=20).mean()
        df_weekly['MA60'] = df_weekly['Close'].rolling(window=60).mean()

        current = df_weekly.iloc[-1]
        prev_1w = df_weekly.iloc[-2]
        prev_2w = df_weekly.iloc[-3]

        if pd.isna(current['MA60']) or pd.isna(prev_1w['MA60']) or pd.isna(prev_2w['MA60']):
            return False

        c_10, c_20, c_60 = current['MA10'], current['MA20'], current['MA60']
        p1_10, p1_20, p1_60 = prev_1w['MA10'], prev_1w['MA20'], prev_1w['MA60']
        p2_10, p2_20, p2_60 = prev_2w['MA10'], prev_2w['MA20'], prev_2w['MA60']

        current_aligned = (c_10 > c_20 > c_60)
        prev_1w_aligned = (p1_10 > p1_20 > p1_60)
        prev_2w_aligned = (p2_10 > p2_20 > p2_60)

        if current_aligned and (not prev_1w_aligned or not prev_2w_aligned):
            return True

        return False
    except Exception:
        return False


def process_kr_asset(ticker, name):
    """한국 종목 개별 다운로드 및 이평선 검사"""
    try:
        start_date = (datetime.datetime.now() - datetime.timedelta(days=730)).strftime('%Y-%m-%d')
        df = fdr.DataReader(ticker, start_date)

        if df.empty or len(df) < 300 or 'Close' not in df.columns:
            return None

        df_ticker = df[['Close']].copy()

        if check_ma_logic(df_ticker):
            return f"{name}({ticker})"

    except Exception:
        pass
    return None


def run_kr_concurrent(items, max_workers=10):
    """한국 주식/ETF 다중 처리"""
    aligned_list = []
    ticker_col = 'Code' if 'Code' in items.columns else 'Symbol'

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_kr_asset, row[ticker_col], row['Name']) for _, row in items.iterrows()]

        for future in as_completed(futures):
            result = future.result()
            if result:
                aligned_list.append(result)
    return aligned_list


def get_filtered_kr_etfs(min_marcap_100m=4000):
    """시가총액 4천억 이상 국내 ETF만 가져오기"""
    try:
        url = "https://finance.naver.com/api/sise/etfItemList.nhn"
        response = session.get(url, timeout=10)
        data = response.json()
        
        etf_list = data['result']['etfItemList']
        df = pd.DataFrame(etf_list)
        
        df_filtered = df[df['marketSum'] >= min_marcap_100m]
        df_filtered = df_filtered.rename(columns={'itemcode': 'Symbol', 'itemname': 'Name'})
        
        print(f"    ✓ 전체 ETF 중 {len(df_filtered)}개가 4000억 이상 조건 충족")
        return df_filtered[['Symbol', 'Name']]
    except Exception as e:
        print(f"⚠ 국내 ETF 목록 로드 실패: {e}")
        return pd.DataFrame(columns=['Symbol', 'Name'])


def check_us_market_cap(ticker, name, asset_type):
    """정배열을 통과한 종목에 한하여 API를 통해 시가총액/AUM 검사"""
    try:
        time.sleep(random.uniform(0.5, 1.0))
        
        ticker_obj = yf.Ticker(ticker, session=session)
        if asset_type == 'STOCK':
            cap = ticker_obj.fast_info.get('market_cap', 0)
        else:
            cap = ticker_obj.info.get('totalAssets', 0)

        if cap is not None and cap >= 25_000_000_000:
            return f"{name}({ticker})"
    except Exception:
        pass
    return None


def process_us_batch(items, asset_type, chunk_size=50, max_workers=2):
    """미국 종목: 대량 다운로드 (threads=False로 429 차단 방지) -> 이평선 검사 -> 시가총액 검사"""
    passed_ma_tickers = []
    symbol_name_map = dict(zip(items['Symbol'], items['Name']))
    symbols = list(items['Symbol'].unique())

    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        print(f"    📥 데이터 다운로드 및 분석 중... ({i + 1} ~ {min(i + chunk_size, len(symbols))} / {len(symbols)})")

        time.sleep(2) # 서버 부하 방지용 대기
        df_all = yf.download(chunk, period='2y', progress=False, session=session, threads=False)

        if df_all.empty:
            continue

        if isinstance(df_all.columns, pd.MultiIndex):
            if 'Close' not in df_all.columns.levels[0]:
                continue
            close_prices = df_all['Close']

            for ticker in chunk:
                if ticker in close_prices.columns:
                    df_ticker = close_prices[[ticker]].copy()
                    df_ticker.columns = ['Close']
                    df_ticker.dropna(inplace=True)

                    if len(df_ticker) >= 300 and check_ma_logic(df_ticker):
                        passed_ma_tickers.append(ticker)
        else:
            if 'Close' in df_all.columns:
                df_ticker = df_all[['Close']].copy()
                df_ticker.dropna(inplace=True)

                if len(df_ticker) >= 300 and check_ma_logic(df_ticker):
                    passed_ma_tickers.append(chunk[0])

    print(f"    🎯 정배열 통과 종목 ({len(passed_ma_tickers)}개). 시가총액/AUM 필터링 시작...")

    final_list = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(check_us_market_cap, ticker, symbol_name_map[ticker], asset_type)
            for ticker in passed_ma_tickers
        ]
        for future in as_completed(futures):
            result = future.result()
            if result:
                final_list.append(result)

    return final_list


def main():
    print("🚀 주봉 정배열(2주 이내 진입) 조건 검색 시작...")

    print("🇰🇷 국내 주식 (KOSPI & KOSDAQ) 검색 중...")
    krx = fdr.StockListing('KRX')
    kr_stocks_filtered = krx[
        (krx['Market'].isin(['KOSPI', 'KOSDAQ'])) &
        (krx['Marcap'] >= 400_000_000_000) 
        ]
    kr_aligned_stocks = run_kr_concurrent(kr_stocks_filtered, max_workers=10)

    print("🇰🇷 국내 ETF 검색 중 (시가총액 4000억 이상)...")
    kr_etfs_filtered = get_filtered_kr_etfs(4000)
    if not kr_etfs_filtered.empty:
        kr_aligned_etfs = run_kr_concurrent(kr_etfs_filtered, max_workers=10)
    else:
        kr_aligned_etfs = []

    print("🇺🇸 미국 주식 (S&P 500 전체 및 NASDAQ 상위 500개) 검색 중...")
    sp500 = fdr.StockListing('S&P500')
    nasdaq = fdr.StockListing('NASDAQ')
    
    # 나스닥은 시가총액 정보가 있는 경우 정렬 후 상위 500개 추출 (없으면 상위 500개 슬라이스)
    if 'Marcap' in nasdaq.columns:
        nasdaq = nasdaq.sort_values(by='Marcap', ascending=False)
    nasdaq_top500 = nasdaq.head(500)

    us_stocks = pd.concat([sp500, nasdaq_top500]).drop_duplicates(subset='Symbol')
    us_aligned_stocks = process_us_batch(us_stocks, asset_type='STOCK', chunk_size=50, max_workers=2)

    print("🇺🇸 미국 ETF 검색 중...")
    try:
        us_etfs = fdr.StockListing('ETF/US')
        if us_etfs.empty:
            raise ValueError("미국 ETF 리스트가 비어 있습니다.")
        us_aligned_etfs = process_us_batch(us_etfs, asset_type='ETF', chunk_size=50, max_workers=2)
    except Exception as e:
        print(f"⚠ 미국 ETF 리스트 로드 실패: {e}")
        us_aligned_etfs = []

    message = "📈 **주간 10/20/60 정배열 포착 리포트 (최근 2주 내 진입)**\n\n"

    message += "🇰🇷 **국내 주식 (시총 4천억 이상)**\n"
    message += ", ".join(kr_aligned_stocks) if kr_aligned_stocks else "포착된 종목이 없습니다."
    message += "\n\n"

    message += "🇰🇷 **국내 ETF (시총 4천억 이상)**\n"
    message += ", ".join(kr_aligned_etfs) if kr_aligned_etfs else "포착된 종목이 없습니다."
    message += "\n\n"

    message += "🇺🇸 **미국 주식 (S&P500 & 나스닥 상위 500 / 시총 250억$ 이상)**\n"
    message += ", ".join(us_aligned_stocks) if us_aligned_stocks else "포착된 종목이 없습니다."
    message += "\n\n"

    message += "🇺🇸 **미국 ETF (AUM 250억$ 이상)**\n"
    message += ", ".join(us_aligned_etfs) if us_aligned_etfs else "포착된 종목이 없습니다."

    print("\n✅ 검색 완료. 텔레그램으로 전송합니다.")
    send_telegram_message(message)


if __name__ == "__main__":
    main()
