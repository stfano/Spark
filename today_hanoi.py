import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
import pytz
def update_stock_data():
    now_time_utc = datetime.utcnow()
    
    vietnam_timezone = pytz.timezone('Asia/Ho_Chi_Minh')
    now_time_local = now_time_utc.replace(tzinfo=pytz.utc).astimezone(vietnam_timezone)
    formatted_time_local = now_time_local.strftime("%Y%m%d%H%M")
    now_time_vietnam = int(formatted_time_local)

    now_time_local = now_time_local.replace(hour=0, minute=0, second=0, microsecond=0)

    formatted_time_local = now_time_local.strftime("%Y%m%d%H%M")
    now_time_00= int(formatted_time_local)
    
    url_stock_list = "https://api.stock.naver.com/stock/exchange/HANOI/"
    all_data = []  

    page = 1
    while True:
        payload = {
            "page": str(page),
            "pageSize": "20"
        }
        response = requests.get(url_stock_list, params=payload)
        if response.status_code == 200:
            data = response.json().get('stocks', [])
            all_data.extend(data)

            if len(data) == 0:
                break 
            else:
                page += 1  
        else:
            print(f'HTTP 요청 실패: {response.status_code}')
            break
            
    hanoi_stocks = pd.DataFrame(all_data)
    reutersCode = hanoi_stocks['reutersCode'].tolist()

    for code in tqdm(reutersCode):
        url = f"https://api.stock.naver.com/chart/foreign/item/{code}/day?startDateTime={now_time_00}&endDateTime={now_time_vietnam}"
        r = requests.get(url, data=payload)
        r = r.json()
        df_r = pd.DataFrame(r)
        df_r['Symbol_code'] = code
        df_r['localDate'] = pd.to_datetime(df_r['localDate'], format='%Y%m%d')
        df_r = df_r.rename(columns={'localDate': 'date', 'closePrice': 'close', 'openPrice': 'open',   'highPrice': 'high', 'lowPrice': 'low', 'accumulatedTradingVolume' : 'volume', 'Symbol_code':'Symbol_code'})
        df_r.to_csv(f"./today_hanoi/{code}.csv", index=False, encoding='utf-8')

if __name__ == "__main__":
    update_stock_data()