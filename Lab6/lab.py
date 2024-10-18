from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests


spark = SparkSession.builder \
    .appName("CoinGecko Data Processing") \
    .getOrCreate()

api_key = "CG-isTrT5ZBmtKTv5mFos7XbrDa"

api_url = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    "vs_currency": "usd",   
    "order": "market_cap_desc",  
    "per_page": 250,  
    "page": 1,  
    "sparkline": False  
}

response = requests.get(api_url, params=params)
cryptos_data = response.json()

cryptos = [{
    'id': crypto['id'],
    'name': crypto['name'],
    'symbol': crypto['symbol'],
    'current_price': crypto['current_price'],
    'market_cap': crypto['market_cap'],
    'total_volume': crypto['total_volume']
} for crypto in cryptos_data]

df_cryptos = pd.DataFrame(cryptos)

cryptos_df = spark.createDataFrame(df_cryptos)
cryptos_df.show()

filtered_cryptos = cryptos_df.filter(col("current_price") < 100)
filtered_cryptos.show()

from pyspark.sql.functions import avg

average_market_cap = filtered_cryptos.agg(avg(col("market_cap")).alias("average_market_cap"))
average_market_cap.show()