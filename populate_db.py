import pandas as pd
from dotenv import load_dotenv
from utils.database import Database
import os


load_dotenv()

database = Database(
    host = os.environ.get('DATABASE_HOST'),
    port = os.environ.get('DATABASE_PORT'),
    user = os.environ.get('DATABASE_USER'),
    password = os.environ.get('DATABASE_PASSWORD'),
    database = os.environ.get('DATABASE_NAME')
)

### mintue code
def get_minute_objects(row):
    return database.minute(
        timestamp = row['tm'],
        open = row['open'],
        high = row['high'],
        low = row['low'],
        close = row['close'],
        rsi = row['rsi'],
        supertrend = row['supertrend'],
        direction = row['direction'],
    )
    
minute_csv = pd.read_csv('old_db/1m.csv').apply(get_minute_objects, axis=1)
database.session.bulk_save_objects(minute_csv)
database.session.commit()


### quater
def get_quarter_objects(row):
    return database.quarter(
        timestamp = row['tm'],
        open = row['open'],
        high = row['high'],
        low = row['low'],
        close = row['close'],
        rsi = row['rsi'],
        supertrend = row['supertrend'],
        direction = row['direction'],
    )
    
quarter_csv = pd.read_csv('old_db/15min.csv').apply(get_quarter_objects, axis=1)
database.session.bulk_save_objects(quarter_csv)
database.session.commit()


### hourly
def get_hourly_objects(row):
    return database.hour(
        timestamp = row['tm'],
        open = row['open'],
        high = row['high'],
        low = row['low'],
        close = row['close'],
        rsi = row['rsi'],
        supertrend = row['supertrend'],
        direction = row['direction'],
    )
    
hourly_csv = pd.read_csv('old_db/hourly.csv').apply(get_hourly_objects, axis=1)
database.session.bulk_save_objects(hourly_csv)
database.session.commit()


### daily
def get_daily_objects(row):
    return database.day(
        timestamp = row['tm'],
        open = row['open'],
        high = row['high'],
        low = row['low'],
        close = row['close'],
        rsi = row['rsi'],
        supertrend = row['supertrend'],
        direction = row['direction'],
    )
    
daily_csv = pd.read_csv('old_db/daily.csv').apply(get_daily_objects, axis=1)
database.session.bulk_save_objects(daily_csv)
database.session.commit()


### options
# def get_option_objects(row):
#     return database.options(
#         timestamp = row['tm'],
#         open = row['open'],
#         close = row['close'],
#         high = row['high'],
#         low = row['low'],
#         strike = row['strike_price'],
#         from_date = row['from_date'], 
#         to_date = row['to_date'],
#         type = row['type'],
#         volume = row['volume'],     
#     )
# options_csv = pd.read_csv('old_db/options.csv').apply(get_option_objects, axis=1)
# database.session.bulk_save_objects(options_csv)
# database.session.commit()
