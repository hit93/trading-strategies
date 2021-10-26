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

hourly_csv = pd.read_csv('hourly_final.csv')

to_push = []

for index, row in hourly_csv.iterrows():
    to_push.append(
        database.hour(
            timestamp = row['tm'],
            open = row['open'],
            high = row['high'],
            low = row['low'],
            close = row['close'],
            rsi = row['rsi'],
            supertrend = row['supertrend'],
            direction = row['direction'],
            ppo = row['ppo'],
            ppo_signal = row['ppo_signal'],
            hist = row['hist']
        )        
    )
database.session.bulk_save_objects(to_push)
database.session.commit()
print('Done Hourly')

