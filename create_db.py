from sqlalchemy import MetaData, Table, Column, Float, DateTime, Integer, String, create_engine
import os
from sqlalchemy.orm import Session, session
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
meta = MetaData()

connection_string = "{}://{}:{}@{}:{}/{}".format(
    'postgresql',
    os.environ.get('DATABASE_USER'),
    os.environ.get('DATABASE_PASSWORD'),
    os.environ.get('DATABASE_HOST'),
    os.environ.get('DATABASE_PORT'),
    os.environ.get('DATABASE_NAME')
)

engine = create_engine(connection_string)

minute_table = Table(
    'minute', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('timestamp', DateTime),
    Column('open', Float),
    Column('high', Float),
    Column('low', Float),
    Column('close', Float),
    Column('rsi', Float),
    Column('supertrend', Float),
    Column('direction', Integer),
)
minute_table.create(engine, checkfirst=True)

quarter_table = Table(
    'quarter', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('timestamp', DateTime),
    Column('open', Float),
    Column('high', Float),
    Column('low', Float),
    Column('close', Float),
    Column('rsi', Float),
    Column('supertrend', Float),
    Column('direction', Integer),
)
quarter_table.create(engine, checkfirst=True)

hour_table = Table(
    'hour', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('timestamp', DateTime),
    Column('open', Float),
    Column('high', Float),
    Column('low', Float),
    Column('close', Float),
    Column('rsi', Float),
    Column('supertrend', Float),
    Column('direction', Integer),
    Column('ppo', Float),
    Column('ppo_signal', Float),
    Column('hist', Float)
)
hour_table.create(engine, checkfirst=True)

day_table = Table(
    'day', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('timestamp', DateTime),
    Column('open', Float),
    Column('high', Float),
    Column('low', Float),
    Column('close', Float),
    Column('rsi', Float),
    Column('supertrend', Float),
    Column('direction', Integer),
)
day_table.create(engine, checkfirst=True)

options_table = Table(
    'options', meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('timestamp', DateTime),
    Column('strike', Float),
    Column('from_date', DateTime),
    Column('to_date', DateTime),
    Column('type', String(2)),
    Column('open', Float),
    Column('high', Float),
    Column('low', Float),
    Column('close', Float),
    Column('volume', Integer),    
)
options_table.create(engine, checkfirst=True)

