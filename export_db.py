###########################################################################################
#
# MATHRITHMS PROPRIETARY INFORMATION
#
# This software is confidential.  Mathrithms, or one of its subsidiaries, has supplied this
# software to you under the terms of a license agreement,nondisclosure agreement or both.
# You may not copy, disclose, or use this software except in accordance with those terms.
#
# Copyright 2021 Mathrithms or its subsidiaries.  All Rights Reserved.
#
# Mathrithms MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF THE SOFTWARE,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.
# Mathrithms SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
# MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
#
###########################################################################################

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from sqlalchemy import create_engine, select
import os
from dotenv import load_dotenv
import datetime
import numpy as np
import pandas as pd
import dask.dataframe as dd
from pandas_ta.momentum.rsi import rsi
from pandas_ta.overlap.supertrend import supertrend
from sqlalchemy import Table, Column, MetaData, Integer, String, DateTime, Float, insert

load_dotenv()

class SQLDatabase:
    
    def __init__(self, host, port, database, user, password, dialect="postgresql"):
        self.CONN_STR = "{}://{}:{}@{}:{}/{}".format(dialect, user, password, host, port, database)
        
        try:
            self.engine = create_engine(self.CONN_STR)
            Base = automap_base()
            Base.prepare(self.engine, reflect=True)
            self.session = Session(self.engine)
            self.ohlcv_1ds= Base.classes.ohlcv_1ds
            self.ohlcv_1ms= Base.classes.ohlcv_1ms
            self.ohlcv_1hs = Base.classes.ohlcv_1hs
            self.ohlcv_options = Base.classes.ohlcv_options
            self.indicators_h = Base.classes.indicators_h
            self.indicators_d = Base.classes.indicators_d
            self.indicators_15m = Base.classes.indicators_15m
        except Exception as error:
            raise error
    
    def create_tables(self, metadata):
        return metadata.create_all(self.engine)
    
    def get_ohlc_1d(self):
        
        return self.session.query(
                self.ohlcv_1ds.open,
                self.ohlcv_1ds.high,
                self.ohlcv_1ds.low,
                self.ohlcv_1ds.close,
                self.ohlcv_1ds.date,
            ).order_by(
                self.ohlcv_1ds.date
            ).all()
    
    def get_ohlc_1h(self):
        return self.session.query(
                self.ohlcv_1hs.open,
                self.ohlcv_1hs.high,
                self.ohlcv_1hs.low,
                self.ohlcv_1hs.close,
                self.ohlcv_1hs.tm,
            ).order_by(
                self.ohlcv_1hs.tm
            ).all()
            
    def get_ohlc_1m(self):
        return self.session.query(
                self.ohlcv_1ms.open,
                self.ohlcv_1ms.high,
                self.ohlcv_1ms.low,
                self.ohlcv_1ms.close,
                self.ohlcv_1ms.tm,
            ).order_by(
                self.ohlcv_1ms.tm
            ).all()
                
    def get_ohlc_15min(self):
        data = self.get_ohlc_1m()
        
        to_return = []
        temp = []
        
        for i in range(len(data)):
            
            temp.append(data[i])
            
            if data[i][4].minute % 15 == 0:
                open = temp[0][0]
                close = temp[-1][3]
                high = max(temp, key=lambda x: x[1])[1]
                low = min(temp, key=lambda x: x[2])[2]
                to_return.append((open, high, low, close, data[i][4]))
                temp = []
        
        return to_return
    
    def get_options_data(self):
        return self.session.query(
                self.ohlcv_options.strike_price,
                self.ohlcv_options.from_date,
                self.ohlcv_options.to_date,
                self.ohlcv_options.open,
                self.ohlcv_options.high,
                self.ohlcv_options.low,
                self.ohlcv_options.close,
                self.ohlcv_options.volume,
                self.ohlcv_options.tm,
                self.ohlcv_options.ins_type,
            ).filter(
                self.ohlcv_options.close <= 100
            ).order_by(
                self.ohlcv_options.tm
            ).all()
        
database = SQLDatabase(host=os.getenv('OLD_DATABASE_HOST'),
                            port=os.getenv('OLD_DATABASE_PORT'),
                            database=os.getenv('OLD_DATABASE_NAME'),
                            user=os.getenv('OLD_DATABASE_USER'),
                            password=os.getenv('OLD_DATABASE_PASSWORD'))

def process_data(data):
    df = pd.DataFrame(data, columns=['open', 'high', 'low', 'close', 'tm'])
    df['rsi'] = rsi(df['close'], 14)
    super_trend = supertrend(df['high'], df['low'], df['close'], 7, 2)
    super_trend = super_trend.rename(columns={
                                                'SUPERT_7_2.0': 'supertrend',
                                                'SUPERTd_7_2.0': 'direction'
                                            })
    super_trend = super_trend.drop(columns=['SUPERTl_7_2.0', 'SUPERTs_7_2.0'])
    df = pd.concat([df, super_trend], axis=1)
    return df.fillna(0)


data = process_data(database.get_ohlc_1d())
data.to_csv('old_db/daily.csv', index=False)

data = process_data(database.get_ohlc_1h())
data.to_csv('old_db/hourly.csv', index=False)

data = process_data(database.get_ohlc_15min())
data.to_csv('old_db/15min.csv', index=False)

data = process_data(database.get_ohlc_1m())
data.to_csv('old_db/1m.csv', index=False)

pd.DataFrame(database.get_options_data(), columns=['strike_price', 'from_date', 'to_date', 'open', 'high', 'low', 'close', 'volume', 'tm', 'type']).to_csv('old_db/options.csv', index=False)