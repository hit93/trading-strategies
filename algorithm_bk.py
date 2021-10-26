from datetime import date, datetime, timedelta

# from dotenv.compat import to_env
from utils.database import Database
from dotenv import load_dotenv
from datetime import time
from utils.utils import crossunder, crossover, get_next_thursday, find_option, has_pe, has_ce, has_sell, has_type_2
from utils.wallet import Trade
from utils.logger import Logger
import os
import sys

load_dotenv()

database = Database(
    host=os.environ.get("DATABASE_HOST"),
    user=os.environ.get("DATABASE_USER"),
    password=os.environ.get("DATABASE_PASSWORD"),
    port=os.environ.get("DATABASE_PORT"),
    database=os.environ.get("DATABASE_NAME")
)

logger = Logger('logs.csv')

days = database.get_daily_data()

prev_hour = {
    "timestamp": None,
    "rsi": 0,
    "supertrend": 0,
    "direction": 0,
}

stoploss = {
    "CE": 500,
    "PE": 500,
}
balance = 0

open_postions = []
options = []
started = "not_started"

def open_trade(ins_type, type, conidion, timestamp):

    option = find_option(options, timestamp, 20, 40, 30, ins_type)
    
    if option == None:
        print(conidion, "No option found")
        return
    if type == 1:
        if ((has_ce(open_postions) == False and option["ins_type"] == "CE") 
            or (has_pe(open_postions) == False and option["ins_type"] == "PE")):
        
            option = Trade(option, "sell", type, stoploss[option["ins_type"]])
            ins_type = option.ins_type

            logger.log(
                timestamp=timestamp,
                condition=conidion,
                ins_type=option.ins_type,
                strike_price=option.strike_price,
                expiry=option.expiry,
                price=option.start_price,
                type=option.type,
                buy_or_sell="sell",
                status=option.status,
                pnl="-",
                balance=balance
            )
            open_postions.append(option)
    if type == 2:
        option = Trade(option, "sell", type, stoploss[option["ins_type"]])
        ins_type = option.ins_type

        logger.log(
            timestamp=timestamp,
            condition=conidion,
            ins_type=option.ins_type,
            strike_price=option.strike_price,
            expiry=option.expiry,
            price=option.start_price,
            type=option.type,
            buy_or_sell="sell",
            status=option.status,
            pnl="-",
            balance=balance
        )


        open_postions.append(option)

        option = find_option(options, timestamp, 0, 5, 5, ins_type)
        
        if option == None:
            print(conidion, "No option found (Hedging)")
            return
        
        option = Trade(option, "buy", type, stoploss[option["ins_type"]])

        logger.log(
            timestamp=timestamp,
            condition=conidion + " (Hedging)",
            ins_type=option.ins_type,
            strike_price=option.strike_price,
            expiry=option.expiry,
            price=option.start_price,
            type=option.type,
            buy_or_sell="buy",
            status=option.status,
            pnl="-",
            balance=balance
        )

        open_postions.append(option)


def close_all(ins_type, condition, timestamp):

    global balance

    length = len(open_postions)
    to_pop = []
    
    for index in range(length):

        trade = open_postions[index]

        if trade.ins_type == ins_type and trade.buy_or_sell == "sell":

            try:
                pnl, price, action = trade.close(options, timestamp)
                balance += pnl
                
                logger.log(
                    timestamp=timestamp,
                    condition=condition,
                    ins_type=trade.ins_type,
                    strike_price=trade.strike_price,
                    expiry=trade.expiry,
                    price=price,
                    type=trade.type,
                    buy_or_sell=action,
                    status=trade.status,
                    pnl=pnl,
                    balance=balance
                )

                to_pop.append(index)
            
            except:
                print("[ERROR] Unable to close, {}, {}, {}, {}".format(trade.ins_type, trade.strike_price, trade.expiry, trade.expiry))
                
    for index in reversed(to_pop):
        open_postions.pop(index)


def stoploss_hit(timestamp):

    global balance

    length = len(open_postions)
    to_pop = []
    
    # print(open_postions)
    
    for index in range(length):

        trade = open_postions[index]

        if trade.buy_or_sell == "sell":
            
            # print("## Checking for timestamp", timestamp, " strike price : ", trade.strike_price, " expiry : ", trade.expiry, " type : ", trade.ins_type, " stoploss : ", stoploss[trade.ins_type])

            if not trade.check_stoploss_hit(options, timestamp, stoploss[trade.ins_type]):
               continue 

            try:
                pnl, price, action = trade.close(options, timestamp)
                balance += pnl

                logger.log(
                    timestamp=timestamp,
                    condition="Stoploss Hit",
                    ins_type=trade.ins_type,
                    strike_price=trade.strike_price,
                    expiry=trade.expiry,
                    price=price,
                    type=trade.type,
                    buy_or_sell=action,
                    status=trade.status,
                    pnl=pnl,
                    balance=balance
                )

                to_pop.append(index)
    
            except:
                print("[ERROR] Unable to close, {}, {}, {}, {}".format(trade.ins_type, trade.strike_price, trade.expiry, trade.expiry))
        
    for index in reversed(to_pop):
        open_postions.pop(index)


def type2_purchase(timestamp):

    for trade in open_postions:
        if has_type_2(open_postions) == False and trade.buy_or_sell == "sell" and trade.premium_more_than(options, timestamp, 45):

            if trade.ins_type == "PE":
                ins_type = "CE"
            else:
                ins_type = "PE"

            open_trade(ins_type, 2, "premimum > 40, type2 purchase", timestamp)
            open_trade(ins_type, 1, "premimum > 40, type2 purchase", timestamp)


def make_profit(timestamp):

    global balance

    length = len(open_postions)

    types = []
    to_pop = []

    for index in range(length):

        trade = open_postions[index]

        if trade.buy_or_sell == "sell" and trade.premium_less_than(options, timestamp, 15):

            try:
                pnl, price, action = trade.close(options, timestamp)
                balance += pnl

                logger.log(
                    timestamp=timestamp,
                    condition="Profit Premimum below 15",
                    ins_type=trade.ins_type,
                    strike_price=trade.strike_price,
                    expiry=trade.expiry,
                    price=price,
                    type=trade.type,
                    buy_or_sell=action,
                    status=trade.status,
                    pnl=pnl,
                    balance=balance
                )

                types.append(trade.ins_type)
                to_pop.append(index)
            
            except:
                print("[ERROR] Unable to close, {}, {}, {}, {}".format(trade.ins_type, trade.strike_price, trade.expiry, trade.expiry))
            
    for index in reversed(to_pop):
        open_postions.pop(index)

    for type in types:
        open_trade(type, 1, "Profit Premimum below 15, bying again", timestamp)


def close_type2(timestamp):

    global balance

    length = len(open_postions)
    to_pop = []

    for index in range(length):

        trade = open_postions[index]

        if trade.buy_or_sell == "sell" and trade.type == 2 and trade.premium_less_than(options, timestamp, 15):

            try:
                pnl, price, action = trade.close(options, timestamp)
                balance += pnl

                logger.log(
                    timestamp=timestamp,
                    condition="Type 2 premium < 30",
                    ins_type=trade.ins_type,
                    strike_price=trade.strike_price,
                    expiry=trade.expiry,
                    price=price,
                    type=trade.type,
                    buy_or_sell=action,
                    status=trade.status,
                    pnl=pnl,
                    balance=balance
                )

                to_pop.append(index)
            
            except:
                print("[ERROR] Unable to close, {}, {}, {}, {}".format(trade.ins_type, trade.strike_price, trade.expiry, trade.expiry))
                
    for index in reversed(to_pop):
        open_postions.pop(index)


nifty = []
nifty_prev = 0

def get_nifty_current(timestamp):
    
    for nifty_current in nifty:
        # print(nifty_current)
        if nifty_current["timestamp"] == timestamp:
            return nifty_current["close"]
    
    return 0


def stoploss_nifty(timestamp):
    
    global nifty_prev
    nifty_current = get_nifty_current(timestamp)

    if hour["rsi"] > 70:
            
        if nifty_current < hour["supertrend"] <= nifty_prev:
            close_all("CE", "RSI > 70 and nifty crossunder supertrend", timestamp)

    if hour["rsi"] < 30:
        
        if nifty_prev <= hour["supertrend"] < nifty_current:
            close_all("PE", "RSI < 30 and nifty crossover supertrend", timestamp)

    nifty_prev = nifty_current


for day in days:
    print()
    print("[NEW DAY], Date {}, RSI-D {}".format(day["date"], day["rsi"]))

    hours = database.get_hourly_data(day["date"])
    nifty = database.get_nifty(day["date"])

    for hour in hours:
        
        if started == "not_started":
            if day["is_week_start"] == True and hour["timestamp"] == datetime(day["date"].year, day["date"].month, day["date"].day, 15, 15):
                    started = "hourly_approved"
            else:
                continue
        
        # print(started)
        print("[NEW HOUR], Hour {}, RSI-H {}, Supertrend {}, Direction {}".format(
            hour["timestamp"].time(), hour["rsi"], hour["supertrend"], hour["direction"]))

        minute = 0

        if len(options) != 0:

            if day["rsi"] < 40:

                
                if hour["rsi"] < 30:
                    close_all("PE", "RSI-D < 40, RSI-H < 30", hour["timestamp"])

                if crossunder(hour["rsi"], prev_hour["rsi"], 45):
                    open_trade("CE", 1, "RSI-D < 40, crossunder 45",
                            hour["timestamp"])
                if crossunder(hour["rsi"], prev_hour["rsi"], 40):
                    open_trade("CE", 1, "RSI-D < 40, crossunder 40",
                            hour["timestamp"])

                if not has_sell(open_postions):
                    if prev_hour["direction"] == -1 and hour["direction"] == 1:
                        open_trade(
                            "PE", 1, "RSI-D < 40, no open positions and supertrend changes from down to up", hour["timestamp"])

            if 40 < day["rsi"] < 60:

                if crossover(hour["rsi"], prev_hour["rsi"], 70):
                    close_all("CE", "40 < RSI-D < 60, crossover 70",
                            hour["timestamp"])

                    if hour["direction"] == 1:
                        stoploss["CE"] = hour["supertrend"]
                        

                if crossunder(hour["rsi"], prev_hour["rsi"], 30):
                    close_all("PE", "40 < RSI-D < 60, crossunder 30",
                            hour["timestamp"])

                    if hour["direction"] == -1:
                        stoploss["PE"] = hour["supertrend"]
                        stoploss["CE"] = hour["supertrend"]

                if crossover(hour["rsi"], prev_hour["rsi"], 45):
                    if has_pe(open_postions):
                        open_trade(
                            "PE", 1, "45 < RSI-D < 60, crossover 45", hour["timestamp"])


                if crossunder(hour["rsi"], prev_hour["rsi"], 55):
                    if not has_ce(open_postions):
                        open_trade(
                            "CE", 1, "45 < RSI-D < 60, crossunder 55", hour["timestamp"])

            if 60 < day["rsi"]:

                if hour["rsi"] < 35:
                    close_all("PE", "60 < RSI-D, RSI-H < 35", hour["timestamp"])
                    open_trade("CE", 1, "60 < RSI-D, RSI-H < 35",
                            hour["timestamp"])

                if crossover(hour["rsi"], prev_hour["rsi"], 55):
                    open_trade("PE", 1, "60 < RSI-D, crossover 55",
                            hour["timestamp"])

                if not has_sell(open_postions):
                    if prev_hour["direction"] == -1 and hour["direction"] == 1:
                        open_trade(
                            "PE", 1, "60 < RSI-D, no open positions and supertrend changes from down to up", hour["timestamp"])

        # iterating over mintues
        for minute in range(60):
        
            # maintains the timestamp
            timestamp = hour["timestamp"] + timedelta(minutes=minute)
            # print(timestamp)

            if started == "hourly_approved":
                if timestamp < datetime(day["date"].year, day["date"].month, day["date"].day, 15, 29):
                    continue
                else:
                    started = "approved"
            #         print("[STARTED]")
                    # sys.exit()
            # print(timestamp)

            # break at 3:30
            if timestamp.time() > time(hour=15, minute=30):
                break

            # condition for closing the week
            if day["is_week_start"] == True and timestamp.time() == time(hour=15, minute=30):

                # maintaining closed types
                closed_types = []

                # closing all open positions
                for trade in open_postions:

                    try:
                        pnl, price, action = trade.close(options, timestamp)
                        balance += pnl
                        
                        # print("================================")
                        # print(pnl, price, action)

                        logger.log(
                            timestamp=timestamp,
                            condition="weekly closing",
                            ins_type=trade.ins_type,
                            strike_price=trade.strike_price,
                            expiry=trade.expiry,
                            price=price,
                            type=trade.type,
                            buy_or_sell=action,
                            status=trade.status,
                            pnl=pnl,
                            balance=balance
                        )

                        # trades which were sold are bought back
                        if action == "buy":
                            # print(trade.ins_type)
                            closed_types.append(trade.ins_type)

                    except:
                        print("[ERROR] Unable to close, {}, {}, {}, {}".format(trade.ins_type, trade.strike_price, trade.expiry, trade.expiry))

                # refresh options cache from database
                options = database.get_options_data(
                    timestamp, get_next_thursday(timestamp).date())
                # print(options)
                print("[STARTING NEW INSTANCE] at {}, next thusday at {}".format(timestamp, get_next_thursday(timestamp)))

                # reset variables
                stoploss["CE"] = 500
                stoploss["PE"] = 500
                open_postions = []

                # again purchasing closed trades
                for trade_type in closed_types:
                    # print(" **** ", trade_type, timestamp)
                    open_trade(trade_type, 1, "weekly reopening",
                               timestamp)

            if time(hour=9, minute=15) < timestamp.time() < time(hour=9, minute=30):
                pass
            #     for trade in open_postions:
            #         if trade.buy_or_sell == "sell" and trade.check_stoploss_hit(options, timestamp, stoploss[trade.ins_type]):
            #             trade.update_stoploss(options)

            # if timestamp.time() == time(hour=9, minute=30):
            #     pass
                # final stoploss condition
                # if hour["direction"] == -1 and len(options) > 0:
                #     nifty_data = database.get_nifty_915_930(day["date"])

                #     for nifty in nifty_data:
                #         if nifty["close"] >= hour["supertrend"]:

                #             temp_nifty = [x['close'] for x in nifty_data]
                #             stoploss["CE"] = max(temp_nifty)
                #             stoploss["PE"] = min(temp_nifty)

                #             break

            # working hours
            # all stop loss conditions goes here
            if timestamp.time() >= time(hour=9, minute=30):
                # stoploss_hit(timestamp)
                make_profit(timestamp)
                type2_purchase(timestamp)
                close_type2(timestamp)
                stoploss_nifty(timestamp)

        # storing previous hour data
        prev_hour = hour