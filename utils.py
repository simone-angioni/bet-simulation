import datetime
import pandas as pd

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

def get_next_day_magnitude(daily_market):
    next_day_percentage = daily_market['delta_percentage_next_day']
    return abs(next_day_percentage)

def get_next_day_delta(daily_market):
    next_day_percentage = daily_market['delta_percentage_next_day']
    return next_day_percentage

def create_dict(day, daily_market):
    d = dict()
    d['day'] = day
    d['delta_percentage_next_day'] = daily_market['delta_percentage_next_day']
    d['bet'] = 0
    d['successfull'] = 0
    return d

def save_simulation(simulation, filepath):
    writer = pd.ExcelWriter(filepath)
    for type in simulation:
        temp = pd.DataFrame(simulation[type])
        temp.to_excel(writer, sheet_name=type)
    writer.save()

def string_to_datetime(string):
    l = string.split("-")
    day = datetime.datetime(int(l[0]), int(l[1]), int(l[2]))
    return day

def get_nearest_day(day, series):
    while True:
        try:
            date_str = day.strftime("%Y-%m-%d")
            index = series.index(date_str)
        except ValueError:
            day = day + datetime.timedelta(days=1)
        else:
            return index

def isSameDay(day1, day2):
    return day1 == day2