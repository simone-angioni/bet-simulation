from elasticsearch import Elasticsearch
import pandas as pd
from .utils import *

class FinancialMarket:

    def __init__(self, filepath):
        self.parse(filepath)
        self.create_date_time_series()

    def parse(self, filepath):
        self.series = dict()
        if filepath.endswith(".csx"):
            df = pd.read_csv(filepath)
        if filepath.endswith(".xlsx"):
            df = pd.read_excel(filepath)
        for i,r in df.iterrows():
            if not r['date_time'] in self.series:
                self.series[r['date_time']] = dict()
            self.series[r['date_time']]['date_time'] = r['date_time']
            self.series[r['date_time']]['open'] = r['open']
            self.series[r['date_time']]['close'] = r['close']
            self.series[r['date_time']]['high'] = r['high']
            self.series[r['date_time']]['low'] = r['low']
            self.series[r['date_time']]['up'] = r['up']
            self.series[r['date_time']]['down'] = r['down']
            self.series[r['date_time']]['volume'] = r['volume']
            self.series[r['date_time']]['delta_current_day'] = r['delta_current_day']
            self.series[r['date_time']]['delta_next_day'] = r['delta_next_day']
            self.series[r['date_time']]['delta_percentage_current_day'] = r['delta_current_day_percentage']
            self.series[r['date_time']]['delta_percentage_next_day'] = r['delta_next_day_percentage']

    def create_date_time_series(self):
        self.series_list = []
        self.date_times = []
        for r in self.series:
            d = {}
            d['date_time'] = r['date_time']
            d['open'] = r['open']
            d['close'] = r['close']
            d['high'] = r['high']
            d['low'] = r['low']
            d['up'] = r['up']
            d['down'] = r['down']
            d['volume'] = r['volume']
            d['delta_current_day'] = r['delta_current_day']
            d['delta_next_day'] = r['delta_next_day']
            d['delta_percentage_current_day'] = r['delta_current_day_percentage']
            d['delta_percentage_next_day'] = r['delta_next_day_percentage']
            self.series_list.append(d)
            self.date_times.append(r['date_time'])

    def get_next_x_days_stats(self, index, x, start=1):
        stats = {}
        opens = []
        closes = []
        delta_percentages = []
        deltas = []
        max_range = x if not index + x > len(self.series_list) else len(self.series_list) - index
        for i in range(start, max_range):
            day = self.series_list[index + i]
            opens.append(day['open'])
            closes.append(day['close'])
            deltas.append(day['delta_current_day'])
            delta_percentages.append(abs(day['delta_percentage_current_day']))

        if len(closes) == 0 or len(opens) == 0:
            stats['open'] = 0
            stats['close'] = 0
            stats['delta'] = 0
            stats['delta_percentage'] = 0
            stats['average_delta'] = 0
            stats['average_delta_percentage'] = 0
        else:
            stats['open'] = opens[0]
            stats['close'] = closes[-1]
            stats['delta'] = closes[-1] - opens[0]
            stats['delta_percentage'] = ((closes[-1] - opens[0])/opens[0])*100 if not opens[0] == 0 else 0
            stats['average_delta'] = sum(deltas)/len(deltas) if not len(deltas) == 0 else 0
            stats['average_delta_percentage'] = sum(delta_percentages)/len(delta_percentages) if not len(delta_percentages) == 0 else 0

        return stats

    def get_delta_in_period(self, start, end):
        open_day_list = []
        close_day_list = []
        percentage = []
        for day in daterange(start, end):
            str_day = day.strftime("%Y-%m-%d")
            if str_day in self.date_times:
                index = self.date_times.index(str_day)
                open_day_list.append(self.series_list[index]['open'])
                close_day_list.append(self.series_list[index]['close'])
                percentage.append(abs(self.series_list[index]['delta_percentage_current_day']))

        #print(f"close: {close_day_list[-1]} \nopen: {open_day_list[0]}\nlen: {len(open_day_list)} {len(close_day_list)}\ndelta percentage: {abs((close_day_list[-1] - open_day_list[0])/open_day_list[0])}")
        #return abs((close_day_list[-1] - open_day_list[0])/open_day_list[0])/len(open_day_list)
        average = sum(percentage)/len(percentage) if len(percentage) > 0 else 0
        return average #, open_day_list[0], close_day_list[-1]

    def get_daily_stats(self, days):
        next_day_deltas = []
        denominator = 0
        for day in days:
            current_day = string_to_datetime(day['key'])
            num_news = day['doc_count']
            denominator+= num_news
            found_index = get_nearest_day(current_day, self.date_times)
            next_day = abs(self.get_next_x_days_stats(found_index, 2, start = 1)['average_delta_percentage'])
            next_day_deltas.append(num_news*next_day)
        stats = {
            'next_day': sum(next_day_deltas)/denominator if denominator > 0 else "NaN",
            'total_days': len(days)
        }
        return stats

    def get_avg_bet_results(self, days, threshold):
        bets = {}
        bets['tried'] = 0
        bets['successfull'] = 0
        for day in days:
            current_day = string_to_datetime(day['key'])
            num_news = day['doc_count']
            found_index = get_nearest_day(current_day, self.date_times)
            found_day = string_to_datetime(self.series_list[found_index]['date_time'])
            if isSameDay(current_day, found_day):
                next_day = abs(self.get_next_x_days_stats(found_index, 2, start = 1)['average_delta_percentage'])
            else:
                next_day = abs(self.get_next_x_days_stats(found_index, 1, start=0)['average_delta_percentage'])
            bets['tried'] += 1
            if next_day > threshold:
                bets['successfull'] += 1
        bets['percentage'] = bets['successfull']/bets['tried'] if not bets['tried'] == 0 else 0
        return bets

    def bet(self, entities_appearence, threshold, period, simulation_type = "classic"):
        simulation_summary = dict()
        bet_tried = 0
        successfull_bets = 0
        for day in daterange(*period):
            daystr = day.strftime("%Y-%m-%d")
            if daystr in self.series:
                if daystr in entities_appearence:
                    if simulation_type == "classic":
                        bet_tried+=1
                    elif simulation_type == "occurrence":
                        if entities_appearence[daystr] > 1:
                            bet_tried += 1
                    magnitude = get_next_day_magnitude(self.series[daystr])
                    if magnitude > threshold:
                        successfull_bets += 1

        simulation_summary['bet_tried'] = bet_tried
        simulation_summary['bet_successfull'] = successfull_bets
        simulation_summary['win_percentage'] = (successfull_bets/bet_tried) * 100
        return simulation_summary

    def random_bet(self, period, thresholds):
        simulation = []
        for threshold in thresholds:
            simulation_summary = dict()
            bet_tried = 0
            successfull_bets = 0
            for day in daterange(*period):
                daystr = day.strftime("%Y-%m-%d")
                if daystr in self.series:
                    bet_tried+=1
                    magnitude = get_next_day_magnitude(self.series[daystr])
                    if magnitude > threshold:
                        successfull_bets += 1

            simulation_summary['1) type'] = 'random'
            simulation_summary['2) threshold'] = threshold
            simulation_summary['3) bet_tried'] = bet_tried
            simulation_summary['4) bet_successfull'] = successfull_bets
            simulation_summary['5) win_percentage'] = (successfull_bets/bet_tried) * 100
            simulation.append(simulation_summary)
        return simulation