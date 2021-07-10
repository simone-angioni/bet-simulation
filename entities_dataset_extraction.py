from .elasticsearch_financial import Connection
from .financial_market import *
from .utils import *
import copy

class FinancialEntitiesSet:

    def __init__(self,financial_market_filepath, start = '', end=''):
        self.entities_set = []
        self.period = {}
        self.setPeriod(start, end)
        self._load_financial_market(financial_market_filepath)
        if start != '' and end != '':
            self.es_instance = Connection(self.period)


    def setPeriod(self, start, end):
        self.period['start'] = start
        self.period['end'] = end
        self.es_instance = Connection(self.period)

    def getPeriod(self):
        return self._period.copy()

    def sort_entities_by(self, field, reverse=False):
        self.entities_set = list(sorted(self.entities_set, key=lambda x: x[field], reverse=reverse))

    def get_first_n_entities(self, n):
        return [e['entity'] for e in self.entities_set[:n]]

    def get_entities_appearence_in_days(self, entities):
        days = []
        for entity in entities:
            entity_name = entity
            entity_days = self.es_instance.get_all_news_daily_aggregations(entity_name)
            for day in entity_days:
                if not day in days:
                    days[day] = 0
                days[day] += 1
        return days

    def get_printable_set(self):
        printable_entities = copy.deepcopy(self.entities_set)
        for entity in printable_entities:
            name = entity['entity']
            name = name.split("/")[-1]
            entity['entity'] = name
        return printable_entities

    def _load_financial_market(self, filepath):
        self.financial_market = FinancialMarket(filepath)

    def load_entities_impact(self, min_doc_count, impact_type="magnitude", bet_t = 0):
        entities = self.es_instance.get_entities_in_period(min_doc_count)
        #i=1
        for entity in entities:
            days, total_news = self.es_instance.get_aggregation(entity)
            if impact_type == "magnitude":
                average_stats = self.financial_market.get_daily_stats(days)
                self.entities_set.append({'entity': entity,
                        'total_news': total_news,
                        'num_days': average_stats['total_days'],
                        'next_day': average_stats['next_day']
                        })
            elif impact_type == "bet":
                average_stats = self.financial_market.get_avg_bet_results(days, bet_t)
                self.entities_set.append({'entity': entity,
                        'total_news': total_news,
                        'tried': average_stats['tried'],
                        'successfull': average_stats['successfull'],
                        'win_percentage': average_stats['percentage']
                        })



