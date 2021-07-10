import datetime
from financial_market import *
from entities_dataset_extraction import *
from config import Config

cf = Config(1.0)
cf.set_up()

print("> Loading financial market series...")

bet_market = FinancialMarket(cf.financial_market_path)

types = ['magnitude', 'bet_0.5', 'bet_1', 'bet_1.5', 'bet_2', 'bet_3']

### Creating Training Set with entities that appears at least 33 times
entity_ranking = {}

for type in types:
    print(f"> Creating training set for {type}...")
    entity_ranking[type] = FinancialEntitiesSet(cf.financial_market_path, start=cf.period_train['start'], end=cf.period_train['end'])
    if "bet" in type:
        entity_ranking[type].load_entities_impact(cf.min_doc_count, impact_type='bet', bet_t=float(type.split("_")[-1]))
        entity_ranking[type].sort_entities_by("win_percentage", reverse=True)
    else:
        entity_ranking[type].load_entities_impact(cf.min_doc_count)
        entity_ranking[type].sort_entities_by("next_day", reverse=True)


### Set test period
for type in entity_ranking:
    entity_ranking[type].setPeriod(cf.period_test['start'], cf.period_test['end'])


### Set Bet Parameters
top = cf.top
thresholds = cf.thresholds

period = cf.period_datetime
### Simulate
simulation = {}
for type in entity_ranking:
    print(f"> Performing Bet Simulation on {type}...")
    simulation[type] = []
    for n in top:
        entity_days = entity_ranking[type].get_entities_appearence_in_days(entity_ranking[type].get_first_n_entities(n))
        for x in thresholds:
            d = dict()
            d['1) Type'] = type
            d['2) Number of Top Entities'] = n
            d['3) Magnitude Threshold'] = x
            d['4) Period'] = [period[0].strftime("%Y-%m-%d"), period[1].strftime("%Y-%m-%d")]
            result = bet_market.bet(entity_days, x, period)
            i = 5
            for field in result:
                d[f'{i}) {field}'] = result[field]
                i+=1
            simulation[type].append(d)

simulation['random'] = bet_market.random_bet(period, thresholds)

print(f"> Saving simulation on {cf.result_filepath}")

save_simulation(simulation, cf.result_filepath)




