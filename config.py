import os, datetime

class Config:

    def __init__(self, version):
        self.version = version
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.financial_market_path = os.path.join(self.base_dir, "csv/Microsoft/msft_1d.csv")

    def set_up(self):
        self.period_train = {'start':'2017-01-01', 'end': '2018-01-01'}
        self.period_test = {'start':'2018-01-01', 'end':'2019-01-01'}
        start = datetime.datetime(2014, 1, 1)
        end = datetime.datetime(2018, 1, 1)
        n_year = 4
        self.period_datetime = (start, end)
        self.thresholds = [0.5, 0.75, 1, 1.25]
        self.top = [10]
        self.min_doc_count = 25*n_year
        self.result_filepath = os.path.join(self.base_dir, f'''results/
            {self.period_train["start"]}-{self.period_train["end"]}_prediction_power_evaluation_{self.version}.xlsx''')
