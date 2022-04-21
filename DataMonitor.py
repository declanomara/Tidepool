import time
import pprint
from datetime import datetime

import pymongo

from helpers import load_config


class DataMonitor:
    def __init__(self, db_string):
        self.db_string = db_string
        self.client = pymongo.MongoClient(db_string)
        self.data_db = self.client["tidepool"]
        self.stats_db = self.client["tidepool-stats"]

    def get_instruments(self):
        collections = self.data_db.list_collection_names()
        collections.remove("raw")
        return collections

    def data_count(self, instrument):
        collection = self.data_db[instrument]
        return collection.count_documents({})

    def total_data_count(self):
        return self.data_count("raw")

    def profile_instrument(self, time_span, instrument):
        initial = self.data_count(instrument)
        time.sleep(time_span)
        gain = self.data_count(instrument) - initial

        return gain / time_span

    def profile_total(self, time_span):
        initial = {}
        for instrument in self.get_instruments():
            initial[instrument] = self.data_count(instrument)

        initial["raw"] = self.total_data_count()

        time.sleep(time_span)

        gain = {}
        for instrument in initial:
            gain[instrument] = self.data_count(instrument) - initial[instrument]

        speeds = {instrument: gain[instrument] / time_span for instrument in gain}
        return speeds

    def log_profile(self, profile):
        latest = self.stats_db['latest']
        historical = self.stats_db['historical']
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        for key, value in profile.items():
            stats = {'type': 'data_speed', 'instrument': key, 'data_rate': value, 'timestamp': timestamp}
            latest.update_one({'instrument': key}, {'$set': stats}, upsert=True)
            historical.insert_one(stats)

    def monitor(self):
        while True:
            profile = self.profile_total(10)
            self.log_profile(profile)
            print(f'Profiled {len(profile.keys())} currency pairs.')


if __name__ == "__main__":
    cfg = load_config()
    db_string = cfg['db_string']
    dm = DataMonitor(db_string)
    dm.monitor()
