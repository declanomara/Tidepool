import time
import pprint
import pymongo


class DataMonitor:
    def __init__(self, db_string):
        self.db_string = db_string
        self.client = pymongo.MongoClient(db_string)
        self.db = self.client["tidepool"]

    def get_instruments(self):
        collections = self.db.list_collection_names()
        collections.remove("raw")
        return collections

    def data_count(self, instrument):
        collection = self.db[instrument]
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

    def monitor(self):
        while True:
            pprint.pprint(self.profile_total(1))


if __name__ == "__main__":
    db_string = "localhost:27017"
    dm = DataMonitor(db_string)
    dm.monitor()
