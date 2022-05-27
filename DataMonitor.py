import time
import sys
import datetime
import pymongo

from helpers.misc import load_config


def progress_bar(i: int, total: int, post_text: str, n: int = 10) -> None:
    """
    :param i: Current progress tick as int
    :param total: Total progress ticks as int
    :param post_text: Text to append to progress bar
    :param n: Length of bar in numbers of characters
    :rtype: None
    """
    n_bar = n
    j = i / total
    sys.stdout.write("\r")
    sys.stdout.write(f"[{'=' * int(n_bar * j):{n_bar}s}] {int(100 * j)}%  {post_text}")
    sys.stdout.flush()


class DataMonitor:
    RUN_INTERVAL = 60

    def __init__(self, db_string: str) -> None:
        """
        :param db_string: MongoDB connection string
        :rtype: None
        """
        self.db_string = db_string
        self.client = pymongo.MongoClient(db_string)
        self.data_db = self.client["tidepool"]
        self.stats_db = self.client["tidepool-stats"]

    def get_instruments(self) -> list:
        """
        :rtype: list
        """
        collections = self.data_db.list_collection_names()
        collections.remove("raw")
        return collections

    def data_count(self, instrument: str) -> int:
        """
        :param instrument: Instrument to get data count of
        :return: Returns the count of datapoints as an int
        :rtype: int
        """
        collection = self.data_db[instrument]
        return collection.count_documents({})

    def total_data_count(self) -> int:
        """
        :rtype: int
        :return: Returns the total number of data points in database
        """
        return self.data_count("raw")

    def profile_instrument(self, time_span: int, instrument: str) -> float:
        """
        :param time_span: Time span to profile instrument for
        :param instrument: Instrument to profile
        :return: Returns a data speed as a float
        :rtype: float
        """
        initial = self.data_count(instrument)
        time.sleep(time_span)
        gain = self.data_count(instrument) - initial

        return gain / time_span

    def profile_total(self, time_span: int) -> dict:
        """
        :param time_span: Time span to profile instruments for
        :return: Returns dictionary with keys instrument and values data speeds
        :rtype: dict
        """
        print("Gathering intial data...", end="")
        initial = {}
        for instrument in self.get_instruments():
            initial[instrument] = self.data_count(instrument)

        initial["raw"] = self.total_data_count()
        print("done")

        for i in range(time_span):
            progress_bar(i + 1, time_span, "Measuring data speeds...")
            time.sleep(1)
        print("done")

        print("Gathering new data...", end="")
        gain = {}
        for instrument in initial:
            gain[instrument] = self.data_count(instrument) - initial[instrument]

        speeds = {instrument: gain[instrument] / time_span for instrument in gain}
        print("done")
        return speeds

    def log_profile(self, profile: dict) -> None:
        """
        :param profile: Dictionary containing instrument name keys and data speed values
        :rtype: None
        """
        latest = self.stats_db["latest"]
        historical = self.stats_db["historical"]
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        for key, value in profile.items():
            stats = {
                "instrument": key,
                "data_rate": value,
                "count": self.data_count(key),
                "timestamp": timestamp,
            }
            latest.update_one({"instrument": key}, {"$set": stats}, upsert=True)
            historical.insert_one(stats)

    def monitor(self) -> None:
        """
        :rtype: None
        """
        while True:
            profile = self.profile_total(DataMonitor.RUN_INTERVAL)
            self.log_profile(profile)
            print(f"Profiled {len(profile.keys())} currency pairs.")


if __name__ == "__main__":
    cfg = load_config()
    db_string = cfg["db_string"]
    dm = DataMonitor(db_string)
    dm.monitor()
