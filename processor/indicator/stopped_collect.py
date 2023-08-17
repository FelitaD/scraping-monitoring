from datetime import timedelta

from config.definitions import CollectFields


class StoppedCollectDetector:

    ACTIVE_COLLECT_TRESHOLD = 9

    def __init__(self, collects):
        self.collect_data = self.filter_insufficient_collects(collects, 2)
        self.pairs = None
        self.processed_pairs = None
        self.currently_stopped = None
        self.ever_active_uids = None
        self.candidates = None

    def process(self):
        self.get_pairs_with_last_collect()
        self.get_processed_pairs()
        self.get_sum_scraped_currently_stopped()
        self.get_ever_active()
        self.get_candidates(last_active_day_treshold=12)

    @staticmethod
    def filter_insufficient_collects(collects, minimal_collect_count):
        """Exclude collects from territories where the total number of collect is less than minimal_collect_count."""
        return collects.groupby(
            CollectFields.territory_uid
        ).filter(lambda x: x[CollectFields.id].count() >= minimal_collect_count)

    def get_pairs_with_last_collect(self):
        """Pair  each  collect with the last one on a given territory"""
        last_collects = self.collect_data.loc[self.collect_data.groupby(['territory_uid'])["updated_at"].idxmax()]
        self.pairs = self.collect_data.merge(last_collects, on=[CollectFields.territory_uid], suffixes=('', '_last'))
        return self.pairs.drop(['website_last', 'id_last', 'finish_reason_last', 'status_last'], axis=1, inplace=True)

    def get_processed_pairs(self):
        """Creates columns based on a collect's info and/or the last associated collect"""
        self.processed_pairs = self.pairs.copy()
        self.processed_pairs["interval"] = self.processed_pairs.apply(lambda x: self._compute_timedelta(x), axis=1)
        self.processed_pairs["is_stopped"] = self.processed_pairs.apply(lambda x: self._is_collect_stopped(x), axis=1)
        self.processed_pairs["was_active"] = self.processed_pairs.apply(lambda x: self._was_active(x, ACTIVE_COLLECT_TRESHOLD=9), axis=1)
        return self.processed_pairs

    def get_sum_scraped_currently_stopped(self):
        """Calculates the cumulative sum of items scraped for each territory where the last collect is stopped"""
        self.currently_stopped = self.processed_pairs[self.processed_pairs["is_stopped"]]
        self.currently_stopped = self.currently_stopped.sort_values(["territory_uid", "updated_at"], ascending=False)
        self.currently_stopped["sum_scrapped"] = self.currently_stopped.groupby(["territory_uid"])[
            "item_scraped_count"].cumsum()
        return self.currently_stopped

    def get_ever_active(self):
        """Extracts territorry uids where at least one collect was active."""
        ever_active = self.currently_stopped.groupby("territory_uid")["was_active"].agg(lambda x: any(x)).reset_index()
        self.ever_active_uids = ever_active[ever_active["was_active"]]["territory_uid"]
        return self.ever_active_uids

    def get_candidates(self, last_active_day_treshold=12):
        """Combining Active collect, no collect inbetween, and interval between collects above treshold."""

        # - We only want to keep collects that have been active at some point.
        # - If the sum of scrapped document is 0 between two collects, that means that the collect has been inactive
        # in-between.
        # - Interval must be above a certain treshold.
        stopped_collect = self.currently_stopped[(self.currently_stopped["territory_uid"].isin(self.ever_active_uids)) &
                                                 (self.currently_stopped["sum_scrapped"] == 0) &
                                                 (self.currently_stopped["interval"] >= timedelta(last_active_day_treshold))]
        # We keep only the furthest inactive collect.
        self.candidates = stopped_collect.loc[stopped_collect.groupby(["territory_uid"])["interval"].idxmax()]
        return self.candidates

    @staticmethod
    def _compute_timedelta(x):
        return x["updated_at_last"] - x["updated_at"]

    @staticmethod
    def _is_collect_stopped(x):
        return x["item_scraped_count_last"] == 0

    @staticmethod
    def _was_active(x, ACTIVE_COLLECT_TRESHOLD=None):
        return x["item_scraped_count"] > ACTIVE_COLLECT_TRESHOLD
