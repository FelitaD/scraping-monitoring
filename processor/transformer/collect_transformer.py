import pandas as pd

from config.definitions import CollectFields


class CollectTransformer:
    """This class aims at structuring a list of dictionaries describing collect operations."""

    COLS_TO_KEEP = [
        CollectFields.updated_at,
        CollectFields.website,
        CollectFields.territory_uid,
        CollectFields.id,
        CollectFields.item_scraped_count,
        CollectFields.finish_reason,
        CollectFields.status,
    ]
    COLLECT_ORDERING = [
        CollectFields.territory_uid,
        CollectFields.updated_at,
        CollectFields.id,
    ]

    def __init__(self):
        self.collect_data = None

    def load_and_transform(self, data):
        self.collect_data = self.load(data)
        self.collect_data[CollectFields.updated_at] = pd.to_datetime(self.collect_data[CollectFields.updated_at])
        return self.transform(self.collect_data)

    @staticmethod
    def load(data):
        return pd.DataFrame(data)

    @classmethod
    def transform(cls, collects):
        collects[CollectFields.item_scraped_count] = collects.apply(
            lambda x: cls.get_items_scraped_count(x), axis=1
        )
        collects[CollectFields.finish_reason] = collects.apply(
            lambda x: cls.get_finish_reason(x), axis=1
        )
        collects = collects.filter(items=cls.COLS_TO_KEEP)
        collects = collects[collects['status'] == 'success']
        collects.sort_values(by=cls.COLLECT_ORDERING, inplace=True)
        return collects

    @staticmethod
    def get_items_scraped_count(x):
        try:
            return x["infos"]["stats"]["item_scraped_count"]
        except KeyError:
            return 0  # No item scraped count attribute, return 0

    @staticmethod
    def get_finish_reason(x):
        try:
            return x["infos"]["stats"]["finish_reason"]
        except KeyError:
            return pd.NA



