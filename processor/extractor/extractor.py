from datetime import timedelta
from pathlib import Path
import json
import pandas as pd

from config import logger, PROJECT_PATH
from helpers.interface.pensieve_interface import PensieveBasic


class CollectOperationExtractor:
    """Extracts collects within a specified date range."""

    def __init__(self, from_date=None, to_date=None):
        self.from_date = from_date
        self.to_date = to_date
        self.pensieve_interface = PensieveBasic
        self.collect_data = None
        self.logger = logger
        # pas sur s'il faut retourner un self.path

    def collect_and_consolidate(self):
        """Consolidate collects from date range into a list"""
        collects = []
        self.logger.info("---- Retrieving data -----")
        for collect_date in pd.date_range(self.from_date, self.to_date):
            collects += self.retrieve_collects(from_date=collect_date.strftime('%Y-%m-%d'),
                                               to_date=(collect_date + timedelta(1)).strftime('%Y-%m-%d'))
        self.collect_data = collects

    def retrieve_collects(self, from_date, to_date):
        """Retrieve collects from Pensieve."""
        collect_path = self.get_path(from_date, to_date)
        if collect_path.exists():
            self.logger.info(f"... data for {from_date} already exists, loading ...")
            collect_data = self.load_collects(collect_path)
        else:
            self.logger.info(f"... data  for {from_date} needs to be downloaded ...")
            collect_data = self.request_collects(from_date, to_date)
            json.dump(collect_data, collect_path.open('w'))
        return collect_data

    @staticmethod
    def get_path(from_date, to_date):
        return Path(PROJECT_PATH / f'rooms/collect_operation_from_{from_date}_to_{to_date}.json')

    @staticmethod
    def load_collects(collect_path):
        return json.load(collect_path.open('r'))

    @staticmethod
    def request_collects(from_date, to_date):
        return PensieveBasic.request('/admin_docs_collect_operations/by_collect_date', method='GET',
                                     from_collect_date=from_date, to_collect_date=to_date)
