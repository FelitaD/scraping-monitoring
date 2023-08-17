import unittest
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from processor.indicator.stopped_collect import StoppedCollectDetector


class StoppedCollectDetectorTestCase(unittest.TestCase):
    def setUp(self):
        test_data = [
            ['2021-03-31 17:00:51', 'http://www.coursegoules.net', 'FRCOMM06050', 131854, 34, 'finished', 'success'],
            ['2021-04-13 7:04:20', 'http://www.coursegoules.net', 'FRCOMM06050', 163620, 0, 'finished', 'success'],
            ['2021-04-15 1:00:40', 'http://www.coursegoules.net', 'FRCOMM06050', 166874, 0, 'finished', 'success'],
            ['2021-04-22 1:01:47', 'http://www.coursegoules.net', 'FRCOMM06050', 184130, 0, 'finished', 'success'],
            ['2021-04-29 1:45:24', 'http://www.coursegoules.net', 'FRCOMM06050', 201720, 0, 'finished', 'success'],
            ['2021-03-23 3:06:40', 'http://www.ars-village.fr', 'FRCOMM01021', 96911, 0, 'finished', 'success'],
            ['2021-03-25 4:06:29', 'http://www.ars-village.fr', 'FRCOMM01021', 105715, 0, 'finished', 'success'],
            ['2021-03-27 2:17:05', 'http://www.ars-village.fr', 'FRCOMM01021', 114460, 0, 'finished', 'success'],
            ['2021-04-28 1:38:19', 'https://www.ars-sur-formans.fr/', 'FRCOMM01021', 198433, 15, 'finished', 'success'],
            ['2021-02-12 22:06:20', 'http://www.clacyetthierret.fr', 'FRCOMM02196', 26207, 0, 'finished', 'success']
        ]
        self.collects_test = pd.DataFrame(test_data,
                                          columns=['updated_at', 'website', 'territory_uid', 'id', 'item_scraped_count',
                                                   'finish_reason', 'status'])
        self.collects_test['updated_at'] = pd.to_datetime(self.collects_test['updated_at'])

        self.StoppedCollectDetector = StoppedCollectDetector(self.collects_test)
        self.StoppedCollectDetector.process()

        self.collects_test_get_pairs_with_last_collect = self.collects_test.copy()
        self.collects_test_get_pairs_with_last_collect.drop([9], axis=0, inplace=True)
        self.collects_test_get_pairs_with_last_collect['updated_at_last'] = np.nan
        self.collects_test_get_pairs_with_last_collect['updated_at_last'][0:5] = '2021-04-29 01:45:24'
        self.collects_test_get_pairs_with_last_collect['updated_at_last'][5:9] = '2021-04-28 01:38:19'
        self.collects_test_get_pairs_with_last_collect['updated_at_last'] = pd.to_datetime(
            self.collects_test_get_pairs_with_last_collect['updated_at_last'])
        self.collects_test_get_pairs_with_last_collect['item_scraped_count_last'] = [0, 0, 0, 0, 0, 15, 15, 15, 15]

        self.collects_test_get_processed_pairs = self.collects_test_get_pairs_with_last_collect.copy()
        self.collects_test_get_processed_pairs['interval'] = ['28 days 08:44:33', '15 days 18:41:04',
                                                              '14 days 00:44:44', '7 days 00:43:37', '0 days 00:00:00',
                                                              '35 days 22:31:39', '33 days 21:31:50',
                                                              '31 days 23:21:14', '0 days 00:00:00']
        self.collects_test_get_processed_pairs['interval'] = pd.to_timedelta(
            self.collects_test_get_processed_pairs['interval'])
        self.collects_test_get_processed_pairs['is_stopped'] = [True, True, True, True, True, False, False, False,
                                                                False]
        self.collects_test_get_processed_pairs['was_active'] = [True, False, False, False, False, False, False, False,
                                                                True]

        self.collects_test_get_sum_scraped_currently_stopped = self.collects_test_get_processed_pairs.copy()
        self.collects_test_get_sum_scraped_currently_stopped.drop([5, 6, 7, 8], axis=0, inplace=True)
        self.collects_test_get_sum_scraped_currently_stopped.sort_values(['updated_at'], ascending=False, inplace=True)
        self.collects_test_get_sum_scraped_currently_stopped['sum_scrapped'] = [0, 0, 0, 0, 34]

    def test_filter_insufficient_collects(self):
        self.assertEqual(len(self.StoppedCollectDetector.collect_data), 9)

    def test_get_pairs_with_last_collect(self):
        assert_frame_equal(self.StoppedCollectDetector.pairs, self.collects_test_get_pairs_with_last_collect)

    def test_get_processed_pairs(self):
        assert_frame_equal(self.StoppedCollectDetector.processed_pairs, self.collects_test_get_processed_pairs)

    def test_get_sum_scraped_currently_stopped(self):
        assert_frame_equal(self.StoppedCollectDetector.currently_stopped,
                           self.collects_test_get_sum_scraped_currently_stopped)

    def test_get_ever_active(self):
        expected = pd.Series({0: 'FRCOMM06050'}, name='territory_uid')
        assert_series_equal(self.StoppedCollectDetector.ever_active_uids, expected)

    def test_get_candidates(self):
        assert_frame_equal(self.StoppedCollectDetector.candidates, self.collects_test_get_sum_scraped_currently_stopped.loc[[1]])

    def test_compute_timedelta(self):
        test_data = pd.DataFrame({'updated_at': ['2020-01-01', '2020-01-01'],
                                  'updated_at_last': ['2020-01-01', '2020-01-02']},
                                 dtype='datetime64[ns]')
        expected = pd.Series({0: '0 days', 1: '1 days'}, dtype='timedelta64[ns]')
        assert_series_equal(self.StoppedCollectDetector._compute_timedelta(test_data), expected)

    def test_is_collect_stopped(self):
        test_data = pd.DataFrame({'item_scraped_count_last': [0, 2, 56, 0]})
        expected = pd.Series(np.array([True, False, False, True]), name='item_scraped_count_last')
        assert_series_equal(self.StoppedCollectDetector._is_collect_stopped(test_data), expected)

    def test_was_active(self):
        test_data = pd.DataFrame({'item_scraped_count': [0, 2, 56, 0]})
        expected = pd.Series(np.array([False, False, True, False]), name='item_scraped_count')
        assert_series_equal(self.StoppedCollectDetector._was_active(test_data), expected)

    def tearDown(self):
        self.StoppedCollectDetector = None


if __name__ == '__main__':
    unittest.main()
