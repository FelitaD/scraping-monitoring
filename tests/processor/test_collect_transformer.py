import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

from processor.transformer.collect_transformer import CollectTransformer


class CollectTransformerTestCase(unittest.TestCase):
    COLS_TO_KEEP = ['updated_at', 'website', 'territory_uid', 'id', 'item_scraped_count', 'finish_reason', 'status']
    COLLECT_ORDERING = ['territory_uid', 'updated_at', 'id']

    def setUp(self):
        self.CollectTransformer = CollectTransformer()
        # raw_collect_data : 1 completed and 1 running collects
        self.raw_collect_data = [
            {
                "collect_uid": "07122020_20h33m58s269657_my_user",
                "created_at": "2020-12-07T20:33:58.313277",
                "id": 151,
                "infos": {
                    "collected_urls": [
                        "http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/note_synthese_CA_20173640.pdf",
                        "http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/ralentisseur-vertai2643.pdf",
                        "http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/stationnement-rte-vertai0037.pdf"
                    ],
                    "stats": {
                        "calendar_link_filter/agenda/skip_count": 15,
                        "calendar_link_filter/none/keep_count": 1645,
                        "downloader/request_bytes": 201803,
                        "downloader/request_count": 514,
                        "downloader/request_method_count/GET": 514,
                        "downloader/response_bytes": 133685218,
                        "downloader/response_count": 514,
                        "downloader/response_status_count/200": 513,
                        "downloader/response_status_count/403": 1,
                        "dupefilter/filtered": 5879,
                        "elapsed_time_seconds": 615.23174,
                        "finish_reason": "finished",
                        "finish_time": "2020-12-07T20:44:13.678416",
                        "item_scraped_count": 105,
                        "log_count/DEBUG": 3017,
                        "log_count/ERROR": 1,
                        "log_count/INFO": 27,
                        "memusage/max": 122765312,
                        "memusage/startup": 87216128,
                        "offsite/domains": 43,
                        "offsite/filtered": 122,
                        "request_depth_max": 5,
                        "response_received_count": 514,
                        "scheduler/dequeued": 514,
                        "scheduler/dequeued/memory": 514,
                        "scheduler/enqueued": 514,
                        "scheduler/enqueued/memory": 514,
                        "sqs_push_steps/pushed_count": 105,
                        "start_time": "2020-12-07T20:33:58.446676"
                    },
                    "territory_uid": "FRCOMM63049",
                    "triggered_by": "crawler-worker"
                },
                "log_url": "FIXME",
                "status": "success",
                "territory_uid": "FRCOMM63049",
                "updated_at": "2020-12-07T20:33:58.313277",
                "website": "http://www.bouzel.fr"
            },
            {
                "collect_uid": "07122020_18h12m11s354746_my_user",
                "created_at": "2020-12-07T18:12:11.398115",
                "id": 150,
                "infos": {
                    "triggered_by": "crawler-worker"
                },
                "log_url": "FIXME",
                "status": "running",
                "territory_uid": "FRCOMM77342",
                "updated_at": "2020-12-07T18:12:11.398115",
                "website": "http://www.saintcalaisdudesert.mairie53.fr"
            }
        ]

        self.expected_load = pd.DataFrame(self.raw_collect_data)

        self.expected_transform = self.expected_load.copy()
        self.expected_transform['item_scraped_count'] = [105, 0]
        self.expected_transform['finish_reason'] = ['finished', 0]
        self.expected_transform = self.expected_transform[self.COLS_TO_KEEP]
        self.expected_transform.drop([1], axis=0, inplace=True)
        self.expected_transform = self.expected_transform[self.expected_transform['status'] == 'success']
        self.expected_transform.sort_values(by=self.COLLECT_ORDERING, inplace=True)

        self.expected_load_and_transform = self.expected_transform.copy()
        self.expected_load_and_transform['updated_at'] = pd.to_datetime(self.expected_load_and_transform['updated_at'])

    def test_load(self):
        assert_frame_equal(self.CollectTransformer.load(self.raw_collect_data), self.expected_load)

    def test_transform(self):
        assert_frame_equal(self.CollectTransformer.transform(self.expected_load), self.expected_transform)

    def test_load_and_transform(self):
        assert_frame_equal(self.CollectTransformer.load_and_transform(self.raw_collect_data), self.expected_load_and_transform)

    def test_get_item_scraped_count(self):
        self.assertEqual(self.CollectTransformer.get_items_scraped_count(self.raw_collect_data[0]), 105)

    def test_get_item_scraped_count_when_no_pushed_count(self):
        self.assertEqual(self.CollectTransformer.get_items_scraped_count(self.raw_collect_data[1]), 0)

    def test_get_finish_reason(self):
        self.assertEqual(self.CollectTransformer.get_finish_reason(self.raw_collect_data[0]), 'finished')

    def test_get_finish_reason_when_not_finished(self):
        self.assertTrue(pd.isna(self.CollectTransformer.get_finish_reason(self.raw_collect_data[1])))

    def tearDown(self):
        self.CollectTransformer = None


if __name__ == '__main__':
    unittest.main()
