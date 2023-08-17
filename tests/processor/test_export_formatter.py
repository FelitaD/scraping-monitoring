import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from datetime import datetime

from processor.transformer.export_formatter import GoogleSheetFormatter
from config import PROJECT_PATH


class ExportFormatterTestCase(unittest.TestCase):

    TERRITORIES_INFOS_PATH = PROJECT_PATH / 'data' / 'commune_epci_departement.tsv'
    DEPARTMENTS_INFOS_PATH = PROJECT_PATH / 'data' / 'departements.json'
    EXPORT_FIELDS = ['Date de mise à jour', "Id de l'alerte", 'Nom département', 'Code', 'Nom', 'URL dans Pensieve',
                     'Statut de la collecte']

    def setUp(self):
        test_data = [
            ['2021-04-13 7:04:20', 'http://www.coursegoules.net', 'FRCOMM06050', 163620, 0, 'finished', 'success',
             '2021-04-29 1:45:24', 0, '13 days 00:00:00', True, False, 0],
            ['2021-04-10 21:49:44', 'http://www.mairie-saint-andre-de-corcy.fr', 'FRCOMM01333', 159792, 0, 'finished',
             'success', '2021-04-28 1:41:31', 0, '33 days 21:31:50', True, False, 0],
            ['2021-02-27 1:00:22', 'http://www.condesursuippe.fr', 'FRCOMM02211', 46474, 0, 'finished', 'success',
             '2021-04-14 1:00:14', 0, '29 days 20:00:00', True, False, 0]
        ]
        self.collects_test = pd.DataFrame(test_data, columns=['updated_at', 'website', 'territory_uid', 'id', 'item_scraped_count', 'finish_reason', 'status', 'updated_at_last', 'item_scraped_count_last', 'interval', 'is_stopped', 'was_active', 'sum_scraped'])
        self.collects_test['updated_at'] = pd.to_datetime(self.collects_test['updated_at'])
        self.collects_test['interval'] = pd.to_timedelta(self.collects_test['interval'])

        self.collects_test_add_territory = self.collects_test.copy()
        self.collects_test_add_territory['code commune'] = ['06050', '01333', '02211']
        self.collects_test_add_territory['nom commune'] = ['Coursegoules', 'Saint-André-de-Corcy', 'Condé-sur-Suippe']

        self.collects_test_add_departement = self.collects_test.copy()
        self.collects_test_add_departement['departement'] = ['Alpes-Maritimes', 'Ain', 'Aisne']

        self.collects_test_rename_columns = self.collects_test_add_territory.copy()
        self.collects_test_rename_columns['departement'] = ['Alpes-Maritimes', 'Ain', 'Aisne']

        self.ExportFormatter = GoogleSheetFormatter()

    def test_get_departements_infos(self):
        self.assertEqual(type(self.ExportFormatter.get_departements_infos()), dict)
        self.assertEqual(len(self.ExportFormatter.get_departements_infos()), 101)

    def test_get_territories_infos(self):
        self.assertEqual(self.ExportFormatter.get_territories_infos().loc[0, 'code commune'], '01001')
        self.assertEqual(self.ExportFormatter.get_territories_infos().loc[0, 'nom commune'], "L'Abergement-Clémenciat")

    def test_add_territory_name(self):
        assert_frame_equal(self.ExportFormatter.add_territory_name(self.collects_test), self.collects_test_add_territory)

    def test_add_departement(self):
        assert_frame_equal(self.ExportFormatter.add_departement(self.collects_test), self.collects_test_add_departement)

    def test_rename_columns(self):
        expected = pd.DataFrame({'Date de mise à jour': datetime.today().strftime('%Y-%m-%d'),
                                 "Id de l'alerte": '',
                                 'Nom département': ['Alpes-Maritimes', 'Ain', 'Aisne'],
                                 'Code': ['FRCOMM06050', 'FRCOMM01333', 'FRCOMM02211'],
                                 'Nom': ['Coursegoules', 'Saint-André-de-Corcy', 'Condé-sur-Suippe'],
                                 'URL dans Pensieve': ['http://www.coursegoules.net', 'http://www.mairie-saint-andre-de-corcy.fr', 'http://www.condesursuippe.fr'],
                                 'Statut de la collecte': ['finished', 'finished', 'finished']})
        assert_frame_equal(self.ExportFormatter.rename_columns(self.collects_test_rename_columns), expected)

    def tearDown(self):
        self.ExportFormatter = None


if __name__ == '__main__':
    unittest.main()
