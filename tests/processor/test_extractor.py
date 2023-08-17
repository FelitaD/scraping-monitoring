import unittest
from unittest.mock import patch

from pathlib import Path
from datetime import datetime

from config import PROJECT_PATH
from processor.extractor.extractor import CollectOperationExtractor


class CollectOperationExtractorTestCase(unittest.TestCase):
    def setUp(self):
        self.from_date = datetime(2020, 12, 7)
        self.to_date = datetime(2020, 12, 8)
        self.path = Path(PROJECT_PATH / 'rooms/collect_operation_from_2020-12-07_to_2020-12-08.json')
        self.collect_data = [{'collect_uid': '07122020_20h33m58s269657_my_user', 'created_at': '2020-12-07T20:33:58.313277', 'id': 151, 'infos': {'collected_urls': ['http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/ACTUALITES/2020_ACTU/2020-loto_de_Noel.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/AMGT_SECURITE_RTE_VERTAIZON_arrete1906.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Arretes/17A13EXTEP-2-signed3332.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Arretes/Arrete_du_Maire_alcool2913.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Arretes/arrete_du_maire_neige2821.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Arretes/arrete_ralentisseur-ecole1720.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Arretes/caca-chien0611.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Brulage_de_vegetaux.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-03-10-20144420.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-04-04-20144055.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-12-12-20141152.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-14-03-20143706.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-15A232813.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-15B272331.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-15C27-1053.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-15E29-signed3721.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-15F261837.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-15G31-signed5056.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-15I23-signed3339.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-15K27mod-signed0621.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-16A291311.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-16B124743.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-16C182632.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-16D084505.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-16F011004.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-16G06-signed3021.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-16I070719.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-16J141556.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-16L021848.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-17A274924.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-17C101127.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-17D075907.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-17E18-signed5252.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-17F30-signed0034.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-17I22-signed3031.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-17J20-signed0233.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-17L082710.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-18C091522.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-18D11-definitif5929.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-18F061721.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-18G183732.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-18K165131.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-18K302339.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-18L121028.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-19-09-20144405.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-19C082738.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-19D053810.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-19E243455.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-19L04-sig3210.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-21-10-20143217.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-25-04-20144209.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-25-06-20144326.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-31-01-20143542.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM-ELECTIONS-29-03-20145240.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2014-2019/CM_08-02-190515.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2019/CM-19G240426.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2019/CM_11-10-20191851.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2019/Note_de_presentation_synthetique_du_Compte_Administratif_2018_-_CM_08-03-20192818.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2019/Note_synthese_BP_2019_CM_05-04-193925.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2019/cm-19D245011.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2020/Conseil_Municipal_12.06.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2020/Conseil_Municipal_26.05.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2020/Conseil_Municipal_du_30.09.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2020/Conseil_Municipal_du_31.01.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2020/Note_de_presentation_synthetique_BP_2020.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/CM_2020/Note_de_presentation_synthetique_du_CA_2019.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Documents_officiels/2d-BOUZEL-PLAN_D_AMENAGEMENT-PHASE_FINAL.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Documents_officiels/5-BOUZEL-plan_decomposition.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Documents_officiels/6A_Bouzel_5000e_914_800__1__compressed.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Documents_officiels/9A_reglement_ecrit_VAVJ_compressed.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Documents_officiels/BOUZEL-ASST-PLAN_N__2.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Documents_officiels/DICRIM-Fevrier_2013_WEB3616.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Menus_S45_a_47_4_jours_fixe4712.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/Menus_S48_a_51_4_jours_fixe0851.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/NOTE_SYNTHESE_BP_20170032.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/NOTE_SYNTHESE_CA_2016_CM_10-03-174803.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/PV_ELECTIONS_SENATORIALES_30-06-20170106.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/SBA/Bien_comprendre_ma_taxe_fonciere0827.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/SBA/Bouzel_calendrier_collecte_2021.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/SBA/Calendrier_collecte_Bouzel_2020.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/SBA/memo_du_tri.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/SBA_-_Information_decheteries_-_DASRI3918.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/brochure_cambriolages3203.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/A.L.S.H_Nouveautes_et_tarifs_2020-2021.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/A.L.S.H_Reglement_interieur_services_2020-2021.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Avis_WEB_a_afficher_a_la_porte_de_la_mairie_et_a_la_porte_du_cimetiere_pour_informer_les_interesses5921.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Comprendre_les_dangers_du_monoxyde_de_carbone.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Deliberation_18C09-053348.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Demande_de_raccordement_SIAREC_2020.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Guide_Reduc-Dechets_Verts-Particuliers_complet_10_fiches_conseils.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Guide_pratique_sur_la_PFAC_SIAREC_2020.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Les_infos_du_SIAREC_assainissement_06_2020.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Liste_Assistant.e.s_Maternelles.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Menu_du_14_au_18.12.2020.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/PLAN_CIMETIERE_2020.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/PV_de_constatation_etat_abandon_a_afficher_16-05-20185127.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Panneau_aire_fitness.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Planning_activites_mercredi_11_et_12.2020.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Reglement_Cimetiere_2017-signed4522.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/doc/Utilisation_agrees_aire_fitness.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/flyer-validite-cni_002_3706.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/note_synthese_BP_20180146.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/note_synthese_CA_20173640.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/ralentisseur-vertai2643.pdf', 'http://www.bouzel.fr/fileadmin/Bouzel/2_Documents/stationnement-rte-vertai0037.pdf'], 'stats': {'calendar_link_filter/agenda/skip_count': 15, 'calendar_link_filter/none/keep_count': 1645, 'downloader/request_bytes': 201803, 'downloader/request_count': 514, 'downloader/request_method_count/GET': 514, 'downloader/response_bytes': 133685218, 'downloader/response_count': 514, 'downloader/response_status_count/200': 513, 'downloader/response_status_count/403': 1, 'dupefilter/filtered': 5879, 'elapsed_time_seconds': 615.23174, 'finish_reason': 'finished', 'finish_time': '2020-12-07T20:44:13.678416', 'item_scraped_count': 105, 'log_count/DEBUG': 3017, 'log_count/ERROR': 1, 'log_count/INFO': 27, 'memusage/max': 122765312, 'memusage/startup': 87216128, 'offsite/domains': 43, 'offsite/filtered': 122, 'request_depth_max': 5, 'response_received_count': 514, 'scheduler/dequeued': 514, 'scheduler/dequeued/memory': 514, 'scheduler/enqueued': 514, 'scheduler/enqueued/memory': 514, 'sqs_push_steps/pushed_count': 105, 'start_time': '2020-12-07T20:33:58.446676'}, 'territory_uid': 'FRCOMM63049', 'triggered_by': 'crawler-worker'}, 'log_url': 'FIXME', 'status': 'success', 'territory_uid': 'FRCOMM63049', 'updated_at': '2020-12-07T20:33:58.313277', 'website': 'http://www.bouzel.fr'}, {'collect_uid': '07122020_18h12m11s354746_my_user', 'created_at': '2020-12-07T18:12:11.398115', 'id': 150, 'infos': {'triggered_by': 'crawler-worker'}, 'log_url': 'FIXME', 'status': 'running', 'territory_uid': 'FRCOMM77342', 'updated_at': '2020-12-07T18:12:11.398115', 'website': 'http://www.saintcalaisdudesert.mairie53.fr'}]

        self.CollectOperationExtractor = CollectOperationExtractor(self.from_date, self.to_date)

    def test_get_path(self):
        self.assertEqual(self.CollectOperationExtractor.get_path('2020-12-07', '2020-12-08'), self.path)

    def test_load_collects(self):
        self.assertEqual(self.CollectOperationExtractor.load_collects(self.path), self.collect_data)

    @patch('processor.extractor.extractor.PensieveBasic')
    def test_request_collects(self, mock_requests):
        mock_requests.request.return_value = self.collect_data
        assert self.CollectOperationExtractor.request_collects(self.from_date, self.to_date)[0]['collect_uid'] == '07122020_20h33m58s269657_my_user'

    @patch('processor.extractor.extractor.CollectOperationExtractor.load_collects')
    def test_retrieve_collects_existing_collect_date(self, mock_load):
        """ Test that an existing collect file is loaded. """
        self.CollectOperationExtractor.retrieve_collects(self.from_date, self.to_date)
        mock_load.assert_called_once()

    @patch('processor.extractor.extractor.CollectOperationExtractor.request_collects')
    def test_retrieve_collects(self, mock_request):
        """ Test that a new collect is resquested from Pensieve. """
        mock_request.return_value = self.collect_data
        self.assertEqual(self.CollectOperationExtractor.retrieve_collects(self.from_date, self.to_date), self.collect_data)

    @patch('processor.extractor.extractor.CollectOperationExtractor.retrieve_collects')
    def test_collect_and_consolidate(self, mock_retrieve):
        self.CollectOperationExtractor.collect_and_consolidate()
        self.assertTrue(mock_retrieve.called)

    def tearDown(self):
        self.CollectOperationExtractor = None


if __name__ == '__main__':
    unittest.main()