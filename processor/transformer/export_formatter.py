from datetime import datetime
import json
import pandas as pd
import boto3
from pathlib import Path

from config import PROJECT_PATH


class GoogleSheetFormatter:
    """Format dataframe into google sheet desired format."""
    EXPORT_FIELDS = ['Date de mise à jour', "Id de l'alerte", 'Nom département', 'Code', 'Nom', 'URL dans Pensieve',
                     'Statut de la collecte']

    TERRITORIES_INFOS_PATH = PROJECT_PATH / 'data' / 'commune_epci_departement.tsv'

    DEPARTMENTS_INFOS_PATH = PROJECT_PATH / 'data' / 'departements.json'

    def __init__(self):
        self.collect_data = None
        self.departements_infos = self.get_departements_infos()
        self.territories_infos = self.get_territories_infos()

    @classmethod
    def get_departements_infos(cls):
        with cls.DEPARTMENTS_INFOS_PATH.open('r') as f:
            return json.load(f)

    @classmethod
    def get_territories_infos(cls):
        territories_infos = pd.read_csv(cls.TERRITORIES_INFOS_PATH, sep='\t')
        return territories_infos[['code commune', 'nom commune']]

    def format_for_export(self, collects):
        """Keep the most recent collect when there is more than one by territory that passed the check and renames relevant columns."""
        self.add_territory_name(collects)
        self.add_departement(collects)
        self.rename_columns(collects)
        return collects

    def add_territory_name(self, collects):
        collects['code commune'] = collects['territory_uid'].apply(lambda x: x[6:])
        return collects.merge(self.territories_infos, on='code commune')

    def add_departement(self, collects):
        collects["departement"] = collects["territory_uid"].apply(
            lambda x: self.departements_infos.get(x[6:8]) if x[:6] != 'FREPCI' else None)
        return collects

    def rename_columns(self, collects):
        collects["Id de l'alerte"] = ""
        collects['Date de mise à jour'] = datetime.today().strftime('%Y-%m-%d')

        collects.rename(columns={
            'departement': 'Nom département',
            'territory_uid': 'Code',
            'nom commune': 'Nom',
            'website': 'URL dans Pensieve',
            'finish_reason': 'Statut de la collecte'
        }, inplace=True)

        return collects[self.EXPORT_FIELDS]


class AWSExporter:
    """Export structured collects in bucket S3."""
    s3 = boto3.resource('s3')
    BUCKET = 's3-prd-raw-files'
    path = Path(PROJECT_PATH, 'data/structured_collects_monitoring.csv')

    def __init__(self, collects):
        self.collects = collects

    def write_and_upload_file(self):
        self.write_file()
        self.s3.Bucket(self.BUCKET).upload_file(str(self.path), 'collecte_monitoring/structured_collects.csv')

    def write_file(self):
        self.collects.to_csv(self.path)
