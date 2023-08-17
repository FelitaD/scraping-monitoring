from argparse import ArgumentParser
from datetime import datetime, timedelta
import pandas as pd

from gspread_dataframe import get_as_dataframe, set_with_dataframe

from config import logger
from processor.extractor.extractor import CollectOperationExtractor
from processor.transformer.collect_transformer import CollectTransformer
from processor.transformer.export_formatter import GoogleSheetFormatter, AWSExporter
from processor.indicator.stopped_collect import StoppedCollectDetector
from helpers.interface.gsheet_interface import GoogleSheetInterface


def parse_arguments():
    """Parse arguments from console."""
    parser = ArgumentParser()
    parser.add_argument('--days', '-d', type=int, default=45, help="Number of days to include in computation.")
    parser.add_argument('--gsheet', '-g', type=str, default='Suivi de la collecte des docs admins - Ancien', help="Name of google sheet.")
    parser.add_argument('--sheet', '-s', type=str, default='Collectes_en_échec', help="Sheet size to copy data in.")
    return parser.parse_args()


def main(args):
    gsheet = GoogleSheetInterface.client.open(args.gsheet)
    sheet = gsheet.worksheet(args.sheet)

    logger.info('-------------- Loading data --------------------')
    from_date = datetime.today() - timedelta(args.days)
    to_date = datetime.today() - timedelta(2) # We don't want to collect today's collect because it might not be finished
    collect_operations = CollectOperationExtractor(from_date=from_date.strftime('%Y-%m-%d'),
                                                   to_date=to_date.strftime('%Y-%m-%d'))
    collect_operations.collect_and_consolidate()

    logger.info('-------------- Structuring the response data ---')
    structured_collects = CollectTransformer().load_and_transform(collect_operations.collect_data)
    logger.info(f'{len(structured_collects)} rows found')

    aws_exporter = AWSExporter(structured_collects)
    aws_exporter.write_and_upload_file()

    collects = StoppedCollectDetector(structured_collects)
    collects.process()
    last_stopped_collects = collects.candidates

    logger.info('-------------- Formatting passed collects -------')
    formatted_collects = GoogleSheetFormatter().format_for_export(last_stopped_collects)

    logger.info('-------------- Exporting to Google Sheet --------')
    existing = get_as_dataframe(sheet, usecols=GoogleSheetFormatter.EXPORT_FIELDS)
    updated = formatted_collects.append(existing)
    updated.drop_duplicates(subset=['Code'], inplace=True)
    updated.sort_values(by='Date de mise à jour', ascending=False, inplace=True)
    set_with_dataframe(sheet, updated, row=2, include_column_header=False)


if __name__ == '__main__':
    args = parse_arguments()
    main(args)
