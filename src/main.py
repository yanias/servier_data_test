import logging
from logging import StreamHandler
import sys

from src.data.data_cleaning import *
from src.data.data_reader import csv_reader

logger = logging.getLogger('DATA_PIPELINE')
logger.setLevel(logging.INFO)
ch = StreamHandler(stream=sys.stdout)
ch.setLevel(level=logging.INFO)
formatter = logging.\
    Formatter('[%(name)s - %(asctime)s - %(levelname)s] => %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

fields_to_keep = ['drug', 'date', 'journal', "journal_type"]


def data_pipeline():
    logger.info("start data pipeline test")
    clinical_df = csv_reader("datas/raw/clinical_trials.csv")
    drugs_df = csv_reader("datas/raw/drugs.csv")
    pubmed_df = csv_reader("datas/raw/pubmed.csv")
    logger.info("save to json file")
    json_path = "datas/cleaned/clinical_pubmed_drug.json"
    test_json_path = "../tests/datas/cleaned/clinical_pubmed_drug.json"
    save_to_json(date_cleaning(clinical_df), date_cleaning(pubmed_df),
                 drugs_df, json_path, fields_to_keep)

    fd = ["journal", "drug"]
    df = read_json(json_path)
    logger.info("show the most drugs by journal")
    print(most_journal_by_drug(df, fd, "journal"))
    print(pubmed_drug(df, "journal_type", "pubmed"))


if __name__ == '__main__':
    data_pipeline()

