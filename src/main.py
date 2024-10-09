from pathlib import Path
import os

from json_repair import json_repair

from src.data.adhoc_processing import *
from src.data.data_cleaning import *
from src.data.data_loader import *
from src.logger import logger

# I put this constant like that but it could be on the config.py file
# where we have all the environment and where the file are maybe on GCS , S3 or other

fields_to_keep = ['journal', 'journal_type', 'id', 'drug', 'mention_date']
pubmed_jon_path = Path("data/raw/pubmed.json")
pubmed_csv_path = Path("data/raw/pubmed.csv")
clinical_csv_path = Path("data/raw/clinical_trials.csv")
drugs_csv_path = Path("data/raw/drugs.csv")
json_graph_path = Path("data/cleaned/clinical_pubmed_drug.json")

pl.Config.set_fmt_str_lengths(900)
pl.Config.set_tbl_width_chars(900)


def data_pipeline():
    logger.info("start data pipeline processing")
    logger.info("start data reading and merge the pubmed csv and json file")
    logger.info("start reading clinical pubmed and drugs files to create pandas DataFrame")
    clinical_df = csv_reader(clinical_csv_path)
    drugs_df = csv_reader(drugs_csv_path)
    pubmed_csv_df = csv_reader(pubmed_csv_path)
    pubmed_json_df = pd.DataFrame.from_dict(json_repair.from_file(pubmed_jon_path))

    logger.info("start merging drug csv and json dataframe ")
    pubmed_df = pd.concat([pubmed_csv_df, pubmed_json_df])

    logger.info("start cleaning the dataframes ")

    # date cleaning
    clinical_df = date_cleaning(clinical_df)
    pubmed_df = date_cleaning(pubmed_df)

    # remove weird character with remove_weird_char
    field_to_normalize = "journal"
    clinical_df = remove_weird_char(clinical_df, field_to_normalize)

    pubmed_df = remove_weird_char(pubmed_df, field_to_normalize)

    # rename scientific_title to title on clinical df
    clinical_df = rename_title(clinical_df)

    # add journal type field on clinical and pubmed DataFrame
    clinical_df["journal_type"] = "clinical"
    pubmed_df["journal_type"] = "pubmed"

    logger.info("concat clinical and pubmed dataframe")
    all_journal = pd.concat([clinical_df, pubmed_df])

    logger.info("join  clinical_pubmed dataframe with drugs dataframe")
    journal_drug_df = merge_df(all_journal, drugs_df, fields_to_keep)

    logger.info("save the graph link to json")
    save_to_json(journal_drug_df, json_graph_path, fields_to_keep)

    fd = ["journal", "drug"]
    df = read_json(json_graph_path)
    logger.info("show the most drugs by journal")
    print(most_journal_by_drug(df, fd, "journal"))
    logger.info("list of drug by pubmed journal")
    print(pubmed_drug(df, "journal_type", "pubmed"))


if __name__ == '__main__':
    data_pipeline()
