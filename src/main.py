from pathlib import Path
import os
from json_repair import json_repair
from src.data.adhoc_processing import *
from src.data.data_cleaning import *
from src.data.data_loader import *
from src.logger import logger

# Constants: These could be moved to a config.py file, 
# where all environment settings and file paths are managed 
# (e.g., GCS, S3, or local file system paths).
FIELDS_TO_KEEP = ["journal", "journal_type", "id", "drug", "mention_date"]
PUBMED_JSON_PATH = Path("data/raw/pubmed.json")
PUBMED_CSV_PATH = Path("data/raw/pubmed.csv")
CLINICAL_CSV_PATH = Path("data/raw/clinical_trials.csv")
DRUGS_CSV_PATH = Path("data/raw/drugs.csv")
JSON_GRAPH_PATH = Path("data/cleaned/clinical_pubmed_drug.json")

# Configure polars display settings for tables and string lengths
pl.Config.set_fmt_str_lengths(900)
pl.Config.set_tbl_width_chars(900)


def data_pipeline():
    """Main data pipeline function."""

    # Logging the start of the pipeline process
    logger.info("Start data pipeline processing")
    logger.info("Start reading and merging the PubMed CSV and JSON files")

    # Reading clinical, drugs, and pubmed data from CSV and JSON files
    clinical_df = csv_reader(CLINICAL_CSV_PATH)
    drugs_df = csv_reader(DRUGS_CSV_PATH)
    pubmed_csv_df = csv_reader(PUBMED_CSV_PATH)
    pubmed_json_df = pd.DataFrame.from_dict(json_repair.from_file(PUBMED_JSON_PATH))

    logger.info("Start merging drug CSV and JSON dataframes")
    pubmed_df = pd.concat([pubmed_csv_df, pubmed_json_df])

    logger.info("Start cleaning the dataframes")

    # Data cleaning
    clinical_df = date_cleaning(clinical_df)
    pubmed_df = date_cleaning(pubmed_df)

    # Remove weird characters from 'journal' field
    field_to_normalize = "journal"
    clinical_df = remove_weird_char(clinical_df, field_to_normalize)
    pubmed_df = remove_weird_char(pubmed_df, field_to_normalize)

    # Rename 'scientific_title' to 'title' on the clinical dataframe
    clinical_df = rename_title(clinical_df)

    # Add journal type field to both clinical and pubmed dataframes
    clinical_df["journal_type"] = "clinical"
    pubmed_df["journal_type"] = "pubmed"

    logger.info("Concatenate clinical and pubmed dataframes")
    all_journal = pd.concat([clinical_df, pubmed_df])

    logger.info("Join clinical_pubmed dataframe with drugs dataframe")
    journal_drug_df = merge_df(all_journal, drugs_df, FIELDS_TO_KEEP)

    # Transform data to match required graph output format
    logger.info("Save the graph data to JSON")
    data_dict = transform_data(journal_drug_df)
    save_dict_to_json(data_dict, JSON_GRAPH_PATH)

    # Filtered fields for drug and journal analysis
    fd = ["journal", "drug"]

    # Read the JSON graph data and filter it based on PubMed and clinical trials
    link_data = read_json_file(JSON_GRAPH_PATH)
    link_data_filter = filter_pubmed_and_clinical_trials(link_data)
    df = read_json_df(link_data_filter)

    logger.info("Display the most drugs by journal")
    print(most_journal_by_drug(df, fd, "journal"))

    logger.info("List of drugs by PubMed journal")
    print(pubmed_drug(df, "journal_type", "pubmed"))


if __name__ == "__main__":
    data_pipeline()
