import pandas as pd
import polars as pl
from polars import DataFrame

from src.data.data_cleaning import merge_df


def csv_reader(path):
    """
    this function is about read csv file with pandas
    :param path: csv path
    :return: DataFrame in pandas
    """
    df = pd.read_csv(path, quotechar='"', delimiter=",", encoding="utf-8", encoding_errors='strict')
    return df


def rename_title(df):
    if 'scientific_title' in df.columns:
        df = df.rename(columns={"scientific_title": "title"})
    else:
        df
    return df


def read_json(path) -> DataFrame():
    """
    This function is about read json file
    :param path: json path
    :return: DataFrame in Polars
    """
    df = pl.read_json(path)
    return df


def save_to_json(df: DataFrame, json_path, fields_to_keep):
    """
    This function is about the linked json graph
    :param df:
    :param journal_df: DataFrame of journal
    :param drug_df: drug DataFrame
    :param json_path: where to save json file
    :param fields_to_keep: the set of share fields of journal
    """

    df.to_json(json_path, orient='records', date_format='iso', force_ascii=False, indent=4)
