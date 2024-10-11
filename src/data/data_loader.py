import pandas as pd
import polars as pl
from polars import DataFrame
import json


def csv_reader(path):
    """
    this function is about read csv file with pandas
    :param path: csv path
    :return: DataFrame in pandas
    """
    df = pd.read_csv(
        path, quotechar='"', delimiter=",", encoding="utf-8", encoding_errors="strict"
    )
    return df


def rename_title(df):
    if "scientific_title" in df.columns:
        df = df.rename(columns={"scientific_title": "title"})
    else:
        df
    return df


def read_json_pd(path) -> DataFrame():
    """
    This function is about read json file
    :param path: json path
    :return: DataFrame in Polars
    """
    df = pl.read_json(path)
    return df


def read_json_df(path):
    df = pl.DataFrame(path)
    return df


def save_dict_to_json(dictionary, filename):
    with open(filename, "w", encoding="utf-8") as json_file:
        json.dump(dictionary, json_file, indent=4, ensure_ascii=False)


def read_json_file(filename):
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data
