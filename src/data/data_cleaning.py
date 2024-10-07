from datetime import datetime
from pandas import DataFrame
import pandas as pd
import polars as pl


def date_cleaning(df: DataFrame):
    df['mention_date'] = df['date'].apply(
        lambda x: datetime.strptime(x, "%d %B %Y") if len(x) > 10 else
        (datetime.strptime(x, "%Y-%m-%d") if "-" in x else
         datetime.strptime(x, "%d/%m/%Y")))
    df["mention_date"] = df["mention_date"].apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))

    return df


def remove_weird_char(df, col) -> DataFrame:
    df[f'{col}'] = df[f'{col}'].str.replace('\\xc3\\x28', '', regex=False)
    return df


def merge_df(df1: DataFrame, df2: DataFrame, fields_to_keep) -> DataFrame:
    """
    This function about merging 2 Df with case sensitive
    join 2 df if a word on one df could be found in the field of the second one
    the merge is done on the field title
    :param df1:
    :param df2:
    :param fields_to_keep:
    :return: DataFrame
    """

    df1['merge_col'] = 1
    df2['merge_col'] = 1
    merge_dataframe = df1.merge(df2, on='merge_col').drop('merge_col', axis=1)
    df2.drop('merge_col', axis=1, inplace=True)
    merge_dataframe["to_keep"] = merge_dataframe\
        .apply(lambda x: str(x['title']).lower().find(str(x['drug']).lower()) >= 0, axis=1)
    df = merge_dataframe[merge_dataframe["to_keep"]].filter(items=fields_to_keep)
    df['journal'] = df['journal'].str.replace('\\xc3\\x28', '', regex=False)
    return df


def read_json(path):
    df = pl.read_json(path)
    return df
