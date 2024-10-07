from datetime import datetime
from pandas import DataFrame
import pandas as pd
import polars as pl

pl.Config.set_fmt_str_lengths(300)


def date_cleaning(df: DataFrame):
    df['date'] = df['date'].apply(
        lambda x: datetime.strptime(x, "%d %B %Y") if len(x) > 10 else
        (datetime.strptime(x, "%Y-%m-%d") if "-" in x else
         datetime.strptime(x, "%d/%m/%Y")))
    return df


def merge_df_1(clinical_df: DataFrame, drugs_df: DataFrame, fields_to_keep):
    clinical_df['merge_col'] = 1
    drugs_df['merge_col'] = 1
    merge_dataframe = clinical_df.merge(drugs_df, on='merge_col').\
        drop('merge_col', axis=1)
    drugs_df.drop('merge_col', axis=1, inplace=True)
    merge_dataframe["to_keep"] = merge_dataframe.apply(
        lambda x: x.scientific_title.lower().find(x.drug.lower()), axis=1).ge(0)
    df = merge_dataframe[merge_dataframe["to_keep"]].\
        filter(items=fields_to_keep)
    df["journal_type"] = "clinical"
    df['journal'] = df['journal'].str.replace('\\xc3\\x28', '', regex=False)
    return df


def merge_df_2(pubmed_df: DataFrame, drugs_df: DataFrame, fields_to_keep):
    pubmed_df['merge_col'] = 1
    drugs_df['merge_col'] = 1
    merge_dataframe = pubmed_df.merge(drugs_df, on='merge_col').drop('merge_col', axis=1)
    drugs_df.drop('merge_col', axis=1, inplace=True)
    merge_dataframe["to_keep"] = merge_dataframe.apply(
        lambda x: x.title.lower().find(x.drug.lower()), axis=1).ge(0)
    df = merge_dataframe[merge_dataframe["to_keep"]].\
        filter(items=fields_to_keep)
    df["journal_type"] = "pubmed"
    df['journal'] = df['journal'].str.replace('\\xc3\\x28', '', regex=False)
    return df


def save_to_json(clinical_df, pubmed_df, drugs_df, json_path, fields_to_keep):
    """save result to json file"""
    cli_drugs_df = merge_df_1(clinical_df, drugs_df, fields_to_keep).\
        reset_index()
    pub_drugs = merge_df_2(pubmed_df, drugs_df, fields_to_keep).\
        reset_index()
    concat_df = pd.concat([cli_drugs_df, pub_drugs])
    concat_df.drop(columns=['index']).to_json(json_path, orient='records', date_format="iso",
                                              force_ascii=False, indent=4)


def read_json(path):
    df = pl.read_json(path)
    return df


def most_journal_by_drug(df, fields, group_by_field):
    """get the most unique appeared drug in the journal"""
    df = df.select(fields).unique()
    group_count_df = df.group_by(group_by_field).len()
    group_count_df = group_count_df.with_columns(c_max=pl.col("len").max())
    filter_df = group_count_df.filter(pl.col("len") == pl.col("c_max"))
    return filter_df.drop(["len", "c_max"])


def pubmed_drug(df, filter_fields, filter_clause):
    """list of drug in pubmed by journal"""
    return df.filter(pl.col(filter_fields) == str(filter_clause)) \
        .sql("""select journal,ARRAY_AGG(drug) from self group by journal""")
