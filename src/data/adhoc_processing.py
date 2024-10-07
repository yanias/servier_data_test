import polars as pl


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
        .sql("""select journal,ARRAY_AGG(drug) from self group by journal"""
)