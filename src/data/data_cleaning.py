from datetime import datetime
from pandas import DataFrame
import pandas as pd
import polars as pl


def date_cleaning(df: DataFrame) -> DataFrame:
    """
    Cleans and standardizes the 'mention_date' column into a consistent date format.

    Args:
        df: Input DataFrame containing the 'date' column.

    Returns:
        DataFrame: DataFrame with a new 'mention_date' column in YYYY-MM-DD format.
    """
    # Validate that the 'date' column exists
    if "date" not in df.columns:
        raise ValueError("Column 'date' not found in DataFrame")

    # Standardize date formats to YYYY-MM-DD
    df["mention_date"] = df["date"].apply(
        lambda x: (
            datetime.strptime(x, "%d %B %Y")
            if len(x) > 10
            else (
                datetime.strptime(x, "%Y-%m-%d")
                if "-" in x
                else datetime.strptime(x, "%d/%m/%Y")
            )
        )
    )
    df["mention_date"] = df["mention_date"].apply(
        lambda x: datetime.strftime(x, "%Y-%m-%d")
    )

    return df


def remove_weird_char(df: DataFrame, col: str) -> DataFrame:
    """
    Removes specific unwanted characters from a given column in the DataFrame.

    Args:
        df: Input DataFrame.
        col: The column from which to remove unwanted characters.

    Returns:
        DataFrame: DataFrame with cleaned column values.
    """
    # Ensure the column exists in the DataFrame
    if col not in df.columns:
        raise ValueError(f"Column '{col}' not found in DataFrame")

    # Replace unwanted characters in the specified column
    df[f"{col}"] = df[f"{col}"].str.replace("\\xc3\\x28", "", regex=False)
    return df


def merge_df(df1: DataFrame, df2: DataFrame, fields_to_keep: list) -> DataFrame:
    """
    Merges two DataFrames based on a case-insensitive match of 'title' and 'drug' columns.

    Args:
        df1: First DataFrame.
        df2: Second DataFrame.
        fields_to_keep: List of columns to retain in the merged DataFrame.

    Returns:
        DataFrame: Merged DataFrame with specified columns.
    """
    # Ensure required columns are present
    if "title" not in df1.columns or "drug" not in df2.columns:
        raise ValueError("Columns 'title' or 'drug' missing from DataFrames")

    # Add a temporary column for the merge
    df1["merge_col"] = 1
    df2["merge_col"] = 1
    merge_dataframe = df1.merge(df2, on="merge_col").drop("merge_col", axis=1)
    df2.drop("merge_col", axis=1, inplace=True)

    # Apply case-insensitive match and keep only rows where 'drug' appears in 'title'
    merge_dataframe["to_keep"] = merge_dataframe.apply(
        lambda x: str(x["title"]).lower().find(str(x["drug"]).lower()) >= 0, axis=1
    )
    df = merge_dataframe[merge_dataframe["to_keep"]].filter(items=fields_to_keep)

    # Clean the 'journal' column
    df["journal"] = df["journal"].str.replace("\\xc3\\x28", "", regex=False)
    return df


def transform_data(df: DataFrame) -> dict:
    """
    Transforms the input DataFrame into a nested dictionary structure based on 'journal' and 'journal_type'.

    Args:
        df: Input DataFrame containing 'journal', 'journal_type', 'id', 'mention_date', and 'drug' columns.

    Returns:
        dict: Transformed dictionary structure categorized by journals and their corresponding pubmed and clinical trial articles.
    """
    # Check if required columns exist
    required_columns = ["journal", "journal_type", "id", "mention_date", "drug"]
    if not all(col in df.columns for col in required_columns):
        raise ValueError(
            f"One or more required columns are missing: {', '.join(required_columns)}"
        )

    # Start by creating an empty dictionary to hold the results
    journals = []

    # Group the data by 'journal'
    for journal, group in df.groupby("journal"):
        # Create a new dictionary for each journal
        journal_entry = {journal: {"pubmed": [], "clinical_trials": []}}

        # Separate pubmed and clinical trials based on 'journal_type'
        pubmed_group = group[group["journal_type"] == "pubmed"]
        clinical_group = group[group["journal_type"] == "clinical"]

        # Process pubmed articles
        for _, row in pubmed_group.iterrows():
            article = {
                "article_id": str(row["id"]),
                "mention_date": row["mention_date"],
                "drug": row["drug"],
            }
            journal_entry[journal]["pubmed"].append(article)

        # Process clinical trials
        for _, row in clinical_group.iterrows():
            article = {
                "article_id": str(row["id"]),
                "mention_date": row["mention_date"],
                "drug": row["drug"],
            }
            journal_entry[journal]["clinical_trials"].append(article)

        # Add the journal entry to the list
        journals.append(journal_entry)

    # Return the final structure
    return {"journals": journals}
