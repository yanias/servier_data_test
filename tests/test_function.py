import polars as pl
from polars.testing import assert_frame_equal
from src.data.data_cleaning import read_json, most_journal_by_drug

json_path = "../datas/cleaned/clinical_pubmed_drug.json"
fd = ["journal", "drug"]


def test_most_journal_by_drug():
    data = {"col1": ["Psychopharmacology", "Journal of emergency nursing"]}
    test_df = pl.DataFrame(data, schema=["journal"])
    df = read_json(json_path)
    result_df = most_journal_by_drug(df, fd, "journal")
    assert_frame_equal(test_df.sort("journal"), result_df.sort("journal"))
