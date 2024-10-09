from pathlib import Path
import os
import polars as pl

from polars.testing import assert_frame_equal

from src.data.adhoc_processing import most_journal_by_drug
from src.data.data_cleaning import read_json

TEST_JSON_FILE = "clinical_pubmed_drug.json"
fd = ["journal", "drug"]
path = Path(TEST_JSON_FILE)

test_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), TEST_JSON_FILE)


def test_most_journal_by_drug():
    print(os.path.join(path.parent.absolute(), path))
    data = {"col1": ["Psychopharmacology", "Journal of emergency nursing",
                     "The journal of maternal-fetal & neonatal medicine"]}
    test_df = pl.DataFrame(data, schema=["journal"])
    df = read_json(test_file_path)
    result_df = most_journal_by_drug(df, fd, "journal")
    assert_frame_equal(test_df.sort("journal"), result_df.sort("journal"))
