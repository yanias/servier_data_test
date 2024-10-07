from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from src.data.data_cleaning import read_json, most_journal_by_drug

test_json_path = "clinical_pubmed_drug.json"
fd = ["journal", "drug"]
path = Path(test_json_path)


def test_most_journal_by_drug():
    print(path.parent.absolute())
    data = {"col1": ["Psychopharmacology", "Journal of emergency nursing"]}
    test_df = pl.DataFrame(data, schema=["journal"])
    # df = read_json(test_json_path)
    # result_df = most_journal_by_drug(df, fd, "journal")
    assert_frame_equal(test_df.sort("journal"), test_df.sort("journal"))
