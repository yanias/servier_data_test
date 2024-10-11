from pathlib import Path
import os
import polars as pl
import pandas as pd
import pytest

from polars.testing import assert_frame_equal

from src.data.adhoc_processing import (
    most_journal_by_drug,
    filter_pubmed_and_clinical_trials,
)
from src.data.data_cleaning import date_cleaning, remove_weird_char, transform_data
from src.data.data_loader import read_json_file, read_json_df

TEST_JSON_FILE = "clinical_pubmed_drug.json"
fd = ["journal", "drug"]
path = Path(TEST_JSON_FILE)

test_file_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), TEST_JSON_FILE
)


# Sample data for testing
@pytest.fixture
def sample_data():
    data = {
        "journal": [
            "Journal of Emergency Nursing",
            "Journal of Emergency Nursing",
            "Journal of Pediatrics",
        ],
        "journal_type": ["pubmed", "clinical", "pubmed"],
        "id": [1, 2, 3],
        "date": ["01 January 2019", "2020-01-01", "02 January 2019"],
        "drug": ["Diphenhydramine", "Diphenhydramine", "Diphenhydramine"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def clean_data():
    data = {
        "journal": [
            "Journal of Emergency Nursing",
            "Journal of Emergency Nursing",
            "Journal of Pediatrics",
        ],
        "journal_type": ["pubmed", "clinical", "pubmed"],
        "id": [1, 2, 3],
        "mention_date": ["2019-01-01", "2020-01-01", "2019-01-02"],
        "drug": ["Diphenhydramine", "Diphenhydramine", "Diphenhydramine"],
    }
    return pd.DataFrame(data)


# Test date_cleaning function
def test_date_cleaning(sample_data):
    cleaned_df = date_cleaning(sample_data)
    print(cleaned_df["mention_date"])

    # Check if 'mention_date' column is in the correct format
    assert pd.to_datetime(cleaned_df["mention_date"]).notnull().all()
    assert cleaned_df["mention_date"].to_list() == [
        "2019-01-01",
        "2020-01-01",
        "2019-01-02",
    ]
    # assert cleaned_df['mention_date'].iloc[0] == '2019-01-01'  # Correct format check


# Test remove_weird_char function
def test_remove_weird_char(sample_data):
    sample_data["drug"] = sample_data["drug"].str.cat(["\\xc3\\x28"] * len(sample_data))
    cleaned_df = remove_weird_char(sample_data, "drug")

    # Check that the unwanted character is removed
    assert "\\xc3\\x28" not in cleaned_df["drug"].values
    assert "Diphenhydramine" in cleaned_df["drug"].values


# Test transform_data function
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_transform_data(clean_data):
    transformed_data = transform_data(clean_data)

    # Ensure the transformed data structure is correct
    assert "journals" in transformed_data
    assert isinstance(transformed_data["journals"], list)

    # Ensure that the first journal has both 'pubmed' and 'clinical_trials' keys
    first_journal = transformed_data["journals"][0]
    assert "pubmed" in first_journal["Journal of Emergency Nursing"]
    assert "clinical_trials" in first_journal["Journal of Emergency Nursing"]

    # Check if pubmed contains the right data
    assert len(first_journal["Journal of Emergency Nursing"]["pubmed"]) > 0

    # Check if clinical_trials contains the right data
    assert len(first_journal["Journal of Emergency Nursing"]["clinical_trials"]) > 0


# test test_most_journal_by_drug function
def test_most_journal_by_drug():
    print(os.path.join(path.parent.absolute(), path))
    data = {
        "col1": [
            "Psychopharmacology",
            "Journal of emergency nursing",
            "The journal of maternal-fetal & neonatal medicine",
        ]
    }
    test_df = pl.DataFrame(data, schema=["journal"])
    link_data = read_json_file(test_file_path)
    link_data_filter = filter_pubmed_and_clinical_trials(link_data)
    df = read_json_df(link_data_filter)
    result_df = most_journal_by_drug(df, fd, "journal")
    assert_frame_equal(test_df.sort("journal"), result_df.sort("journal"))


# Run the tests
if __name__ == "__main__":
    pytest.main()
