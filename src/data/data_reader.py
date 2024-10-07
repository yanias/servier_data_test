import pandas as pd


def csv_reader(path):
    """ read csv file with pandas"""
    df = pd.read_csv(path, quotechar='"', delimiter=",", encoding="utf-8", encoding_errors='strict')
    return df
