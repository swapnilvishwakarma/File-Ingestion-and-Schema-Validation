import logging
import os
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re


################
# File Reading #
################

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)


def replacer(string, char):
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string) 
    return string

def col_header_val(df, table_config):
    cols = df.columns
    cols = cols.str.strip()
    cols.str.replace(" ", "_")
    expected_col = list(map(lambda x: x.lower(),  config_data['columns']))
    cols = list(cols.sort_values())
    expected_col.sort()
    if len(cols) == len(expected_col) and cols == expected_col:
      print("Column name and Column length Validation Passed!!")
      return 1
    else:
      print("Column name and Column length Validation Failed..")
      mismatched_columns_file = list(set(cols).difference(expected_col))
      print("Following File columns are not in the YAML file", mismatched_columns_file)
      missing_YAML_file = list(set(expected_col).difference(cols))
      print("Following YAML columns are not in the file uploaded", missing_YAML_file)
      logging.info(f'df columns: {cols}')
      logging.info(f'expected columns: {expected_col}')
      return 0

def stats(df, config_data):
  if col_header_val(df, config_data) == 1:
    col_names = list(df.columns)
    no_of_cols = df.shape[1]
    no_of_rows = df.shape[0]
    statistics = f"\nNo. of Columns: {no_of_cols} \nNo. of Rows: {no_of_rows} \nColumn Names: {col_names}"
    print(statistics)