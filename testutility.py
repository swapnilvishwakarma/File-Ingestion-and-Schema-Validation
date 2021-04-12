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

def humanbytes(B):
   'Return the given bytes as a human friendly KB, MB, GB, or TB string'
   B = float(B)
   KB = float(1024)
   MB = float(KB ** 2) # 1,048,576
   GB = float(KB ** 3) # 1,073,741,824
   TB = float(KB ** 4) # 1,099,511,627,776

   if B < KB:
      return '{0} {1}'.format(B,'Bytes' if 0 == B > 1 else 'Byte')
   elif KB <= B < MB:
      return '{0:.2f} KB'.format(B/KB)
   elif MB <= B < GB:
      return '{0:.2f} MB'.format(B/MB)
   elif GB <= B < TB:
      return '{0:.2f} GB'.format(B/GB)
   elif TB <= B:
      return '{0:.2f} TB'.format(B/TB)

def stats(df, config_data):
  if col_header_val(df, config_data) == 1:
    col_names = list(df.columns)
    no_of_cols = df.shape[1]
    no_of_rows = df.shape[0]
    size = df.memory_usage(deep=True)
    file_size = humanbytes(size)
    statistics = f"\nNo. of Columns: {no_of_cols} \nNo. of Rows: {no_of_rows} \nColumn Names: {col_names} \nFile Size: {file_size}"
    print(statistics)

def save_gz(df):
  df.to_csv('compressed_train.gz', sep='|', compression='gzip')
  print("File saved in gz format with pipe separator")