import os
import csv
from lightpandas.dataframe import DataFrame


def read_csv(filepath_or_buf, sep=',', index_col=None):
    with open(filepath_or_buf, mode='r') as f:
        reader = csv.reader(f)
        df = DataFrame(columns=next(reader))
        for row in reader:
            df.data_frame.append(row)
    return df


def merge(left, right, how='inner'):
    result_df = None
    if how == 'outer':
        result_df = left
        for r_row_idx in range(len(right)):
            skip_row = False
            row_item = right.iloc[r_row_idx]
            for l_row_idx in range(len(result_df)):
                if row_item == result_df.iloc[l_row_idx]:
                    skip_row = True
                    break
            if skip_row:
                continue
            result_df = result_df.append(row_item, ignore_index=True)
    elif how == 'inner':
        result_df = DataFrame(columns=left.columns)
    return result_df

