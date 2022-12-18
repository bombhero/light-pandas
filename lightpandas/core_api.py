import os
import csv
from lightpandas.dataframe import DataFrame


def _generate_columns(col_list, index_col):
    index_name = ''
    result_col = []
    unnamed_prefix = 'Unnamed: {}'
    unnamed_count = 0
    for idx in range(len(col_list)):
        if index_col is not None:
            if idx == index_col:
                index_name = col_list[idx]
                continue
        if len(col_list[idx]) == 0:
            while True:
                tmp_name = unnamed_prefix.format(unnamed_count)
                if (tmp_name not in col_list) and (tmp_name not in result_col):
                    current_name = tmp_name
                    break
                else:
                    unnamed_count += 1
        else:
            current_name = col_list[idx]
        result_col.append(current_name)
    return index_name, result_col


def read_csv(filepath_or_buf, sep=',', index_col=None):
    with open(filepath_or_buf, mode='r') as f:
        reader = csv.reader(f)
        index_name, columns = _generate_columns(next(reader), index_col)
        df = DataFrame(columns=columns)
        df.index_name = index_name
        for row in reader:
            index_val = None
            if index_col is not None:
                index_val = row[index_col]
                del row[index_col]
            df.data_frame.append(row)
            df.increase_index(index_val)
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

