# coding=utf8
import os
import csv
import pandas as pd

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


def read_csv(filepath_or_buffer, delimiter=',', sep=',', index_col=None, keep_default_na=False, low_memory=False,
             dtype=None):
    delimiter = sep
    with open(filepath_or_buffer, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f)
        index_name, columns = _generate_columns(next(reader), index_col)
        df = DataFrame(columns=columns)
        df.index_name = index_name
        for row in reader:
            index_val = None
            if index_col is not None:
                index_val = row[index_col]
                del row[index_col]
            if len(row) < len(df.columns):
                gap_count = len(df.columns) - len(row)
                for _ in range(gap_count):
                    row.append('')
            df.data_frame.append(row)
            if index_val is not None:
                df.index.append(index_val)
        if index_col is None:
            df._re_index(df.index_name)
    return df


# def merge(left, right, how='inner'):
#     result_df = None
#     if how == 'outer':
#         result_df = left
#         for r_row_idx in range(len(right)):
#             skip_row = False
#             row_item = right.iloc[r_row_idx]
#             for l_row_idx in range(len(result_df)):
#                 if row_item == result_df.iloc[l_row_idx]:
#                     skip_row = True
#                     break
#             if skip_row:
#                 continue
#             result_df = result_df.append(row_item, ignore_index=True)
#     elif how == 'inner':
#         result_df = DataFrame(columns=left.columns)
#     return result_df


def merge(left, right, how='inner'):
    result_df = None
    if how == 'outer':
        result_df = DataFrame(columns=left.columns)
        result_df.data_frame = left.data_frame + right.data_frame
        result_df._re_index(left.index_name)
    elif how == 'inner':
        result_df = DataFrame(columns=left.columns)
    return result_df


def import_from_pandas(pandas_df):
    result_df = DataFrame(columns=pandas_df.columns)
    for idx in range(len(pandas_df)):
        lpd_row = {}
        pd_row = dict(pandas_df.iloc[idx])
        for col_name in pd_row.keys():
            lpd_row[col_name] = str(pd_row[col_name])
        result_df = result_df.append(lpd_row, ignore_index=True)
    return result_df


def read_excel(io, sheet_name=None, keep_default_na=False, na_values=None, header=0):
    pd_df = pd.read_excel(io, sheet_name=sheet_name, keep_default_na=keep_default_na,
                          na_values=na_values, header=header)
    return import_from_pandas(pd_df)


def ExcelWriter(path):
    return pd.ExcelWriter(path=path)

