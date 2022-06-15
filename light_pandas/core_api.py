import os
from light_pandas.dataframe import DataFrame


def read_csv(filepath_or_buf, sep=',', index_col=None):
    if filepath_or_buf is None:
        return DataFrame()
    if not os.path.isfile(filepath_or_buf):
        return DataFrame()
    df = DataFrame()
    fid = open(filepath_or_buf)
    header_line = fid.readline()
    df.columns = [item.strip() for item in header_line.strip().split(sep)]
    if index_col is not None:
        if index_col < len(df.columns):
            del df.columns[index_col]
    while True:
        data_line = fid.readline()
        if not data_line:
            break
        data_list = [item.strip() for item in data_line.strip().split(sep)]
        if index_col is not None:
            if index_col < len(data_list):
                del data_list[index_col]
        df.data_frame.append(data_list)
    fid.close()
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

