import os
from light_pandas.dataframe import DataFrame


def _analyze_one_line(line_str, sep=','):
    out_li = []
    need_combine = False
    col_start = 0
    idx = 0
    while idx < len(line_str):
        if line_str[idx] == '"':
            if need_combine:
                need_combine = False
                out_li.append(line_str[col_start:idx])
                if line_str[idx+1] == sep:
                    idx += 1
            else:
                need_combine = True
            col_start = idx + 1
            idx += 1
            continue
        if need_combine:
            idx += 1
        else:
            if line_str[idx] == sep:
                if col_start == idx:
                    out_li.append('')
                else:
                    out_li.append(line_str[col_start:idx])
                col_start = idx + 1
            idx += 1
    if col_start == idx:
        out_li.append('')
    else:
        out_li.append(line_str[col_start:idx])
    return out_li


def read_csv(filepath_or_buf, sep=',', index_col=None):
    if filepath_or_buf is None:
        return DataFrame()
    if not os.path.isfile(filepath_or_buf):
        return DataFrame()
    df = DataFrame()
    fid = open(filepath_or_buf)
    header_line = fid.readline()
    df.columns = _analyze_one_line(header_line.strip(), sep)
    for col_idx in range(len(df.columns)):
        if len(df.columns[col_idx]) == 0:
            df.columns[col_idx] = 'Unnamed{}'.format(col_idx)
    if index_col is not None:
        if index_col < len(df.columns):
            del df.columns[index_col]
    while True:
        data_line = fid.readline()
        if not data_line:
            break
        data_list = _analyze_one_line(data_line.strip(), sep)
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

