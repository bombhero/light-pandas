# coding=utf8
import copy
import csv
import pandas as pd


class RowItem:
    def __init__(self, df, row_idx):
        self.df = df
        self.row_idx = row_idx
        self.current_col_idx = 0

    def __getitem__(self, key):
        col_idx = self.df.columns.index(key)
        return self.df.data_frame[self.row_idx][col_idx]

    def __setitem__(self, key, value):
        col_idx = self.df.columns.index(key)
        self.df.data_frame[self.row_idx][col_idx] = str(value)

    def __iter__(self):
        for idx in range(len(self)):
            yield self.df.columns[idx], self.df.data_frame[self.row_idx][idx]

    def __len__(self):
        return len(self.df.columns)

    def keys(self):
        return self.df.columns


class ColumnItem:
    def __init__(self, df, col_name):
        self.df = df
        self.col_name = col_name

    def __iter__(self):
        col_idx = self.df.columns.index(self.col_name)
        for row_idx in range(len(self)):
            yield self.df.data_frame[row_idx][col_idx]

    def _compare(self, other, op):
        result_li = []
        col_idx = self.df.columns.index(self.col_name)
        for row_idx in range(len(self)):
            if op == 'gt':
                result_li.append(self.df.data_frame[row_idx][col_idx] > other)
            elif op == 'ge':
                result_li.append(self.df.data_frame[row_idx][col_idx] >= other)
            elif op == 'eq':
                result_li.append(self.df.data_frame[row_idx][col_idx] == other)
            elif op == 'lt':
                result_li.append(self.df.data_frame[row_idx][col_idx] < other)
            elif op == 'le':
                result_li.append(self.df.data_frame[row_idx][col_idx] <= other)
        return result_li

    def __gt__(self, other):
        return self._compare(other, 'gt')

    def __ge__(self, other):
        return self._compare(other, 'ge')

    def __eq__(self, other):
        return self._compare(other, 'eq')

    def __lt__(self, other):
        return self._compare(other, 'lt')

    def __le__(self, other):
        return self._compare(other, 'le')

    def __len__(self):
        return len(self.df)


class IndexLocation:
    def __init__(self, df):
        self.df = df

    def __getitem__(self, item):
        if isinstance(item, slice):
            result_df = DataFrame(columns=self.df.columns)
            if item.start is None:
                start_idx = 0
            else:
                start_idx = item.start
            if item.stop is None:
                stop_idx = len(self.df)
            else:
                stop_idx = item.stop
            if item.step is None:
                step_idx = 1
            else:
                step_idx = item.step
            for row_idx in range(start_idx, stop_idx, step_idx):
                row_dict = {}
                for col_idx in range(len(self.df.columns)):
                    row_dict[self.df.columns[col_idx]] = self.df.data_frame[row_idx][col_idx]
                result_df = result_df.append(row_dict, ignore_index=True)
            return result_df
        elif isinstance(item, int):
            if len(self.df.data_frame) > item:
                row_item = RowItem(self.df, item)
                return row_item
            else:
                raise IndexError("single positional indexer is out-of-bounds")


class Location:
    def __init__(self, df):
        self.df = df

    def __getitem__(self, item):
        if isinstance(item, list):
            result_df = DataFrame(columns=self.df.columns)
            for row_idx in range(len(item)):
                if not item[row_idx]:
                    continue
                row_dict = {}
                for col_idx in range(len(self.df.columns)):
                    row_dict[self.df.columns[col_idx]] = self.df.data_frame[row_idx][col_idx]
                result_df = result_df.append(row_dict, ignore_index=True)
            return result_df
        elif isinstance(item, tuple):
            col_idx = self.df.columns.index(item[1])
            result_li = []
            for row_idx in range(len(item[0])):
                if not item[0][row_idx]:
                    continue
                result_li.append(self.df.data_frame[row_idx][col_idx])
            return result_li

    def __setitem__(self, key, value):
        if key[1] not in self.df.columns:
            self.df.columns.append(key[1])
            for idx in range(len(self.df.data_frame)):
                self.df.data_frame[idx].append('')
        col_idx = self.df.columns.index(key[1])
        for row_idx in range(len(key[0])):
            if not key[0][row_idx]:
                continue
            self.df.data_frame[row_idx][col_idx] = value


class DataFrame:
    def __init__(self, data=None, columns=None):
        if columns is None:
            self.columns = []
        else:
            self.columns = [val for val in columns]
        self.index = []
        self.index_name = ''
        self.data_frame = []
        self.iloc = IndexLocation(self)
        self.loc = Location(self)
        if data is not None:
            max_row_count = 1
            row_idx = 0
            while True:
                row_dict = {}
                skip_row = True
                for col_name in data.keys():
                    if row_idx == 0:
                        if col_name not in self.columns:
                            self.columns.append(col_name)
                        if type(data[col_name]) == list:
                            if len(data[col_name]) > max_row_count:
                                max_row_count = len(data[col_name])
                            if len(data[col_name]) > 0:
                                row_val = str(data[col_name][row_idx])
                                skip_row = False
                            else:
                                row_val = ''
                        else:
                            row_val = str(data[col_name])
                            skip_row = False
                    else:
                        if type(data[col_name]) == list and len(data[col_name]) > row_idx:
                            row_val = str(data[col_name][row_idx])
                        else:
                            row_val = ''
                        skip_row = False
                    row_dict[col_name] = row_val
                if not skip_row:
                    self.append(row_dict)
                row_idx += 1
                if row_idx >= max_row_count:
                    break

    def increase_index(self, defined_index=None):
        if defined_index is None:
            if len(self.index) > 0:
                local_index = self.index[-1] + 1
            else:
                local_index = 0
            while True:
                if local_index not in self.index:
                    break
                else:
                    local_index += 1
        else:
            local_index = str(defined_index)
        self.index.append(local_index)

    def append(self, data_dict, ignore_index=False):
        df = copy.copy(self)
        row_line = ['' for _ in range(len(df.columns))]
        for key in data_dict.keys():
            if key not in df.columns:
                df.columns.append(key)
                for idx in range(len(df.data_frame)):
                    df.data_frame.append('')
                row_line.append('')
            col_num = df.columns.index(key)
            row_line[col_num] = str(data_dict[key])
        df.data_frame.append(row_line)
        df.increase_index()
        return df

    def _append_list(self, data_list):
        df = copy.copy(self)
        if len(data_list) != len(self.columns):
            return df
        else:
            df.data_frame.append(data_list)
            df.increase_index()
            return df

    def _pick_columns(self, col_list):
        new_df = DataFrame(columns=col_list)
        col_idx_list = [self.columns.index(col_name) for col_name in col_list]
        for row_idx in range(len(self)):
            row_li = []
            for col_idx in col_idx_list:
                row_li.append(self.data_frame[row_idx][col_idx])
            new_df._append_list(row_li)
        return new_df

    def to_csv(self, path_or_buf, sep=',', index=True):
        with open(path_or_buf, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if index:
                col_list = copy.deepcopy(self.columns)
                data_frame = copy.deepcopy(self.data_frame)
                col_list.insert(0, self.index_name)
                for row_id in range(len(self.index)):
                    data_frame[row_id].insert(0, self.index[row_id])
                writer.writerow(col_list)
                writer.writerows(data_frame)
            else:
                writer.writerow(self.columns)
                writer.writerows(self.data_frame)

    def to_excel(self, excel_writer, sheet_name='Sheet1', columns=None, header=True, index=True, index_label=None):
        pd_df = self.export_to_pandas()
        pd_df.to_excel(excel_writer, sheet_name=sheet_name, columns=columns, header=header, index=index,
                       index_label=index_label)

    def _sort_value(self, sort_col_list, data_frame, ascending=True):
        if len(sort_col_list) == 0:
            return data_frame
        col_idx = sort_col_list[0]
        result_data_frame = []
        sort_dict = {}
        for data_row in data_frame:
            sort_key = data_row[col_idx]
            if sort_key not in sort_dict.keys():
                sort_dict[sort_key] = [data_row]
            else:
                sort_dict[sort_key].append(data_row)
        next_col_list = copy.copy(sort_col_list)
        del next_col_list[0]
        for key in sort_dict.keys():
            sort_dict[key] = self._sort_value(next_col_list, sort_dict[key])
        key_seq = list(sort_dict.keys())
        if ascending:
            key_seq.sort()
        else:
            key_seq.sort(reverse=True)
        for key in key_seq:
            result_data_frame += sort_dict[key]
        return result_data_frame

    def _re_index(self, index_name):
        self.index = [idx for idx in range(len(self.data_frame))]
        self.index_name = index_name

    def sort_values(self, by=[], ascending=True):
        result_df = DataFrame(columns=self.columns)
        sort_seq = []
        if len(by) > 0:
            for col_name in by:
                sort_seq.append(self.columns.index(col_name))
            result_df.data_frame = self._sort_value(sort_seq, self.data_frame, ascending)
        else:
            result_df.data_frame = self.data_frame
        result_df._re_index(self.index_name)
        return result_df

    def drop_duplicates(self):
        result_df = DataFrame(columns=self.columns)
        for row in self.data_frame:
            if row not in result_df.data_frame:
                result_df._append_list(row)
        return result_df

    def _drop_one_row(self, idx_name):
        idx = None
        if idx_name not in self.index:
            return
        for idx in range(len(self.index)):
            if self.index[idx] == idx_name:
                break
        if idx is not None:
            del self.data_frame[idx]
            del self.index[idx]

    def _drop_rows(self, index):
        df = copy.deepcopy(self)
        if (type(index) is str) or (type(index) is int):
            df._drop_one_row(index)
        elif type(index) is list:
            for idx_name in index:
                df._drop_one_row(idx_name)
        return df

    def _drop_one_column(self, col_name):
        col_idx = None
        if col_name not in self.columns:
            return
        for col_idx in range(len(self.columns)):
            if self.columns[col_idx] == col_name:
                break
        if col_idx is not None:
            for row_list in self.data_frame:
                del row_list[col_idx]
            del self.columns[col_idx]

    def _drop_column(self, columns):
        df = copy.deepcopy(self)
        if type(columns) is str:
            df._drop_one_column(columns)
        elif type(columns) is list:
            for col_name in columns:
                df._drop_one_column(col_name)
        return df

    def drop(self, labels=None, axis=0, inplace=False):
        if axis == 0 or axis == 'index':
            new_df = self._drop_rows(labels)
        elif axis == 1 or axis == 'column':
            new_df = self._drop_column(labels)
        else:
            new_df = self
        if inplace:
            self.index = new_df.index
            self.columns = new_df.columns
            self.data_frame = new_df.data_frame
        else:
            return new_df

    # def export_to_pandas(self):
    #     pd_df = pd.DataFrame(columns=self.columns)
    #     for idx in range(len(self)):
    #         pd_df = pd_df.append(dict(self.iloc[idx]), ignore_index=True)
    #     return pd_df

    def export_to_pandas(self):
        export_dict = {}
        for col_name in self.columns:
            export_dict[col_name] = self[col_name]
        pd_df = pd.DataFrame(export_dict)
        return pd_df

    def __len__(self):
        return len(self.data_frame)

    def __getitem__(self, col_name):
        ret_item = None
        if type(col_name) is str:
            ret_item = ColumnItem(self, col_name)
        elif type(col_name) is list:
            if (type(col_name[0]) is bool) and (len(col_name) == len(self.data_frame)):
                ret_item = self.loc[col_name]
            elif type(col_name[0]) is str:
                ret_item = self._pick_columns(col_name)
        return ret_item

    def __setitem__(self, key, value):
        col_idx = self.columns.index(key)
        for row_idx in range(len(self.data_frame)):
            self.data_frame[row_idx][col_idx] = value

    def __str__(self):
        result_str = ''
        result_str += '\t\t{}'.format('\t\t'.join(self.columns))
        if len(self.index_name) > 0:
            result_str += '\n{}'.format(self.index_name)
        for idx in range(len(self.index)):
            result_str += '\n{}\t\t{}'.format(self.index[idx], '\t\t'.join(self.data_frame[idx]))
        return result_str

