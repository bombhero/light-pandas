import copy


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

    def __gt__(self, other):
        result_li = []
        col_idx = self.df.columns.index(self.col_name)
        for row_idx in range(len(self)):
            result_li.append(self.df.data_frame[row_idx][col_idx] > other)
        return result_li

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
        col_idx = self.df.columns.index(key[1])
        result_li = []
        for row_idx in range(len(key[0])):
            if not key[0][row_idx]:
                continue
            self.df.data_frame[row_idx][col_idx] = value


class DataFrame:
    def __init__(self, columns=None):
        if columns is None:
            self.columns = []
        else:
            self.columns = [val for val in columns]
        self.data_frame = []
        self.iloc = IndexLocation(self)
        self.loc = Location(self)

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
            row_line[col_num] = data_dict[key]
        df.data_frame.append(row_line)
        return df

    def _append_list(self, data_list):
        df = copy.copy(self)
        if len(data_list) != len(self.columns):
            return df
        else:
            df.data_frame.append(data_list)
            return df

    def to_csv(self, path, sep=','):
        fid = open(path, 'w')
        header_line = ',{}\n'.format(sep.join(self.headers))
        fid.write(header_line)
        for idx in range(len(self.data_frame)):
            data_line = '{},{}\n'.format(idx, sep.join(self.data_frame[idx]))
            fid.write(data_line)
        fid.close()

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

    def sort_values(self, by=[], ascending=True):
        result_df = DataFrame(columns=self.columns)
        sort_seq = []
        if len(by) > 0:
            for col_name in by:
                sort_seq.append(self.columns.index(col_name))
            result_df.data_frame = self._sort_value(sort_seq, self.data_frame, ascending)
        else:
            result_df.data_frame = self.data_frame
        return result_df

    def __len__(self):
        return len(self.data_frame)

    def __getitem__(self, col_name):
        column_item = ColumnItem(self, col_name)
        return column_item

    def __setitem__(self, key, value):
        col_idx = self.columns.index(key)
        for row_idx in range(len(self.data_frame)):
            self.data_frame[row_idx][col_idx] = value
