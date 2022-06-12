import copy


class RowItem:
    def __init__(self, df, row_idx):
        self.df = df
        self.row_idx = row_idx
        self.current_col_idx = 0

    def __setitem__(self, key, value):
        col_idx = self.df.columns.index(key)
        self.df.data_frame[self.row_idx][col_idx] = str(value)

    def __iter__(self):
        for idx in range(len(self)):
            yield self.df.columns[idx], self.df.data_frame[self.row_idx][idx]

    def __len__(self):
        return len(self.df.columns)


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


class DataFrame:
    def __init__(self, columns=None):
        if columns is None:
            self.columns = []
        else:
            self.columns = [val for val in columns]
        self.data_frame = []
        self.iloc = IndexLocation(self)

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

    def to_csv(self, path, sep=','):
        fid = open(path, 'w')
        header_line = ',{}\n'.format(sep.join(self.headers))
        fid.write(header_line)
        for idx in range(len(self.data_frame)):
            data_line = '{},{}\n'.format(idx, sep.join(self.data_frame[idx]))
            fid.write(data_line)
        fid.close()

    def __len__(self):
        return len(self.data_frame)

