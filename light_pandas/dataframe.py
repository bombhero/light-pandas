import copy


class DataFrame:
    def __init__(self, columns=None):
        if columns is None:
            self.columns = []
        else:
            self.columns = [val for val in columns]
        self.data_frame = []

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

