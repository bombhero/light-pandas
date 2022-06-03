import os


class DataFrame:
    def __init__(self, columns=None):
        if columns is None:
            self.headers = []
        else:
            self.headers = [val for val in columns]
        self.data_frame = []

    def read_csv(self, path=None, sep=',', index_col=0):
        if path is None:
            return
        if not os.path.isfile(path):
            return
        fid = open(path)
        header_line = fid.readline()
        self.headers = header_line.strip().split(sep)
        if index_col < len(self.headers):
            del self.headers[index_col]
        while True:
            data_line = fid.readline()
            if not data_line:
                break
            data_list = data_line.strip().split(sep)
            if index_col < len(data_list):
                del data_list[index_col]
            self.data_frame.append(data_list)
        fid.close()

    def get_value(self, line_num, col_name):
        col_num = self.headers.index(col_name)
        return self.data_frame[line_num][col_num]

    def append_row(self, data_dict):
        row_line = ['' for _ in range(len(self.headers))]
        for key in data_dict.keys():
            if key not in self.headers:
                self.headers.append(key)
                for idx in range(len(self.data_frame)):
                    self.data_frame.append('')
                row_line.append('')
            col_num = self.headers.index(key)
            row_line[col_num] = data_dict[key]
        self.data_frame.append(row_line)

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

