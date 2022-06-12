import unittest
import light_pandas as lpd
# import pandas as lpd


class Test(unittest.TestCase):
    def test_create_df(self):
        df = lpd.DataFrame(columns=('item1', 'item2'))
        self.assertEqual(len(df), 0)

    def test_add_one_row(self):
        df = lpd.DataFrame(columns=('item1', 'item2'))
        df = df.append({'item1': 't1', 'item2': 't2'}, ignore_index=True)
        self.assertEqual(len(df), 1)

    def test_read_csv(self):
        df = lpd.read_csv('test.csv')
        self.assertEqual(len(df), 5)
        self.assertListEqual(list(df.columns), ['item1', 'item2'])

    def test_iloc(self):
        df = lpd.read_csv('test.csv')
        row_line = dict(df.iloc[1])
        self.assertDictEqual(row_line, {'item1': 't3', 'item2': 't4'})

    def test_iloc_mul(self):
        df = lpd.read_csv('test.csv')
        sub_df = df.iloc[1:3]
        row_line = dict(sub_df.iloc[1])
        self.assertEqual(len(sub_df), 2)
        self.assertDictEqual(row_line, {'item1': 't5', 'item2': 't6'})

    def test_iloc_set(self):
        df = lpd.read_csv('test.csv')
        df.iloc[1]['item1'] = 'bomb'
        row_line = dict(df.iloc[1])
        self.assertDictEqual(row_line, {'item1': 'bomb', 'item2': 't4'})


if __name__ == '__main__':
    unittest.main()
