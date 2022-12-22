# coding=utf8
import unittest
import os
import lightpandas as lpd
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

    def test_read_csv2(self):
        df = lpd.read_csv('test2.csv')
        self.assertEqual(len(df), 2)

    def test_read_csv_with_index(self):
        df = lpd.read_csv('test_with_index.csv', index_col=0)
        self.assertEqual(len(df.columns), 4)

    def test_read_csv_with_index2(self):
        df = lpd.read_csv('test_with_index.csv')
        self.assertEqual(len(df.columns), 5)

    def test_write_csv(self):
        if os.path.exists('test_w.csv'):
            os.remove('test_w.csv')
        df = lpd.DataFrame(columns=('item1', 'item2'))
        df = df.append({'item1': 't1', 'item2': 't2'}, ignore_index=True)
        df.to_csv('test_w.csv')
        df = lpd.read_csv('test_w.csv')
        self.assertEqual(len(df), 1)

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

    def test_col_pick(self):
        df = lpd.read_csv('test.csv')
        row_li = list(df['item1'])
        self.assertListEqual(row_li, ['t1', 't3', 't5', 't7', 't9'])

    def test_loc_gt_pick(self):
        df = lpd.read_csv('test.csv')
        pick_df = df.loc[df['item1'] > 't4']
        row_line = dict(pick_df.iloc[1])
        self.assertDictEqual(row_line, {'item1': 't7', 'item2': 't8'})

    def test_loc_lt_pick(self):
        df = lpd.read_csv('test.csv')
        pick_df = df.loc[df['item1'] < 't4']
        row_line = dict(pick_df.iloc[1])
        self.assertDictEqual(row_line, {'item1': 't3', 'item2': 't4'})

    def test_loc_mul_pick(self):
        df = lpd.read_csv('test.csv')
        row_line = list(df.loc[df['item1'] > 't4', 'item2'])
        self.assertListEqual(row_line, ['t6', 't8', 't0'])

    def test_loc_set(self):
        df = lpd.read_csv('test.csv')
        df.loc[df['item1'] > 't4', 'item2'] = 'bomb'
        row_line = dict(df.iloc[2])
        self.assertDictEqual(row_line, {'item1': 't5', 'item2': 'bomb'})

    def test_merge_df(self):
        df1 = lpd.read_csv('test.csv')
        df2 = lpd.read_csv('test.csv')
        df2['item1'] = 'bomb'
        df = lpd.merge(df1, df2, how='outer')
        self.assertEqual(len(df), 10)

    def test_df_sort(self):
        df = lpd.read_csv('test.csv')
        df = df.sort_values(by=['item1'], ascending=False)
        row_line = dict(df.iloc[0])
        self.assertDictEqual(row_line, {'item1': 't9', 'item2': 't0'})

    def test_df_drop_duplicates(self):
        df = lpd.read_csv('test.csv')
        df = df.append({'item1': 't3', 'item2': 't4'}, ignore_index=True)
        df = df.drop_duplicates()
        self.assertEqual(len(df), 5)


if __name__ == '__main__':
    unittest.main()
