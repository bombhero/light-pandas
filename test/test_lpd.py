# coding=utf8
import unittest
import os
import time
import lightpandas as lpd
import pandas as pd


class Test(unittest.TestCase):
    def test_create_df(self):
        df = lpd.DataFrame(columns=('item1', 'item2'))
        self.assertEqual(len(df), 0)

    def test_add_one_row(self):
        df = lpd.DataFrame(columns=('item1', 'item2'))
        df = df.append({'item1': 't1', 'item2': 't2'}, ignore_index=True)
        self.assertEqual(len(df), 1)

    def test_read_csv(self):
        df = lpd.read_csv('test.csv', index_col=0)
        self.assertEqual(len(df), 5)
        # self.assertListEqual(list(df.columns), ['item1', 'item2'])

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
        self.assertDictEqual(row_line, {'item1': 't3', 'item2': 't4', 'item3': '4'})

    def test_iloc_mul(self):
        df = lpd.read_csv('test.csv')
        sub_df = df.iloc[1:3]
        row_line = dict(sub_df.iloc[1])
        self.assertEqual(len(sub_df), 2)
        self.assertDictEqual(row_line, {'item1': 't5', 'item2': 't6', 'item3': '6'})

    def test_iloc_set(self):
        df = lpd.read_csv('test.csv')
        df.iloc[1]['item1'] = 'bomb'
        row_line = dict(df.iloc[1])
        self.assertDictEqual(row_line, {'item1': 'bomb', 'item2': 't4', 'item3': '4'})

    def test_iloc_set2(self):
        df = lpd.read_csv('test2.csv')
        col_idx = list(df.columns).index('item3')
        df.iloc[0, col_idx] = 't9'
        self.assertEqual(df.iloc[0]['item3'], 't9')

    def test_df_append(self):
        org_df = lpd.read_csv('test2.csv')
        new_df = lpd.DataFrame(columns=org_df.columns)
        for idx in range(len(org_df)):
            new_df = new_df.append(org_df.iloc[idx])
        self.assertEqual(len(org_df), len(new_df))

    def test_col_pick(self):
        df = lpd.read_csv('test.csv')
        row_li = list(df['item1'])
        self.assertListEqual(row_li, ['t1', 't3', 't5', 't7', 't9'])

    def test_col_mul_pick(self):
        df = lpd.read_csv('test.csv')
        row_li = df[['item1', 'item3']]
        self.assertListEqual(list(row_li['item1']), ['t1', 't3', 't5', 't7', 't9'])
        self.assertListEqual(list(row_li['item3']), ['2', '4', '6', '8', '22'])

    def test_loc_gt_pick(self):
        df = lpd.read_csv('test.csv')
        pick_df = df.loc[df['item1'] > 't4']
        row_line = dict(pick_df.iloc[1])
        self.assertDictEqual(row_line, {'item1': 't7', 'item2': 't8', 'item3': '8'})

    def test_loc_gt_pick2(self):
        df = lpd.read_csv('test.csv')
        pick_df = df[df['item1'] > 't4']
        row_line = dict(pick_df.iloc[1])
        self.assertDictEqual(row_line, {'item1': 't7', 'item2': 't8', 'item3': '8'})

    def test_loc_lt_pick(self):
        df = lpd.read_csv('test.csv')
        pick_df = df.loc[df['item1'] < 't4']
        row_line = dict(pick_df.iloc[1])
        self.assertDictEqual(row_line, {'item1': 't3', 'item2': 't4', 'item3': '4'})

    def test_loc_mul_pick(self):
        df = lpd.read_csv('test.csv')
        row_line = list(df.loc[df['item1'] > 't4', 'item2'])
        self.assertListEqual(row_line, ['t6', 't8', 't0'])

    def test_loc_set(self):
        df = lpd.read_csv('test.csv')
        df.loc[df['item1'] > 't4', 'item2'] = 'bomb'
        row_line = dict(df.iloc[2])
        self.assertDictEqual(row_line, {'item1': 't5', 'item2': 'bomb', 'item3': '6'})

    def test_loc_set_and_create(self):
        df = lpd.read_csv('test.csv')
        df.loc[df['item1'] > 't4', 'item4'] = 'bomb'
        row_line = dict(df.iloc[2])
        self.assertDictEqual(row_line, {'item1': 't5', 'item2': 't6', 'item3': '6', 'item4': 'bomb'})

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
        self.assertDictEqual(row_line, {'item1': 't9', 'item2': 't0', 'item3': '22'})

    def test_df_drop_duplicates(self):
        df = lpd.read_csv('test.csv')
        df = df.append({'item1': 't3', 'item2': 't4', 'item3': '4'}, ignore_index=True)
        df = df.drop_duplicates()
        self.assertEqual(len(df), 5)

    def test_df_drop_duplicates_with_subset(self):
        df = lpd.read_csv('test.csv')
        df = df.append({'item1': 't3', 'item2': 't4', 'item3': '4'}, ignore_index=True)
        df = df.append({'item1': 't3', 'item2': 't1', 'item3': '4'}, ignore_index=True)
        df1 = df.drop_duplicates(['item1'])
        df2 = df.drop_duplicates(['item1', 'item2'])
        self.assertEqual(len(df1), 5)
        self.assertEqual(len(df2), 6)

    def test_export_to_pd(self):
        df = lpd.read_csv('test.csv')
        pd_df = df.export_to_pandas()
        self.assertEqual(len(pd_df), 5)

    def test_import_from_pd(self):
        pd_df = pd.read_csv('test.csv', delimiter=',')
        df = lpd.import_from_pandas(pd_df)
        self.assertEqual(len(df), 5)

    def test_drop_one_column(self):
        df = lpd.read_csv('test.csv')
        new_df = df.drop('item2', axis=1)
        self.assertEqual(len(df.columns), 3)
        self.assertEqual(len(df.iloc[0]), 3)
        self.assertEqual(len(new_df.columns), 2)
        self.assertEqual(len(new_df.iloc[0]), 2)

    def test_drop_one_column_with_inplace(self):
        df = lpd.read_csv('test.csv')
        new_df = df.drop('item2', axis=1, inplace=True)
        self.assertEqual(len(df.columns), 2)
        self.assertEqual(new_df, None)

    def test_drop_columns(self):
        df = lpd.read_csv('test.csv')
        new_df = df.drop(['item1', 'item3'], axis=1)
        self.assertEqual(len(df.columns), 3)
        self.assertEqual(len(new_df.columns), 1)

    def test_drop_columns_with_inplace(self):
        df = lpd.read_csv('test.csv')
        new_df = df.drop(['item2', 'item3'], axis=1, inplace=True)
        self.assertEqual(len(df.iloc[0]), 1)
        self.assertEqual(len(df.columns), 1)
        self.assertEqual(new_df, None)

    def test_drop_one_row(self):
        df = lpd.read_csv('test.csv')
        new_df = df.drop(0, axis=0)
        self.assertEqual(len(df), 5)
        self.assertEqual(len(df.index), 5)
        self.assertEqual(len(new_df), 4)
        self.assertEqual(len(new_df.index), 4)

    def test_drop_one_row_with_inplace(self):
        df = lpd.read_csv('test.csv')
        new_df = df.drop(0, axis=0, inplace=True)
        self.assertEqual(len(df), 4)
        self.assertEqual(new_df, None)

    def test_drop_rows(self):
        df = lpd.read_csv('test.csv')
        new_df = df.drop([0, 2], axis=0)
        self.assertEqual(len(df), 5)
        self.assertEqual(len(new_df), 3)

    def test_drop_rows_with_inplace(self):
        df = lpd.read_csv('test.csv')
        new_df = df.drop([0, 2], axis=0, inplace=True)
        self.assertEqual(len(df), 3)
        self.assertEqual(new_df, None)

    def test_read_excel(self):
        df = lpd.read_excel('test.xlsx', sheet_name='TC01')
        self.assertEqual(len(df), 5)

    def test_create_df_from_dict(self):
        df = lpd.DataFrame({'item1': [1, 2], 'item2': [3, 4]}, columns=['item3'])
        self.assertEqual(len(df), 2)

    def test_create_df_from_dict2(self):
        df = lpd.DataFrame({'item1': [1, 2], 'item2': [3, 4, 5]}, columns=['item1'])
        self.assertEqual(len(df), 3)

    def test_append_list(self):
        df = lpd.read_csv('test.csv')
        other_list = ['t1', 't4', '20']
        df = df.append(other_list)
        self.assertEqual(len(df), 6)

    def test_append_df(self):
        df = lpd.read_csv('test.csv')
        other_df = lpd.DataFrame({'item3': ['3', '6'], 'item1': ['1', '4']}, columns=['item2'])
        df = df.append(other_df, ignore_index=True)
        self.assertEqual(len(df), 7)

    def test_huge_pick(self):
        start_ts = time.time()
        col1 = ['i1' for _ in range(100000)] + ['i2' for _ in range(100000)] + ['i3' for _ in range(100000)]
        col2 = [idx for idx in range(300000)]
        df = lpd.DataFrame({'item1': col1, 'item2': col2})
        created_ts = time.time()
        tmp_filter = df['item1'] == 'i2'
        mid_ts = time.time()
        tmp_df = df[tmp_filter]
        end_ts = time.time()
        self.assertEqual(len(tmp_df), 100000)
        self.assertLess((created_ts - start_ts), 0.2)
        self.assertLess((mid_ts - created_ts), 0.1)
        self.assertLess((end_ts - created_ts), 0.1)


if __name__ == '__main__':
    unittest.main()
