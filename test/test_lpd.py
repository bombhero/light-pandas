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
        self.assertEqual(len(df), 2)
        self.assertListEqual(df.columns, ['item1', 'item2'])


if __name__ == '__main__':
    unittest.main()
