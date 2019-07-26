import fastparquet as fp
import pandas as pd
import numpy as np
import tempfile
import unittest


def write_partitioned(input_data, out_path) -> None:
    """
    create a pandas DataFrame and use fp partitioned writing functionality to save it
    """
    df = pd.DataFrame([input_data], columns=['value', 'partition'])
    fp.write(out_path, data=df, compression=None, partition_on=['partition'], file_scheme='hive', write_index=False)


class FastParquetTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        """
        create two simple DataFrames each consisting of two columns ['value', 'partition']. The DataFrames differ
        in the dtype in the column 'value'. One uses 'str' and the second uses 'float'.

        DataFrame 1 with string value stored at {tmp_path}/partition=s/part.0.parquet

        |  value  | partition |
        +---------+-----------+
        | '47.11' |     's'   |

        DataFrame 2 with float value stored at {tmp_path}/partition=f/part.0.parquet

        |  value  | partition |
        +---------+-----------+
        |  47.11  |     'f'   |

        """
        cls.path = tempfile.mkdtemp()
        # creates {out_path}/partition=s/part.0.parquet containing a single column 'value' with a single row
        write_partitioned(input_data=["47.11", 's'], out_path=cls.path)
        # creates {out_path}/partition=f/part.0.parquet containing a single column 'value' with a single row
        write_partitioned(input_data=[47.11, 'f'], out_path=cls.path)

    def test_dtype_of_str_value(self):
        df = fp.ParquetFile([f"{self.path}/partition=s/part.0.parquet"], root=self.path).to_pandas()
        self.assertTrue(df.dtypes['value'] == 'object')
        self.assertTrue(type(df['value'].values[0]) == str)

    def test_dtype_of_float_value(self):
        df = fp.ParquetFile([f"{self.path}/partition=f/part.0.parquet"], root=self.path).to_pandas()
        self.assertTrue(df.dtypes['value'] == 'float64')
        self.assertTrue(type(df['value'].values[0]) == np.float64)

    def test_first_read_string_then_float(self):
        """
        Read both parquet files using fastparquet in the following order:
        1. df with string value
        2. df with float value
        """
        files = [
            f"{self.path}/partition=s/part.0.parquet", # string data
            f"{self.path}/partition=f/part.0.parquet"  # float data
        ]
        pf = fp.ParquetFile(files, root=self.path)
        self.assertEqual(pf.count, 2)
        df = pf.to_pandas()

    def test_first_read_float_then_string(self):
        """
        Read both parquet files using fastparquet in the following order:
        1. df with float value
        2. df with string value
        """
        files = [
            f"{self.path}/partition=f/part.0.parquet",  # float data
            f"{self.path}/partition=s/part.0.parquet"   # string data
        ]
        pf = fp.ParquetFile(files, root=self.path)
        self.assertEqual(pf.count, 2)
        df = pf.to_pandas()

        self.assertIs(len(df.index), 2)
        self.assertListEqual(df.columns.values.tolist(), ['value', 'partition'])
        self.assertEqual(df['value'].dtype, 'float64')
