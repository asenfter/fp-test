import fastparquet as fp
import pandas as pd
import numpy as np
import tempfile
import unittest

from thrift.Thrift import TType


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

    @unittest.expectedFailure
    def test_first_read_string_then_float(self):
        """
        Read both parquet files using fastparquet in the following order:
        1. df with string value
        2. df with float value

        An error is expected.
        """
        files = [
            f"{self.path}/partition=s/part.0.parquet", # string data
            f"{self.path}/partition=f/part.0.parquet"  # float data
        ]
        self._read(files)

    def test_first_read_float_then_string(self):
        """
        Read both parquet files using fastparquet in the following order:
        1. df with float value
        2. df with string value

        No error is expected and the resulting dtype of the column 'value' is expected to be float64
        """
        files = [
            f"{self.path}/partition=f/part.0.parquet",  # float data
            f"{self.path}/partition=s/part.0.parquet"   # string data
        ]
        df = self._read(files)
        self.assertEqual(df['value'].dtype, 'float64')

    def test_first_read_string_then_float_fixed(self):
        """
        Read both parquet files using fastparquet in the following order:
        1. df with string value
        2. df with float value

        No error is expected and the resulting dtype of the column 'value' is expected to be object
        """
        files = [
            f"{self.path}/partition=s/part.0.parquet", # string data
            f"{self.path}/partition=f/part.0.parquet"  # float data
        ]
        df = self._read_fixed(files)
        self.assertEqual(df['value'].dtype, 'object')

    def test_first_read_float_then_string_fixed(self):
        """
        Read both parquet files using fastparquet in the following order:
        1. df with float value
        2. df with string value

        No error is expected and the resulting dtype of the column 'value' is expected to be object
        """
        files = [
            f"{self.path}/partition=f/part.0.parquet", # string data
            f"{self.path}/partition=s/part.0.parquet"  # float data
        ]
        df = self._read_fixed(files)
        self.assertEqual(df['value'].dtype, 'object')

    def _read(self, files):
        pf = fp.ParquetFile(files, root=self.path)
        self.assertEqual(pf.count, 2)
        df = pf.to_pandas()

        self.assertIs(len(df.index), 2)
        self.assertListEqual(df.columns.values.tolist(), ['value', 'partition'])
        return df

    def _read_fixed(self, files):
        pf = fp.ParquetFile(files, root=self.path)
        self.assertEqual(pf.count, 2)

        for schema in pf.fmd.schema:
            if schema.name == 'value':
                schema.converted_type=TType.VOID
                schema.type=TType.UTF8

        df = pf.to_pandas()

        self.assertIs(len(df.index), 2)
        self.assertListEqual(df.columns.values.tolist(), ['value', 'partition'])
        return df