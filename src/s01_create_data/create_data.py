from pathlib import Path
import numpy as np
import pandas as pd
import pyarrow as pa


# SET INPUT AND OUTPUT DIRECTORY PATHS
##############################

def get_synthetic_data_path() -> Path:
    """
    Returns path to directory where synthetic data will be stored
    """

    path = Path.home() / 'data' / 's01_synthetic_data'
    path.mkdir(parents=True, exist_ok=True)

    return path  


def generate_table01(
    rows_n: int, cols_n: int, alpha_n: int,  
    uppercase_alpha_dict: dict[int, str], 
    lowercase_alpha_dict: dict[int, str]) -> pd.DataFrame:
    """
    Creates a Pandas DataFrame with columns of floats, integers, and characters
    """

    cols_n_half = cols_n // 2
    cols_n_quarter = cols_n_half // 2
    half_shape = (rows_n, cols_n_half)

    # create columns of both floats and integers
    df_flt = pd.DataFrame(np.random.rand(rows_n, cols_n_half))
    df_int = pd.DataFrame(np.random.randint(1, alpha_n+1, half_shape))
    
    df = pd.concat([df_int, df_flt], axis=1)
    
    # create columns of characters
    df.iloc[:, :cols_n_quarter] = (
        df.iloc[:, :cols_n_quarter].replace(lowercase_alpha_dict))

    # create column names
    colnames = [uppercase_alpha_dict[i] for i in range(1, cols_n+1)]
    df.columns = colnames

    return df


def generate_table02(
    cols_n: int, alpha_n: int, 
    lowercase_alpha_dict: dict[int, str]) -> pd.DataFrame:
    """
    Creates Pandas DataFrame with a 'key' column with each lowercase letter of
        the alphabet (i.e., the dataframe as 26 rows) and some additional 
        columns of integers
    """

    alphabet_column = [lowercase_alpha_dict[i] for i in range(1, alpha_n+1)]

    df_shape = (len(alphabet_column), cols_n)
    # colnames = ['c' + str(i) for i in range(cols_n)]
    df = pd.DataFrame(np.random.randint(-5, -1, df_shape))

    df['key'] = alphabet_column 

    # re-order columns so that 'key' column is leftmost column
    colnames = list(df.columns)
    colnames = colnames[-1:] + colnames[:-1]
    df = df[colnames]

    return df


def main():
    """
    Generate and save synthetic structured data tables
    """

    alpha_n = 26
    uppercase_alpha_dict = {i:chr(ord('@')+i) for i in range(1, alpha_n+1)}
    lowercase_alpha_dict = {i:chr(ord('`')+i) for i in range(1, alpha_n+1)}


    # GENERATE TABLES
    ##############################

    df_n = 100
    df_rows_n = 100_000
    df_cols_n = 20

    df = generate_table01(
        df_rows_n, df_cols_n, alpha_n, 
        uppercase_alpha_dict, lowercase_alpha_dict)


    # SAVE TABLES
    ##############################

    synthetic_data_path = get_synthetic_data_path()

    for i in range(df_n):

        df_filename = 'table_' + str(i) + '.csv'
        df_filepath = synthetic_data_path / df_filename 
        df.to_csv(df_filepath, index=False)

        df_filename = 'table_' + str(i) + '.parquet'
        df_filepath = synthetic_data_path / df_filename 
        table = pa.Table.from_pandas(df)
        pa.parquet.write_table(table, df_filepath)


    # GENERATE KEY TABLE
    ##############################

    df_key_cols_n = 3
    df_key = generate_table02(df_key_cols_n, alpha_n, lowercase_alpha_dict)


    # SAVE KEY TABLE
    ##############################

    df_key_filename = 'key_table.csv'
    df_key_filepath = synthetic_data_path / df_key_filename 
    df_key.to_csv(df_key_filepath, index=False)

    df_key_filename = 'key_table.parquet'
    df_key_filepath = synthetic_data_path / df_key_filename 
    table = pa.Table.from_pandas(df_key)
    pa.parquet.write_table(table, df_key_filepath)


if __name__ == '__main__':
    main()
