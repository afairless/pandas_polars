import pandas as pd
from pathlib import Path


def get_synthetic_data_path() -> Path:
    """
    Returns path to directory where synthetic data is stored
    """

    path = Path.home() / 'data' / 's01_synthetic_data'
    path.mkdir(parents=True, exist_ok=True)

    return path  


def load_key_table(dir_path: Path, file_extension: str) -> pd.DataFrame:
    """
    Load table with key column that joins to the main data set
    """

    filename_pattern = '*table' + file_extension
    file_list = list(dir_path.glob(filename_pattern))
    df_key_list = [pd.read_csv(e) for e in file_list]
    df_key = df_key_list[0]

    return df_key


def main():
    """
    """

    synthetic_data_path = get_synthetic_data_path()
    file_extension = '.csv'

    df_key = load_key_table(synthetic_data_path, file_extension)

    filename_pattern = 'table_*' + file_extension
    file_list = list(synthetic_data_path.glob(filename_pattern))
    col_list = ['A', 'I', 'P']

    df_list = [pd.read_csv(e, usecols=col_list) for e in file_list]

    df_all_01 = pd.concat(df_list, axis=0).reset_index(drop=True)
    df_all_02 = df_all_01.merge(df_key, left_on='A', right_on='key', how='left')
    df_all_03 = df_all_02.drop('key', axis=1)
    df_all_04 = df_all_03.groupby('A').mean()

    print('means\n', df_all_04.mean())


if __name__ == '__main__':
    main()
