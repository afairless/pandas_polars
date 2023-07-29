# /usr/bin/env python3

from pathlib import Path
import pandas as pd
from numpy import nan as np_nan
import matplotlib.pyplot as plt


def create_plot_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create dataframe suitable for plotting by dividing the single string that
        labels each condition/variation into individual labels
    """

    plot_df = pd.DataFrame(
        df.iloc[:, 0].apply(lambda x: x.split('_')).to_list()).iloc[:, 1:]

    colnames = ['language', 'dataframe', 'execution', 'filetype']
    plot_df.columns = colnames

    # Python Polars variations do not have 'eager' and 'lazy' executions;
    #   correct the misplaced labels
    plot_df.iloc[:2, -1] = plot_df.iloc[:2, -2]
    plot_df.iloc[:2, -2] = np_nan
    
    # create pretty labels to display on the plot
    plot_df['label'] = (
        df.iloc[:, 0]
        .str[4:]
        .str.replace('_', ', ')
        .str.title()
        .str.replace('Csv', 'CSV'))

    plot_df['time'] = df.iloc[:, -1]

    return plot_df


def create_bar_color_lists(df: pd.DataFrame) -> dict[str, list[str]]:
    """
    Create dictionary identifying the colors for each bar in the barplot,
        depending on which conditions/variations are being compared
    """

    # bars that are irrelevant to the comparison are marked in gray
    col1 = 'darkorange'
    col2 = 'blue'
    col3 = 'gray'

    cols_colors = {}
    # each set of comparisons is specified by the 'colname'
    for colname in df.columns[:4]:
        col_values = df[colname].unique()
        color_map = {col_values[0]: col1, col_values[1]: col2, np_nan: col3}
        col_colors = df[colname].map(color_map).to_list()
        cols_colors.update({colname: col_colors})


    # ad hoc corrections to mark irrelevant bars
    cols_colors['language'][-1] = col3
    cols_colors['language'][-2] = col3

    for i in range(4):
        cols_colors['dataframe'][i] = col3

    return cols_colors


def main():

    path = Path.cwd().parent.parent / 'results'
    filepath = path / 'min_times_df.csv'

    df = pd.read_csv(filepath)
    assert isinstance(df, pd.DataFrame)

    plot_df = create_plot_df(df)
    
    # horizontal bar plot plots first row of dataframe at bottom of plot; 
    #   reverse the row indices so that the "first" row is at the top
    reverse_idx = list(range(len(plot_df)))[::-1]
    plot_df = plot_df.iloc[reverse_idx, :]

    # figsize=(6.4*2, 4.8)
    # _, ax = plt.subplots(figsize=figsize)

    cols_colors = create_bar_color_lists(plot_df)

    notebook_path = Path.cwd().parent.parent / 'notebook' / 'img'
    notebook_path.mkdir(exist_ok=True)

    for colname in plot_df.columns[:4]:

        plt.grid(axis='x', zorder=0)
        plt.barh(
            plot_df['label'], 
            plot_df['time'], 
            zorder=2, 
            color=cols_colors[colname])
        plt.xlabel('Run time (sec)')
        plt.tight_layout()

        output_filepath = path / (colname + '.png')
        plt.savefig(output_filepath)

        output_filepath = notebook_path / (colname + '.png')
        plt.savefig(output_filepath)

        plt.clf()
        plt.close()


if __name__ == '__main__':
    main()
