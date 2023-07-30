# # How fast is Polars for realistic data processing jobs?

# + [markdown]
'''
The dataframe library [Pandas](https://pandas.pydata.org/) has long been a 
    workhorse for data scientists, ML engineers, and other data professionals.  
    It has a broad, convenient, user-friendly API for transforming and 
    summarizing data, which allows us to quickly discern insights from our data 
    and engineer data pipelines for production.  But how quickly, exactly?  A 
    new dataframe library [Polars](https://www.pola.rs/) [claims to be much 
    faster](https://www.pola.rs/benchmarks.html) at processing data than Pandas 
    and other common tools.

I was curious about how much faster Polars might be not only on benchmarks but 
    also as part of realistic data workflows.  So, I created some data to be 
    processed by Pandas or Polars.  Since file-reading is typically among the 
    slower parts of a data pipeline, I stored the data in either the older, 
    ubiquitous [comma-separated-values 
    (CSV)](https://en.wikipedia.org/wiki/Comma-separated_values) format or the 
    newer, column-based [
    Parquet](https://en.wikipedia.org/wiki/Apache_Parquet) format. 
    Additionally, Polars can be used from either 
    [Python](https://pola-rs.github.io/polars/py-polars/html/reference/) or 
    [Rust](https://pola-rs.github.io/polars/polars/), so comparing the workflow 
    in those two languages would be useful.  Finally, Polars jobs can be 
    processed [eagerly or 
    lazily](https://pola-rs.github.io/polars-book/user-guide/concepts/lazy-vs-eager/), 
    so we can compare both execution approaches in our workflow.  

To summarize, we'll compare:

- Pandas vs. **Polars**
- Python Polars vs. **Rust Polars**
- CSV vs. **Parquet**
- Polars Eager vs. **Polars Lazy**

For each comparison pair in the list above, the bolded variation is the one I 
    expected to be faster.  That is, I expected Polars to be faster than Pandas, 
    Rust to be faster than Python, Parquet to be faster than CSV, and Lazy to be 
    faster than Eager.
'''
# -

# ## Creating the Data

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

# + [markdown]
'''
I created a data set of 100 tables, each with 100,000 rows and 20 columns 
    ([lines 88-90](https://github.com/afairless/pandas_polars/blob/main/src/s01_create_data/create_data.py)). 
    The 100 CSV files occupied 2.0 GB of space on my hard drive, while the 100 
    Parquet files occupied 1.0 GB.  The first 5 columns of each table were 
    composed of randomly selected characters; the next 5 columns, of random 
    integers; and the final 10 columns, of random floats.  An example is shown 
    below.
'''
# -

input_filepath = Path.home() / 'data' / 's01_synthetic_data' / 'table_0.csv'
df_0 = pd.read_csv(input_filepath)
df_0

# + [markdown]
'''
All 100 tables like this example above have the same column names and the same
    data types for their corresponding columns.  After loading all these data
    tables, the first data processing step will be to vertically concatenate
    all these tables into a single, 10-million-row table (i.e., 100 tables x
    100,000 rows = 10,000,000 rows).

I created an additional *key* table with 26 rows and 4 columns 
    ([lines 50-72](https://github.com/afairless/pandas_polars/blob/main/src/s01_create_data/create_data.py)). 
    The first *key* column contains every letter of the alphabet, and the other 
    3 columns contain randomly selected integers.
'''
# -

synthetic_data_path = get_synthetic_data_path()
file_extension = '.csv'
df_key = load_key_table(synthetic_data_path, file_extension)
df_key

# + [markdown]
'''
The *key* table is shown above.  It is used in the second data processing step,
    when it is joined on its *key* column to a column of characters in the 
    10-million-row table.
'''
# -

# ## Data Processing Steps

# + [markdown]
'''
Only 3 columns from each of the 100 tables are used from the CSV or Parquet 
    files:  a column of characters, a column of integers, and a column of 
    floats.  An example is shown below.
'''
# -

filename_pattern = 'table_*' + file_extension
file_list = list(synthetic_data_path.glob(filename_pattern))
col_list = ['A', 'I', 'P']
df_list = [pd.read_csv(e, usecols=col_list) for e in file_list]
df_list[0].head()

# + [markdown]
'''
We've already mentioned the first two data processing steps after reading 
    the tables:  vertically concatenating the 100 tables and then joining the 
    *key* table to the resulting, concatenated table.  We then drop the *key* 
    column, because it is no longer needed after the join.  These 3 steps are
    shown below.
'''

# + [markdown]
'''
The vertically concatenated table is shown below.
'''
# -

df_all_01 = pd.concat(df_list, axis=0).reset_index(drop=True)
df_all_01

# + [markdown]
'''
The joined table is shown below.
'''
# -

df_all_02 = df_all_01.merge(df_key, left_on='A', right_on='key', how='left')
df_all_02

# + [markdown]
'''
The joined table without the *key* column is shown below.
'''
# -

df_all_03 = df_all_02.drop('key', axis=1)
df_all_03.head()

# + [markdown]
'''
The next step is to group the rows of the 10-million-row table by the column of 
    characters and calculate the means of the remaining 5 columns (2 columns 
    from the 100 tables and 3 columns from the *key* table) for each group. 
    The resulting table is shown below.
'''
# -

df_all_04 = df_all_03.groupby('A').mean()
df_all_04

# + [markdown]
'''
Finally, the means of the groups are themselves averaged for each column, and
    the results are printed.
'''
# -

print('means\n', df_all_04.mean())

# + [markdown]
'''
In summary, our steps are:

    1. Read 3 columns from each of the 100 tables
    2. Read the "key" table
    3. Vertically concatenate the 100 tables
    4. Join the "key" table to the concatenated table
    5. Drop the "key" column
    6. Group by the column of characters and calculate the group means
    7. Calculate the means of the group means for each column 
    8. Print the final set of means

These steps provide us with a set of tasks that we might see in a typical data 
    pipeline.  While the data values are randomly selected and thus not 
    meaningful, they do offer a mix of characters/strings, integers, and floats, 
    much like what we might find in a lot of data sets.

These steps are implemented for each of the processing approaches that we want
    to compare.
'''
# -

# ## Data Processing Variations

# + [markdown]
'''
As a reminder, we want to compare:

- Pandas vs. Polars
- Python Polars vs. Rust Polars
- CSV vs. Parquet
- Polars Eager vs. Polars Lazy

To do so, we need to implement the data processing steps for each variation that
    we want to compare:  Python Pandas loading CSV files, Python Polars loading
    Parquet files, and so on.

Here is a list of all the variations, which are laid out in the directory 
    [here](https://github.com/afairless/pandas_polars/tree/main/src):
'''
# -

path = Path.cwd().parent / 'src'
dir_list = list(path.glob('s02_*'))
var_list = [e.name[4:] for e in dir_list]
var_list.sort()

for e in var_list:
    print(e)

# ## Data Processing Timing

# + [markdown]
'''
To measure how long each data processing variation required, I ran each 
    variation 50 times 
    ([line 54](https://github.com/afairless/pandas_polars/blob/main/src/s03_time_all_processing/time_data_processing.py)) 
    and used the lowest recorded time.  Specifically, each measured time was the 
    [sum of the reported system time and the user 
    time](https://unix.stackexchange.com/questions/162115/why-does-the-user-and-sys-time-vary-on-multiple-executions).
    During timing runs, my machine wasn't running any web browsers, multimedia
    players, or other processing-intense applications, and I watched the system
    monitor to verify that no unexpected processes were using substantial system
    resources.
'''
# -

# ## Timing Results

# ### Pandas vs. Polars

# + [markdown]
'''
First we'll compare processing the data in Pandas dataframes versus in Polars
    dataframes.  

![image](./img/dataframe.png)

The barplot above shows the processing times for Pandas (blue) and Polars 
    (orange).  The gray bars are for Rust, which doesn't use Pandas, so they 
    aren't relevant for our comparison.  

When we compare "like" to "like", i.e., when we compare the Python-Pandas-CSV
    blue bar to the two Python-Polars-CSV orange bars (for Eager and Lazy), it 
    is clear that Polars processing times are lower than Python times. 
    Likewise, the corresponding comparison for the Parquet bars shows that 
    Polars is faster than Pandas.  This confirms what we anticipated from those 
    [benchmarks](https://www.pola.rs/benchmarks.html).
'''
# -

# ### Python Polars vs. Rust Polars

# + [markdown]
'''
Our next comparison is between Python-Polars and Rust-Polars.

![image](./img/language.png)

Python-Polars (blue) is faster than Rust-Polars (orange).  Since
    Rust is a compiled language and has a 
    [reputation](https://doc.rust-lang.org/book/ch00-00-introduction.html) 
    [for](https://github.com/niklas-heer/speed-comparison) 
    [speed](https://medium.com/@niklasbuechner/how-i-sped-up-my-rust-program-from-30-minutes-to-a-few-seconds-32a00509c7e), 
    this is quite surprising.

First, did I do something wrong?  That's entirely possible, since Rust and 
    Polars are fairly new to me.  I ensured that Rust compilation times were
    excluded from the measurement by first running the Rust program a few times
    manually after compilation to verify that it was optimized and speedy, and
    then by calling Cargo with the ["release" and "frozen" 
    parameters](https://doc.rust-lang.org/cargo/commands/cargo-run.html) during 
    measurement runs 
    ([line 51](https://github.com/afairless/pandas_polars/blob/main/src/s03_time_all_processing/time_data_processing.py)).

Are they fair comparisons?  My purpose was not to write the Python and Rust 
    versions so that they did the exact same lower-level computations.  After 
    all, Python and Rust are different languages:  I expect that they will do
    things somewhat differently "under the hood", even if I write very 
    similar-looking code.  My purpose was to write hopefully-straightforward 
    Pythonic and Rustacean code that implemented the same algorithmic ideas 
    without awkward gymnastics to make them exactly the "same".  I think my 
    implementations did that reasonably well.  Plus, most of the heavy 
    computational lifting is done by the same library Polars, so that may limit 
    the impact of my particular Pythonic or Rustacean choices.  So, I think the
    comparisons are fair, though someone else might produce fair comparisons
    that are rather different from mine.

Why is Rust-Polars slower than Python-Polars?  Let's look at the timing 
    measurements in more detail.
'''
# -

path = Path.cwd().parent / 'results'
table_filepath = path / 'min_times_df.csv'
df = pd.read_csv(table_filepath)
df['ratio'] = (df['user_system'] / df['elapsed']).round(2)
df

# + [markdown]
'''
Notice that the sum of the *user* and *system* times exceeds the *elapsed* times
    for both Python-Polars and Rust-Polars.  This suggests that Polars is
    parallelizing its workload ("[embarrassingly 
    parallel](https://www.pola.rs/)").  The ratios for *user*+*system*:*elapsed* 
    are sometimes higher for Rust-Polars than for Python-Polars -- specifically
    for the Lazy variations.  Does Rust-Polars incur more overhead for 
    parallelization operations than Python-Polars even while the workload isn't 
    heavy enough to justify this extra cost?  That's an interesting hypothesis. 
    But the differences in the ratios aren't that large, and small fluctuations 
    in the small *elapsed* denominators could alter the ratios substantially. 
    Additionally, the ratios for the Eager cases either favor Python or are 
    virtually identical, so we would need a better explanation that might
    account for both Eager and Lazy comparisons between the languages.

A deeper investigation might involve line-profiling the Python and Rust 
    variations of each comparison to locate the most time-consuming operations. 
    Absent that closer look, I would recommend not just assuming (as I did) that 
    one language would be faster than another for a particular use case:  if 
    optimization for speed is that important, it's worth testing the 
    alternatives to know for sure.
'''
# -


# ### CSV vs. Parquet

# + [markdown]
'''
Next we compare reading data from CSV files to reading from Parquet files.

![image](./img/filetype.png)

This comparison turned out as expected:  loading data from Parquet files 
    (orange) is faster than reading CSV files (blue).  And that was true for 
    every comparison pair:  Python-Pandas, Python-Polars-Eager, 
    Python-Polars-Lazy, Rust-Polars-Eager, and Rust-Polars-Lazy.  

One feature of Parquet files that surely makes them faster in this case is that
    they store data as columns, meaning that values that are adjacent in a 
    column are also adjacent in the file structure or in memory when they are
    read.  By contrast, CSV files store data in rows.  When only a subset of
    columns are loaded from a file, read times from Parquet are often fast
    because of this column-based storage.
'''
# -


# ### Polars Eager vs. Polars Lazy

# + [markdown]
'''
Finally we compare eager versus lazy execution in Polars.

![image](./img/execution.png)

There's no discernible difference between eager (blue) and lazy (orange) Polars 
    execution with the Python code.  This would make me question whether I 
    completely botched coding the Lazy implementation, except that for Rust,
    Lazy is indeed faster than Eager for both CSV and Parquet.  That's 
    reassuring; I must've done something right.  

Why is Lazy faster than Eager for Rust but not for Python?  Perhaps Rust's 
    compiler allows optimizations between Polars's Lazy execution mode and other 
    Rust code that the Python interpreter can't do.  A similar improvement in 
    Python might require a lot of manual investigation and optimization effort.
'''
# -

# ## Conclusion

# + [markdown]
'''
For the most part, the results were as I expected:  Polars is faster than 
    Pandas, Parquet is faster than CSV, and Lazy is faster than Eager. 

The major exception was that Python-Polars was faster than Rust-Polars when I
    expected that Rust would be faster than Python.  On the one hand, a computer
    scientist might scoff at the difference because it's less than an order of
    magnitude; in fact, Rust is more than 2-times slower than Python in only one
    case, and even that is only 2.5-times slower:
'''
# -

rust_to_python_ratios = [
    (df['program'].iloc[i], 
     df['program'].iloc[i+4], 
     df['user_system'].iloc[i+4] / 
     df['user_system'].iloc[i]) 
    for i in range(2, 6)]

ratio_df = pd.DataFrame(rust_to_python_ratios)
ratio_df.iloc[:, -1] = ratio_df.iloc[:, -1].round(2)
ratio_df

# + [markdown]
'''
On the other hand, a factor of 2 or 3 can provide important time savings in
    real-world applications, so optimizing for such a difference might be
    worthwhile.

Further investigation should focus on whether some minor adjustments to the Rust 
    code would greatly improve its performance or whether Python has a general 
    advantage for this type of data processing with Polars.
'''
