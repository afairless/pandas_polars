
# How fast is Polars compared to Pandas?

The code and artifacts in this repository document speed comparisons in running a realistic data processing task in the dataframe libraries Pandas and Polars and in the programming languages Python and Rust.  A brief summary is below.  A detailed description and results are available at the [notebook in this repository](https://github.com/afairless/pandas_polars/blob/main/notebook/pandas_polars.ipynb) or at the [webpage](https://afairless.com/pandas-vs-polars/).

## Introduction

The dataframe library [Pandas](https://pandas.pydata.org/) has long been a workhorse for data scientists, ML engineers, and other data professionals.  It has a broad, convenient, user-friendly API for transforming and summarizing data, which allows us to quickly discern insights from our data and engineer data pipelines for production.  But how quickly, exactly?  A new dataframe library [Polars](https://www.pola.rs/) [claims to be much faster](https://www.pola.rs/benchmarks.html) at processing data than Pandas and other common tools.

I was curious about how much faster Polars might be not only on benchmarks but also as part of realistic data workflows.  So, I created some data to be processed by Pandas or Polars.  Since file-reading is typically among the slower parts of a data pipeline, I stored the data in either the older, ubiquitous [comma-separated-values (CSV)](https://en.wikipedia.org/wiki/Comma-separated_values) format or the newer, column-based [ Parquet](https://en.wikipedia.org/wiki/Apache_Parquet) format. Additionally, Polars can be used from either [Python](https://pola-rs.github.io/polars/py-polars/html/reference/) or [Rust](https://pola-rs.github.io/polars/polars/), so comparing the workflow in those two languages would be useful.  Finally, Polars jobs can be processed [eagerly or lazily](https://pola-rs.github.io/polars-book/user-guide/concepts/lazy-vs-eager/), so we can compare both execution approaches in our workflow.  

To summarize, we'll compare:

- Pandas vs. **Polars**
- Python Polars vs. **Rust Polars**
- CSV vs. **Parquet**
- Polars Eager vs. **Polars Lazy**

For each comparison pair in the list above, the bolded variation is the one I expected to be faster.  That is, I expected Polars to be faster than Pandas, Rust to be faster than Python, Parquet to be faster than CSV, and Lazy to be faster than Eager.

## Conclusion

For the most part, the results were as I expected:  Polars is faster than Pandas, Parquet is faster than CSV, and Lazy is faster than Eager. 

The major exception was that Python-Polars was faster than Rust-Polars when I expected that Rust would be faster than Python.  Further investigation should focus on whether some minor adjustments to the Rust code would greatly improve its performance or whether Python has a general advantage for this type of data processing with Polars.
