use glob::glob;
use home;
use polars::prelude::*;
use std::fs::File;
use std::path::PathBuf;

fn get_synthetic_data_path() -> PathBuf {
    // Returns path to directory where synthetic data is stored

    let mut path = home::home_dir().unwrap();
    path.push("data/s01_synthetic_data");
    path
}

fn load_key_table(dir_path: &PathBuf, file_extension: &str) -> LazyFrame {
    // Load table with key column that joins to the main data set

    let filename_pattern = format!(
        "{}{}{}",
        dir_path.to_str().unwrap(),
        "/*table",
        file_extension
    );

    let mut df_key_vec: Vec<DataFrame> = Vec::new();
    for item in glob(filename_pattern.as_str()).unwrap() {
        let filepath = item.unwrap();
        let table_file = File::open(filepath).unwrap();
        let df = ParquetReader::new(table_file).finish().unwrap();
        df_key_vec.push(df);
    }

    let df_key = df_key_vec[0].clone();
    df_key.lazy()
}

fn main() {
    let input_data_filepath = get_synthetic_data_path();
    let file_extension = ".parquet";

    let df_key = load_key_table(&input_data_filepath, file_extension);

    let filename_pattern = format!(
        "{}{}{}",
        input_data_filepath.to_str().unwrap(),
        "/table_*",
        file_extension
    );
    let colnames1 = vec!["A", "I", "P"];
    let colnames2 = vec![
        colnames1[0].to_string(),
        colnames1[1].to_string(),
        colnames1[2].to_string(),
    ];

    let mut df_vec: Vec<LazyFrame> = Vec::new();
    for item in glob(filename_pattern.as_str()).unwrap() {
        let filepath = item.unwrap();
        let table_file = File::open(filepath).unwrap();
        let df = ParquetReader::new(table_file)
            .with_columns(Some(colnames2.clone()))
            .finish()
            .unwrap()
            .lazy();
        df_vec.push(df);
    }

    let df_all_01 = concat(&df_vec, false, false).unwrap();
    let df_all_02 = df_all_01
        .select(&[col(colnames1[0]), col(colnames1[1]), col(colnames1[2])])
        .join(df_key, vec![col("A")], vec![col("key")], JoinType::Left)
        .groupby(["A"])
        .agg([col("*").mean()])
        .collect()
        .unwrap();

    println!("means\n {:?}", df_all_02.mean());
}
