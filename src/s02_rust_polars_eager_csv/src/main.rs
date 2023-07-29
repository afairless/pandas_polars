use glob::glob;
use home;
use polars::prelude::*;
// use std::fs::File;
use std::path::PathBuf;

fn get_synthetic_data_path() -> PathBuf {
    // Returns path to directory where synthetic data is stored

    let mut path = home::home_dir().unwrap();
    path.push("data/s01_synthetic_data");
    path
}

fn load_key_table(dir_path: &PathBuf, file_extension: &str) -> DataFrame {
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
        let df = CsvReader::from_path(filepath).unwrap().finish().unwrap();
        df_key_vec.push(df);
    }

    let df_key = df_key_vec[0].clone();
    df_key
}

fn main() {
    let input_data_filepath = get_synthetic_data_path();
    let file_extension = ".csv";

    let df_key = load_key_table(&input_data_filepath, file_extension);

    let filename_pattern = format!(
        "{}{}{}",
        input_data_filepath.to_str().unwrap(),
        "/table_*",
        file_extension
    );
    let colnames = vec!["A".to_string(), "I".to_string(), "P".to_string()];

    let mut df_vec: Vec<DataFrame> = Vec::new();
    for item in glob(filename_pattern.as_str()).unwrap() {
        let filepath = item.unwrap();
        let df = CsvReader::from_path(filepath)
            .unwrap()
            .with_columns(Some(colnames.clone()))
            .finish()
            .unwrap();
        df_vec.push(df);
    }

    let mut df_all_01 = DataFrame::empty();
    for df in df_vec {
        df_all_01.vstack_mut(&df).unwrap();
    }
    let df_all_02 = df_all_01.select(colnames).unwrap();
    let df_all_03 = df_all_02
        .join(&df_key, ["A"], ["key"], JoinType::Left, None)
        .unwrap();

    // let df_all_04 = df_all_03.groupby(["A"]).unwrap().mean().unwrap();
    // using 'mean' directly (above) gets wrong answers, even though using 'sum'
    //  and 'count' separately each get correct answers
    let df_all_04 = df_all_03
        .groupby(["A"])
        .unwrap()
        .apply(|x| Ok(x.mean()))
        .unwrap();
    println!("means\n {:?}", df_all_04.mean());
}
