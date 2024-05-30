use std::fs;

use oe_schema_parse::{parse_df, print_df, Entity};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_contents = fs::read_to_string("../wyatt.df")?;
    let mut input = file_contents.as_str();

    let results = parse_df(&mut input);
    if let Ok(results) = results {
        print_df(results);
    }

    Ok(())
}
