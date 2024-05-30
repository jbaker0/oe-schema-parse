use std::fs;

use oe_schema_parse::{parse_df, write_df};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_contents = fs::read_to_string("../wyatt.df")?;
    let input = file_contents.as_str();
    let mut output = String::new();
    let results = parse_df(input);
    if let Ok(results) = results {
        write_df(results, &mut output)?;

        print!("{}", &output);
    }

    Ok(())
}
