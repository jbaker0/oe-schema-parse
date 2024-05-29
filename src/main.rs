use std::fs;

use oe_df_parse::{parse_df, Entity};
use serde_json::to_string_pretty;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_contents = fs::read_to_string("../wyatt.df")?;
    let mut input = file_contents.as_str();

    let results = parse_df(&mut input);
    if let Ok(results) = results {
        for result in results {
            match result {
                Entity::Table(table) => {
                    for field in &table.fields {
                        println!("{}", to_string_pretty(field)?);
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}
