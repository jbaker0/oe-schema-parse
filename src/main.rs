use std::fs;

use oe_schema_parse::{parse_df, write_df, write_fmt_df};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let file_contents = fs::read_to_string(&args[1])?;
    let input = file_contents.as_str();
    let results = parse_df(input);
    if let Ok(results) = results {
        if args.len() > 2 && &args[2] == "-json" {
           print!("{}", serde_json::to_string_pretty(&results)?);
        }
        else {
            print!("{}", write_fmt_df(&results));
        }
    }

    Ok(())
}
