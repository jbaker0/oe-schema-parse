use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};
use winnow::ascii::{multispace0, multispace1, newline, space0, space1, till_line_ending};
use winnow::combinator::{
    alt, delimited, fail, iterator, not, opt, peek, preceded, repeat_till, seq, terminated, trace,
};
use winnow::error::ParserError;
use winnow::stream::AsChar;
use winnow::token::{any, take_while};
use winnow::{dispatch, PResult, Parser};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Entity<'a> {
    #[serde(borrow)]
    #[serde(rename_all = "SCREAMING-KEBAB-CASE")]
    Database(Database<'a>),
    Table(Table<'a>),
    Field(Field<'a>),
    Index(Index<'a>),
    Sequence(Sequence<'a>),
    End(End<'a>),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DataType {
    Logical,
    Character,
    Integer,
    Int64,
    Decimal,
    Date,
    DateTime,
    RecId,
    Raw,
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DataType::Logical => write!(f, "logical"),
            DataType::Character => write!(f, "character"),
            DataType::Integer => write!(f, "integer"),
            DataType::Int64 => write!(f, "int64"),
            DataType::Decimal => write!(f, "decimal"),
            DataType::Date => write!(f, "date"),
            DataType::DateTime => write!(f, "datetime"),
            DataType::RecId => write!(f, "recid"),
            DataType::Raw => write!(f, "raw"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE", rename = "DATABASE")]
pub struct Database<'a> {
    pub name: &'a str,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE", rename = "TABLE")]
pub struct Table<'a> {
    pub name: &'a str,
    pub area: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valexp: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valmsg: Option<&'a str>,
    pub dump_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_triggers: Option<Vec<&'a str>>,
    pub fields: Vec<Field<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "")]
    pub indices: Option<Vec<Index<'a>>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE", rename = "FIELD")]
pub struct Field<'a> {
    pub name: &'a str,
    #[serde(rename = "AS")]
    pub r#type: DataType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'a str>,
    pub format: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format_sa: Option<&'a str>,
    pub initial: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_sa: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label_sa: Option<&'a str>,
    pub position: i32,
    pub sql_width: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub can_read: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub can_write: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_as: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_label: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column_label_sa: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valexp: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valmsg: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valmsg_sa: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help_sa: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extent: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decimals: Option<i32>,
    pub order: i32,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub case_sensitive: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub mandatory: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_trigger: Option<&'a str>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE", rename = "INDEX")]
pub struct Index<'a> {
    pub name: &'a str,
    pub area: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'a str>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub unique: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub inactive: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub primary: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub word: bool,
    #[serde(rename = "")]
    pub fields: Vec<IndexField<'a>>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE", rename = "INDEX-FIELD")]
pub struct IndexField<'a> {
    pub name: &'a str,
    pub order: SortOrder,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub abbreviated: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum SortOrder {
    Ascending,
    Descending,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE", rename = "SEQUENCE")]
pub struct Sequence<'a> {
    pub name: &'a str,
    pub initial: i32,
    pub increment: i32,
    pub cycle_on_limit: bool,
    pub min_val: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub struct End<'a> {
    codepage: &'a str,
    length: i32,
}

pub(crate) fn trim<'a, F, O, E>(inner: F) -> impl Parser<&'a str, O, E>
where
    E: ParserError<&'a str>,
    F: Parser<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

pub(crate) fn trim_quotes<'a, F, O, E>(inner: F) -> impl Parser<&'a str, O, E>
where
    E: ParserError<&'a str>,
    F: Parser<&'a str, O, E>,
{
    delimited(opt('"'), inner, opt('"'))
}

pub fn dispatch<'a>(input: &mut &'a str) -> PResult<Entity<'a>> {
    trim(dispatch! {peek(until_whitespace);
    "UPDATE" => parse_start,
    "ADD" => parse_add,
    "." => parse_file_end,
    _ => fail::<_, Entity, _>,
    })
    .parse_next(input)
}

pub fn parse_start<'a>(input: &mut &'a str) -> PResult<Entity<'a>> {
    let _ = delimited(multispace0, "UPDATE DATABASE", multispace0).parse_next(input)?;
    Ok(Entity::Database(Database {
        name: trim_quotes(take_while(0.., |c: char| c != '"')).parse_next(input)?,
    }))
}

pub fn parse_add<'a>(input: &mut &'a str) -> PResult<Entity<'a>> {
    delimited(multispace0, "ADD", multispace0).parse_next(input)?;

    dispatch! {peek(until_whitespace);
      "TABLE" => parse_table,
      "SEQUENCE" => parse_sequence,
      _ => fail::<_, Entity, _>,
    }
    .parse_next(input)
}

pub fn parse_table<'a>(input: &mut &'a str) -> PResult<Entity<'a>> {
    delimited(multispace0, "TABLE", multispace0).parse_next(input)?;
    /* if let Some(line) = input.lines().next() {
        println!("{}", line)
    } */

    Ok(Entity::Table(seq!{Table {
        name: trim_quotes(take_while(0.., |c: char| c != '"')),
        area: preceded(keyword_trim("AREA"), delimited('"', take_while(0.., |c: char| c != '"'), '"')),
        label: opt(preceded(keyword_trim("LABEL"), trim_quotes(until_table_keyword_or_new))),
        description: opt(trim_quotes(trim(preceded(keyword_trim("DESCRIPTION"), until_table_keyword_or_new)))),
        valexp: opt(preceded(keyword_trim("VALEXP"), trim_quotes(until_table_keyword_or_new))),
        valmsg: opt(preceded(keyword_trim("VALMSG"), trim_quotes(until_table_keyword_or_new))),
        dump_name: preceded(keyword_trim("DUMP-NAME"), trim_quotes(until_table_keyword_or_new)),
        table_triggers: opt(repeat_till(0.., preceded(multispace0, preceded("TABLE-TRIGGER", take_while(0.., |c:char|!c.is_newline()))), trace("ADD_NOT_INDEX", peek(preceded(take_while(0..,|c: char| c.is_newline()), ("ADD ", not("INDEX")))))).map(|x: (Vec<&str>, (&str, ())) | x.0)),
        fields: repeat_till(0.., preceded(multispace0, parse_field), trace("ADD_NOT_FIELD", peek(preceded(take_while(0..,|c: char| c.is_newline()), ("ADD ", not("FIELD")))))).map(|x: (Vec<Field>, (&str, ())) | x.0),
        indices: opt(parse_indices),
    }
    }.parse_next(input)?))
}

pub fn parse_index<'a>(input: &mut &'a str) -> PResult<Index<'a>> {
    delimited(multispace0, "ADD INDEX", multispace0).parse_next(input)?;

    

    //println!("{:?}", index);

    seq! {
        Index {
            name: terminated(delimited('"',take_while(0.., |c: char| c != '"'),'"'), seq!(space0, "ON", space0, delimited('"', take_while(0.., |c: char| c != '"'), '"'))),
            area: delimited(
                seq!(multispace0, "AREA", space1),
                trim_quotes(take_while(0.., |c: char| c != '"')),
                space0
            ),
            unique: delimited(multispace0, opt("UNIQUE"), space0).map(|x| x.is_some()),
            inactive: delimited(multispace0, opt("INACTIVE"), space0).map(|x| x.is_some()),
            primary: delimited(multispace0, opt("PRIMARY"), space0).map(|x| x.is_some()),
            description: opt(preceded(
                seq!(multispace0, "DESCRIPTION", space1),
                take_while(0.., |c: char| !c.is_newline())
            )),
            word: delimited(multispace0, opt("WORD"), space0).map(|x| x.is_some()),
            fields: repeat_till(
                0..,
                seq!(
                    preceded(
                        seq!(multispace0, "INDEX-FIELD", space1),
                        delimited('"', take_while(0.., |c: char| c != '"'), '"')
                    ),
                    delimited(space0, alt(("ASCENDING", "DESCENDING")), space0),
                    opt(delimited(space0, "ABBREVIATED", space0))
                ).map(|(name, order, abbrev)| IndexField {
                    name,
                    order: match order {
                        "ASCENDING" => SortOrder::Ascending,
                        "DESCENDING" => SortOrder::Descending,
                        _ => unreachable!(),
                    },
                    abbreviated: abbrev.is_some()
                }),
                peek(seq!(newline, newline))
            ).map(|x| x.0)
        }
    }.parse_next(input)
}

pub fn parse_field<'a>(input: &mut &'a str) -> PResult<Field<'a>> {
    delimited(multispace0, "ADD FIELD", multispace0).parse_next(input)?;

    /* if let Some(line) = input.lines().next() {
        println!("{}", line)
    } */

    

    /* if let Ok(field) = &field {
        if let Some(field) = &field.description {
            println!("{:?}", field.trim());
        }
    }; */

    seq!{Field {
        name: terminated(delimited('"',take_while(0.., |c: char| c != '"'), '"'), seq!(multispace0, "OF", multispace0, delimited('"',take_while(0.., |c: char| c != '"'), '"'))),
        r#type: preceded(seq!(multispace0, "AS", multispace1), alt(("logical", "character", "integer", "int64", "decimal", "date", "datetime", "recid", "raw"))).map(|x| match x {
            "logical" => DataType::Logical,
            "character" => DataType::Character,
            "integer" => DataType::Integer,
            "int64" => DataType::Int64,
            "decimal" => DataType::Decimal,
            "date" => DataType::Date,
            "datetime" => DataType::DateTime,
            "recid" => DataType::RecId,
            "raw" => DataType::Raw,
            _ => unreachable!(),
        }),
        description: opt(preceded(keyword_trim("DESCRIPTION"), delimited(multispace0, until_field_keyword_or_new.recognize(), multispace1))),
        format: preceded(keyword_trim("FORMAT"), trim_quotes(until_field_keyword_or_new)),
        format_sa: opt(preceded(keyword_trim("FORMAT-SA"), trim_quotes(until_field_keyword_or_new))),
        initial: preceded(keyword_trim("INITIAL"), trim_quotes(until_field_keyword_or_new)),
        initial_sa: opt(preceded(keyword_trim("INITIAL-SA"), trim_quotes(until_field_keyword_or_new))),
        label: opt(preceded(keyword_trim("LABEL"), trim_quotes(until_field_keyword_or_new))),
        label_sa: opt(preceded(keyword_trim("LABEL-SA"), trim_quotes(until_field_keyword_or_new))),
        position: preceded(keyword_trim("POSITION"), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
        sql_width: preceded(keyword_trim("SQL-WIDTH"), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
        can_read: opt(preceded(keyword_trim("CAN-READ"), trim_quotes(until_field_keyword_or_new))),
        can_write: opt(preceded(keyword_trim("CAN-WRITE"), trim_quotes(until_field_keyword_or_new))),
        view_as: opt(preceded(keyword_trim("VIEW-AS"), trim_quotes(until_field_keyword_or_new))),
        column_label: opt(preceded(keyword_trim("COLUMN-LABEL"), trim_quotes(until_field_keyword_or_new))),
        column_label_sa: opt(preceded(keyword_trim("COLUMN-LABEL-SA"), trim_quotes(until_field_keyword_or_new))),
        valexp: opt(preceded(keyword_trim("VALEXP"), trim_quotes(until_field_keyword_or_new))),
        valmsg: opt(preceded(keyword_trim("VALMSG"), trim_quotes(until_field_keyword_or_new))),
        valmsg_sa: opt(preceded(keyword_trim("VALMSG-SA"), trim_quotes(until_field_keyword_or_new))),
        help: opt(preceded(keyword_trim("HELP"), trim_quotes(until_field_keyword_or_new))),
        help_sa: opt(preceded(keyword_trim("HELP-SA"), trim_quotes(until_field_keyword_or_new))),
        extent: opt(preceded(keyword_trim("EXTENT"), take_while(0.., |c: char| c.is_ascii_digit()).parse_to())),
        decimals: opt(preceded(keyword_trim("DECIMALS"), take_while(0.., |c: char| c.is_ascii_digit()).parse_to())),
        order: preceded(keyword_trim("ORDER"), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
        case_sensitive: opt(keyword_trim("CASE-SENSITIVE")).map(|x| x.is_some()),
        mandatory: opt(keyword_trim("MANDATORY")).map(|x| x.is_some()),
        field_trigger: opt(preceded(seq!(multispace0, "FIELD-TRIGGER", space1), take_while(0.., |c: char| ! c.is_newline()))),
    }}.parse_next(input)
}

pub(crate) fn keyword_trim<'a, F, O, E>(inner: F) -> impl Parser<&'a str, O, E>
where
    E: ParserError<&'a str>,
    F: Parser<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace1)
}

pub fn until_field_keyword_or_new<'a>(input: &mut &'a str) -> PResult<&'a str> {
    repeat_till(0.., any, peek(alt((field_keyword, add))))
        .map(|x: (String, &str)| x.1)
        .recognize()
        .parse_next(input)
}

pub fn until_table_keyword_or_new<'a>(input: &mut &'a str) -> PResult<&'a str> {
    repeat_till(0.., any, peek(alt((table_keyword, add))))
        .map(|x: (String, &str)| x.1)
        .recognize()
        .parse_next(input)
}

pub fn parse_indices<'a>(input: &mut &'a str) -> PResult<Vec<Index<'a>>> {
    repeat_till(
        0..,
        parse_index,
        peek(alt((add_not_index_succeed, parse_file_end_succeed))),
    )
    .map(|x: (Vec<Index>, bool)| x.0)
    .parse_next(input)
}

pub fn add_not_index<'a>(input: &mut &'a str) -> PResult<&'a str> {
    peek(preceded(multispace0, ("ADD ", not("INDEX"))))
        .map(|x| x.0)
        .parse_next(input)
}

pub fn add_not_index_succeed(input: &mut &str) -> PResult<bool> {
    peek(preceded(multispace0, ("ADD ", not("INDEX"))))
        .map(|_| true)
        .parse_next(input)
}

pub fn parse_file_end<'a>(input: &mut &'a str) -> PResult<Entity<'a>> {
    let _ = seq!(multispace0, ".", multispace0, "PSC", multispace0).parse_next(input)?;
    Ok(Entity::End(
        seq! { End{
            codepage: delimited("cpstream=", till_line_ending, (multispace0, ".")),
            length: preceded(multispace0, till_line_ending.parse_to())
        }}
        .parse_next(input)?,
    ))
}

pub fn parse_file_end_succeed(input: &mut &str) -> PResult<bool> {
    let _ = seq!(multispace0, ".", multispace0, "PSC", multispace0).parse_next(input)?;
    seq! { End{
        codepage: delimited("cpstream=", till_line_ending, (multispace0, ".")),
        length: preceded(multispace0, till_line_ending.parse_to())
    }}
    .map(|x: End| {
        x.codepage == "ISO8859-1"
    })
    .parse_next(input)
}

pub fn field_keyword<'a>(input: &mut &'a str) -> PResult<&'a str> {
    preceded(
        alt(("  ", "\n\n")),
        alt((
            "DESCRIPTION",
            "FORMAT",
            "INITIAL",
            "LABEL",
            "POSITION",
            "SQL-WIDTH",
            "CAN-READ",
            "CAN-WRITE",
            "VIEW-AS",
            "COLUMN-LABEL",
            "VALEXP",
            "VALMSG",
            "HELP",
            "EXTENT",
            "DECIMALS",
            "ORDER",
            "CASE-SENSITIVE",
            "MANDATORY",
            "FIELD-TRIGGER",
        )),
    )
    .parse_next(input)
}

pub fn table_keyword<'a>(input: &mut &'a str) -> PResult<&'a str> {
    preceded(
        alt(("  ", "\n")),
        alt((
            "AREA",
            "LABEL",
            "DESCRIPTION",
            "VALEXP",
            "VALMSG",
            "DUMP-NAME",
            "TABLE-TRIGGER",
            "FIELD",
            "INDEX",
        )),
    )
    .parse_next(input)
}

pub fn add<'a>(input: &mut &'a str) -> PResult<&'a str> {
    peek(trace("ADD", preceded(seq!(newline, newline), "ADD"))).parse_next(input)
}

pub fn parse_sequence<'a>(input: &mut &'a str) -> PResult<Entity<'a>> {
    trim("SEQUENCE").parse_next(input)?;

    Ok(Entity::Sequence(seq!{Sequence {
        name: trim_quotes(take_while(0.., |c: char| c != '"')),
        initial: preceded(seq!(multispace0, "INITIAL", multispace1), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
        increment: preceded(seq!(multispace0, "INCREMENT", multispace1), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
        cycle_on_limit: preceded(seq!(multispace0, "CYCLE-ON-LIMIT", multispace1), alt(("yes", "no"))).map(|x: &str| x == "yes"),
        min_val: preceded(seq!(multispace0, "MIN-VAL", multispace1), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
    }}.parse_next(input)?))
}

pub fn until_whitespace<'a>(input: &mut &'a str) -> PResult<&'a str> {
    take_while(0.., |c: char| !c.is_whitespace()).parse_next(input)
}

pub fn parse_df(input: &str) -> PResult<Vec<Entity<'_>>> {
    let mut iter = iterator(input, dispatch);
    let entities = iter.collect::<Vec<Entity>>();
    let _result = iter.finish();

    Ok(entities)
}

pub fn print_df<'a>(entities: Vec<Entity>) {
    for entity in entities {
        match entity {
            Entity::Database(ref db) => {
                println!("UPDATE DATABASE \"{}\"", db.name);
                println!();
            }
            Entity::Table(ref table) => {
                println!("ADD TABLE \"{}\"", table.name);
                println!("  AREA \"{}\"", table.area);
                if let Some(label) = &table.label {
                    println!("  LABEL \"{}", label.trim_matches('\n'));
                }
                if let Some(desc) = &table.description {
                    println!("  DESCRIPTION {}", desc.trim_matches('\n'));
                }
                if let Some(valexp) = &table.valexp {
                    println!("  VALEXP \"{}", valexp.trim_matches('\n'));
                }
                if let Some(valmsg) = &table.valmsg {
                    println!("  VALMSG \"{}", valmsg.trim_matches('\n'));
                }
                println!("  DUMP-NAME \"{}", table.dump_name.trim_matches('\n'));
                if let Some(table_triggers) = &table.table_triggers {
                    for trigger in table_triggers {
                        println!("  TABLE-TRIGGER {}", trigger.trim_matches('\n').trim());
                    }
                }
                println!();

                for field in &table.fields {
                    println!(
                        "ADD FIELD \"{}\" OF \"{}\" AS {}",
                        field.name, table.name, field.r#type
                    );
                    if let Some(desc) = &field.description {
                        println!("  DESCRIPTION {}", desc.trim_matches('\n'));
                    }
                    println!("  FORMAT \"{}", field.format.trim_matches('\n'));
                    if let Some(format_sa) = &field.format_sa {
                        println!("  FORMAT-SA \"{}", format_sa.trim_matches('\n'));
                    }
                    if field.initial.trim_matches('"').trim() == "?"
                        && (field.r#type == DataType::Date
                            || field.r#type == DataType::Character
                            || field.r#type == DataType::DateTime
                            || field.r#type == DataType::Logical
                            || field.r#type == DataType::RecId
                            || field.r#type == DataType::RecId
                            || field.r#type == DataType::Decimal 
                            || field.r#type == DataType::Int64
                            || field.r#type == DataType::Integer)
                    {
                        println!("  INITIAL {}", field.initial.trim());
                    } else if field.initial.trim_matches('"').trim() == "?" {
                        println!("  INITIAL \"?\"");
                    } else {
                        println!("  INITIAL \"{}", field.initial.trim_matches('\n'));
                    }

                    if let Some(initial_sa) = &field.initial_sa {
                        println!("  INITIAL-SA \"{}", initial_sa.trim_matches('\n'));
                    }
                    if let Some(label) = &field.label {
                        println!("  LABEL \"{}", label.trim_matches('\n'));
                    }
                    if let Some(label_sa) = &field.label_sa {
                        println!("  LABEL-SA \"{}", label_sa.trim_matches('\n'));
                    }
                    println!("  POSITION {}", field.position);
                    println!("  SQL-WIDTH {}", field.sql_width);
                    if let Some(can_read) = &field.can_read {
                        println!("  CAN-READ \"{}", can_read.trim_matches('\n'));
                    }
                    if let Some(can_write) = &field.can_write {
                        println!("  CAN-WRITE \"{}", can_write.trim_matches('\n'));
                    }
                    if let Some(view_as) = &field.view_as {
                        println!("  VIEW-AS \"{}", view_as.trim_matches('\n'));
                    }
                    if let Some(column_label) = &field.column_label {
                        println!("  COLUMN-LABEL \"{}", column_label.trim_matches('\n'));
                    }
                    if let Some(column_label_sa) = &field.column_label_sa {
                        println!("  COLUMN-LABEL-SA \"{}", column_label_sa.trim_matches('\n'));
                    }
                    if let Some(valexp) = &field.valexp {
                        println!("  VALEXP \"{}", valexp.trim_matches('\n'));
                    }
                    if let Some(valmsg) = &field.valmsg {
                        println!("  VALMSG \"{}", valmsg.trim_matches('\n'));
                    }
                    if let Some(valmsg_sa) = &field.valmsg_sa {
                        println!("  VALMSG-SA \"{}", valmsg_sa.trim_matches('\n'));
                    }
                    if let Some(help) = &field.help {
                        println!("  HELP \"{}", help.trim_matches('\n'));
                    }
                    if let Some(help_sa) = &field.help_sa {
                        println!("  HELP-SA \"{}", help_sa.trim_matches('\n'));
                    }
                    if let Some(extent) = &field.extent {
                        println!("  EXTENT {}", extent);
                    }
                    if let Some(decimals) = &field.decimals {
                        println!("  DECIMALS {}", decimals);
                    }
                    println!("  ORDER {}", field.order);
                    if field.case_sensitive {
                        println!("  CASE-SENSITIVE");
                    }
                    if field.mandatory {
                        println!("  MANDATORY");
                    }
                    if let Some(field_trigger) = &field.field_trigger {
                        println!("  FIELD-TRIGGER {}", field_trigger.trim_matches('\n'));
                    }
                    println!();
                }
                for index in table.indices.iter().flatten() {
                    println!("ADD INDEX \"{}\" ON \"{}\"", index.name, table.name);
                    println!("  AREA \"{}\"", index.area);
                    if index.unique {
                        println!("  UNIQUE");
                    }
                    if index.inactive {
                        println!("  INACTIVE");
                    }
                    if index.primary {
                        println!("  PRIMARY");
                    }
                    if let Some(desc) = &index.description {
                        println!("  DESCRIPTION {}", desc.trim_matches('\n'));
                    }
                    if index.word {
                        println!("  WORD");
                    }
                    for field in &index.fields {
                        print!(
                            "  INDEX-FIELD \"{}\" {}",
                            field.name,
                            match field.order {
                                SortOrder::Ascending => "ASCENDING",
                                SortOrder::Descending => "DESCENDING",
                            }
                        );
                        if field.abbreviated {
                            print!(" ABBREVIATED");
                        }
                        println!();
                    }
                    println!();
                }
            }
            Entity::Sequence(ref seq) => {
                println!("ADD SEQUENCE \"{}\"", seq.name);
                println!("  INITIAL {}", seq.initial);
                println!("  INCREMENT {}", seq.increment);
                println!(
                    "  CYCLE-ON-LIMIT {}",
                    if seq.cycle_on_limit { "yes" } else { "no" }
                );
                println!("  MIN-VAL {}", seq.min_val);
                println!();
            }
            Entity::End(ref end) => {
                println!(".");
                println!("PSC");
                println!("cpstream={}", end.codepage);
                println!(".");
                println!("{:0>10}", end.length);
            }
            _ => {}
        }
    }
}
