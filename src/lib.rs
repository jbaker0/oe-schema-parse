use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter, Write};
use std::io::{Cursor, Seek, SeekFrom, Write as ioWrite};
use winnow::ascii::{
    multispace0, multispace1, newline, space0, space1, till_line_ending, Caseless,
};
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
    pub area: String,
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
    #[serde(rename = "INDICES")]
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
    pub format: Option<&'a str>,
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
    pub area: String,
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
    #[serde(rename = "INDEX-FIELDS")]
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
    let _ = delimited(multispace0, Caseless("UPDATE DATABASE"), multispace0).parse_next(input)?;
    Ok(Entity::Database(Database {
        name: trim_quotes(take_while(0.., |c: char| c != '"')).parse_next(input)?,
    }))
}

pub fn parse_add<'a>(input: &mut &'a str) -> PResult<Entity<'a>> {
    delimited(multispace0, Caseless("ADD"), multispace0).parse_next(input)?;

    dispatch! {peek(until_whitespace);
      "TABLE" => parse_table,
      "SEQUENCE" => parse_sequence,
      _ => fail::<_, Entity, _>,
    }
    .parse_next(input)
}

pub fn parse_table<'a>(input: &mut &'a str) -> PResult<Entity<'a>> {
    delimited(multispace0, Caseless("TABLE"), multispace0).parse_next(input)?;

    Ok(Entity::Table(seq!{Table {
        name: trim_quotes(take_while(0.., |c: char| c != '"')),
        area: preceded(keyword_trim(Caseless("AREA")), delimited('"', take_while(0.., |c: char| c != '"'), '"')).parse_to(),
        label: opt(preceded(keyword_trim("LABEL"), trim_quotes(until_table_keyword_or_new))),
        description: opt(trim_quotes(trim(preceded(keyword_trim(Caseless("DESCRIPTION")), until_table_keyword_or_new)))),
        valexp: opt(preceded(keyword_trim(Caseless("VALEXP")), trim_quotes(until_table_keyword_or_new))),
        valmsg: opt(preceded(keyword_trim(Caseless("VALMSG")), trim_quotes(until_table_keyword_or_new))),
        dump_name: preceded(keyword_trim(Caseless("DUMP-NAME")), trim_quotes(until_table_keyword_or_new)),
        table_triggers: opt(repeat_till(0.., preceded(multispace0, preceded(Caseless("TABLE-TRIGGER"), take_while(0.., |c:char|!c.is_newline()))), trace("ADD_NOT_INDEX", peek(preceded(take_while(0..,|c: char| c.is_newline()), ("ADD ", not("INDEX")))))).map(|x: (Vec<&str>, (&str, ())) | x.0)),
        fields: repeat_till(0.., preceded(multispace0, parse_field), trace("ADD_NOT_FIELD", peek(preceded(take_while(0..,|c: char| c.is_newline()), ("ADD ", not("FIELD")))))).map(|x: (Vec<Field>, (&str, ())) | x.0),
        indices: opt(parse_indices),
    }
    }.parse_next(input)?))
}

pub fn parse_index<'a>(input: &mut &'a str) -> PResult<Index<'a>> {
    delimited(multispace0, Caseless("ADD INDEX"), multispace0).parse_next(input)?;

    seq! {
        Index {
            name: terminated(delimited('"',take_while(0.., |c: char| c != '"'),'"'), seq!(space0, Caseless("ON"), space0, delimited('"', take_while(0.., |c: char| c != '"'), '"'))),
            area: delimited(
                seq!(multispace0, Caseless("AREA"), space1),
                trim_quotes(take_while(0.., |c: char| c != '"')),
                space0
            ).parse_to(),
            unique: delimited(multispace0, opt(Caseless("UNIQUE")), space0).map(|x| x.is_some()),
            inactive: delimited(multispace0, opt(Caseless("INACTIVE")), space0).map(|x| x.is_some()),
            primary: delimited(multispace0, opt(Caseless("PRIMARY")), space0).map(|x| x.is_some()),
            description: opt(preceded(
                seq!(multispace0, Caseless("DESCRIPTION"), space1),
                take_while(0.., |c: char| !c.is_newline())
            )),
            word: delimited(multispace0, opt(Caseless("WORD")), space0).map(|x| x.is_some()),
            fields: repeat_till(
                0..,
                seq!(
                    preceded(
                        seq!(multispace0, Caseless("INDEX-FIELD"), space1),
                        delimited('"', take_while(0.., |c: char| c != '"'), '"')
                    ),
                    delimited(space0, alt((Caseless("ASCENDING"), "DESCENDING")), space0),
                    opt(delimited(space0, Caseless("ABBREVIATED"), space0))
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
    delimited(multispace0, Caseless("ADD FIELD"), multispace0).parse_next(input)?;

    seq!{Field {
        name: terminated(delimited('"',take_while(0.., |c: char| c != '"'), '"'), seq!(multispace0, Caseless("OF"), multispace0, delimited('"',take_while(0.., |c: char| c != '"'), '"'))),
        r#type: preceded(seq!(multispace0, Caseless("AS"), multispace1), alt((Caseless("logical"), Caseless("character"), Caseless("integer"), Caseless("int64"), Caseless("decimal"), Caseless("datetime"), Caseless("date"), Caseless("recid"), Caseless("raw")))).map(|x| match x {
            "logical" => DataType::Logical,
            "character" => DataType::Character,
            "integer" => DataType::Integer,
            "int64" => DataType::Int64,
            "decimal" => DataType::Decimal,
            "datetime" => DataType::DateTime,
            "date" => DataType::Date,
            "recid" => DataType::RecId,
            "raw" => DataType::Raw,
            _ => unreachable!(),
        }),
        description: opt(preceded(keyword_trim(Caseless("DESCRIPTION")), delimited(multispace0, until_field_keyword_or_new.recognize(), multispace1))),
        format: opt(preceded(keyword_trim(Caseless("FORMAT")), trim_quotes(until_field_keyword_or_new))),
        format_sa: opt(preceded(keyword_trim(Caseless("FORMAT-SA")), trim_quotes(until_field_keyword_or_new))),
        initial: preceded(keyword_trim(Caseless("INITIAL")), trim_quotes(until_field_keyword_or_new)),
        initial_sa: opt(preceded(keyword_trim(Caseless("INITIAL-SA")), trim_quotes(until_field_keyword_or_new))),
        label: opt(preceded(keyword_trim("LABEL"), trim_quotes(until_field_keyword_or_new))),
        label_sa: opt(preceded(keyword_trim(Caseless("LABEL-SA")), trim_quotes(until_field_keyword_or_new))),
        position: preceded(keyword_trim(Caseless("POSITION")), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
        sql_width: preceded(keyword_trim(Caseless("SQL-WIDTH")), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
        can_read: opt(preceded(keyword_trim(Caseless("CAN-READ")), trim_quotes(until_field_keyword_or_new))),
        can_write: opt(preceded(keyword_trim(Caseless("CAN-WRITE")), trim_quotes(until_field_keyword_or_new))),
        view_as: opt(preceded(keyword_trim(Caseless("VIEW-AS")), trim_quotes(until_field_keyword_or_new))),
        column_label: opt(preceded(keyword_trim(Caseless("COLUMN-LABEL")), trim_quotes(until_field_keyword_or_new))),
        column_label_sa: opt(preceded(keyword_trim(Caseless("COLUMN-LABEL-SA")), trim_quotes(until_field_keyword_or_new))),
        valexp: opt(preceded(keyword_trim(Caseless("VALEXP")), trim_quotes(until_field_keyword_or_new))),
        valmsg: opt(preceded(keyword_trim(Caseless("VALMSG")), trim_quotes(until_field_keyword_or_new))),
        valmsg_sa: opt(preceded(keyword_trim(Caseless("VALMSG-SA")), trim_quotes(until_field_keyword_or_new))),
        help: opt(preceded(keyword_trim("HELP"), trim_quotes(until_field_keyword_or_new))),
        help_sa: opt(preceded(keyword_trim(Caseless("HELP-SA")), trim_quotes(until_field_keyword_or_new))),
        extent: opt(preceded(keyword_trim(Caseless("EXTENT")), take_while(0.., |c: char| c.is_ascii_digit()).parse_to())),
        decimals: opt(preceded(keyword_trim(Caseless("DECIMALS")), take_while(0.., |c: char| c.is_ascii_digit()).parse_to())),
        order: preceded(keyword_trim(Caseless("ORDER")), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
        case_sensitive: opt(keyword_trim(Caseless("CASE-SENSITIVE"))).map(|x| x.is_some()),
        mandatory: opt(keyword_trim(Caseless("MANDATORY"))).map(|x| x.is_some()),
        field_trigger: opt(preceded(seq!(multispace0, Caseless("FIELD-TRIGGER"), space1), take_while(0.., |c: char| ! c.is_newline()))),
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
    peek(preceded(multispace0, (Caseless("ADD "), not("INDEX"))))
        .map(|_| true)
        .parse_next(input)
}

pub fn parse_file_end<'a>(input: &mut &'a str) -> PResult<Entity<'a>> {
    let _ = seq!(multispace0, ".", multispace0, Caseless("PSC"), multispace0).parse_next(input)?;
    Ok(Entity::End(
        seq! { End{
            codepage: delimited("cpstream=", till_line_ending, (multispace0, ".")),
            length: preceded(multispace0, till_line_ending.parse_to())
        }}
        .parse_next(input)?,
    ))
}

pub fn parse_file_end_succeed(input: &mut &str) -> PResult<bool> {
    let _ = seq!(multispace0, ".", multispace0, Caseless("PSC"), multispace0).parse_next(input)?;
    seq! { End{
        codepage: delimited("cpstream=", till_line_ending, (multispace0, ".")),
        length: preceded(multispace0, till_line_ending.parse_to())
    }}
    .map(|x: End| x.codepage == "ISO8859-1")
    .parse_next(input)
}

pub fn field_keyword<'a>(input: &mut &'a str) -> PResult<&'a str> {
    preceded(
        alt(("  ", "\n\n")),
        alt((
            "DESCRIPTION",
            Caseless("FORMAT"),
            Caseless("INITIAL"),
            "LABEL",
            Caseless("POSITION"),
            Caseless("SQL-WIDTH"),
            Caseless("CAN-READ"),
            Caseless("CAN-WRITE"),
            Caseless("VIEW-AS"),
            Caseless("COLUMN-LABEL"),
            Caseless("VALEXP"),
            Caseless("VALMSG"),
            "HELP",
            Caseless("EXTENT"),
            Caseless("DECIMALS"),
            "ORDER",
            Caseless("CASE-SENSITIVE"),
            Caseless("MANDATORY"),
            Caseless("FIELD-TRIGGER"),
        )),
    )
    .parse_next(input)
}

pub fn table_keyword<'a>(input: &mut &'a str) -> PResult<&'a str> {
    preceded(
        alt(("  ", "\n")),
        alt((
            Caseless("AREA"),
            Caseless("LABEL"),
            Caseless("DESCRIPTION"),
            Caseless("VALEXP"),
            Caseless("VALMSG"),
            Caseless("DUMP-NAME"),
            Caseless("TABLE-TRIGGER"),
            "FIELD",
            "INDEX",
        )),
    )
    .parse_next(input)
}

pub fn add<'a>(input: &mut &'a str) -> PResult<&'a str> {
    peek(trace(
        "ADD",
        preceded(seq!(newline, newline), Caseless("ADD")),
    ))
    .parse_next(input)
}

pub fn parse_sequence<'a>(input: &mut &'a str) -> PResult<Entity<'a>> {
    trim(Caseless("SEQUENCE")).parse_next(input)?;

    Ok(Entity::Sequence(seq!{Sequence {
        name: trim_quotes(take_while(0.., |c: char| c != '"')),
        initial: preceded(seq!(multispace0, Caseless("INITIAL"), multispace1), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
        increment: preceded(seq!(multispace0, Caseless("INCREMENT"), multispace1), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
        cycle_on_limit: preceded(seq!(multispace0, Caseless("CYCLE-ON-LIMIT"), multispace1), alt((Caseless("yes"), Caseless("no")))).map(|x: &str| x == "yes"),
        min_val: preceded(seq!(multispace0, Caseless("MIN-VAL"), multispace1), take_while(0.., |c: char| c.is_ascii_digit()).parse_to()),
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

pub fn write_df<'a>(
    entities: &Vec<Entity>,
    destination: &'a mut (impl ioWrite + Seek),
) -> Result<impl ioWrite + 'a, std::io::Error> {
    for entity in entities {
        match entity {
            Entity::Database(ref db) => {
                writeln!(destination, "UPDATE DATABASE \"{}\"", db.name)?;
                writeln!(destination,)?;
            }
            Entity::Table(ref table) => {
                writeln!(destination, "ADD TABLE \"{}\"", table.name)?;
                writeln!(destination, "  AREA \"{}\"", table.area)?;
                if let Some(label) = &table.label {
                    writeln!(destination, "  LABEL \"{}", label.trim_matches('\n'))?;
                }
                if let Some(desc) = &table.description {
                    writeln!(destination, "  DESCRIPTION {}", desc.trim_matches('\n'))?;
                }
                if let Some(valexp) = &table.valexp {
                    writeln!(destination, "  VALEXP \"{}", valexp.trim_matches('\n'))?;
                }
                if let Some(valmsg) = &table.valmsg {
                    writeln!(destination, "  VALMSG \"{}", valmsg.trim_matches('\n'))?;
                }
                writeln!(
                    destination,
                    "  DUMP-NAME \"{}",
                    table.dump_name.trim_matches('\n')
                )?;
                if let Some(table_triggers) = &table.table_triggers {
                    for trigger in table_triggers {
                        writeln!(
                            destination,
                            "  TABLE-TRIGGER {}",
                            trigger.trim_matches('\n').trim()
                        )?;
                    }
                }
                writeln!(destination,)?;

                for field in &table.fields {
                    writeln!(
                        destination,
                        "ADD FIELD \"{}\" OF \"{}\" AS {}",
                        field.name, table.name, field.r#type
                    )?;
                    if let Some(desc) = &field.description {
                        writeln!(destination, "  DESCRIPTION {}", desc.trim_matches('\n'))?;
                    }
                    if let Some(format) = &field.format {
                        writeln!(destination, "  FORMAT \"{}", format.trim_matches('\n'))?;
                    }
                    if let Some(format_sa) = &field.format_sa {
                        writeln!(
                            destination,
                            "  FORMAT-SA \"{}",
                            format_sa.trim_matches('\n')
                        )?;
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
                        writeln!(destination, "  INITIAL {}", field.initial.trim())?;
                    } else if field.initial.trim_matches('"').trim() == "?" {
                        writeln!(destination, "  INITIAL \"?\"")?;
                    } else {
                        writeln!(
                            destination,
                            "  INITIAL \"{}",
                            field.initial.trim_matches('\n')
                        )?;
                    }

                    if let Some(initial_sa) = &field.initial_sa {
                        writeln!(
                            destination,
                            "  INITIAL-SA \"{}",
                            initial_sa.trim_matches('\n')
                        )?;
                    }
                    if let Some(label) = &field.label {
                        writeln!(destination, "  LABEL \"{}", label.trim_matches('\n'))?;
                    }
                    if let Some(label_sa) = &field.label_sa {
                        writeln!(destination, "  LABEL-SA \"{}", label_sa.trim_matches('\n'))?;
                    }
                    writeln!(destination, "  POSITION {}", field.position)?;
                    writeln!(destination, "  SQL-WIDTH {}", field.sql_width)?;
                    if let Some(can_read) = &field.can_read {
                        writeln!(destination, "  CAN-READ \"{}", can_read.trim_matches('\n'))?;
                    }
                    if let Some(can_write) = &field.can_write {
                        writeln!(
                            destination,
                            "  CAN-WRITE \"{}",
                            can_write.trim_matches('\n')
                        )?;
                    }
                    if let Some(view_as) = &field.view_as {
                        writeln!(destination, "  VIEW-AS \"{}", view_as.trim_matches('\n'))?;
                    }
                    if let Some(column_label) = &field.column_label {
                        writeln!(
                            destination,
                            "  COLUMN-LABEL \"{}",
                            column_label.trim_matches('\n')
                        )?;
                    }
                    if let Some(column_label_sa) = &field.column_label_sa {
                        writeln!(
                            destination,
                            "  COLUMN-LABEL-SA \"{}",
                            column_label_sa.trim_matches('\n')
                        )?;
                    }
                    if let Some(valexp) = &field.valexp {
                        writeln!(destination, "  VALEXP \"{}", valexp.trim_matches('\n'))?;
                    }
                    if let Some(valmsg) = &field.valmsg {
                        writeln!(destination, "  VALMSG \"{}", valmsg.trim_matches('\n'))?;
                    }
                    if let Some(valmsg_sa) = &field.valmsg_sa {
                        writeln!(
                            destination,
                            "  VALMSG-SA \"{}",
                            valmsg_sa.trim_matches('\n')
                        )?;
                    }
                    if let Some(help) = &field.help {
                        writeln!(destination, "  HELP \"{}", help.trim_matches('\n'))?;
                    }
                    if let Some(help_sa) = &field.help_sa {
                        writeln!(destination, "  HELP-SA \"{}", help_sa.trim_matches('\n'))?;
                    }
                    if let Some(extent) = &field.extent {
                        writeln!(destination, "  EXTENT {}", extent)?;
                    }
                    if let Some(decimals) = &field.decimals {
                        writeln!(destination, "  DECIMALS {}", decimals)?;
                    }
                    writeln!(destination, "  ORDER {}", field.order)?;
                    if field.case_sensitive {
                        writeln!(destination, "  CASE-SENSITIVE")?;
                    }
                    if field.mandatory {
                        writeln!(destination, "  MANDATORY")?;
                    }
                    if let Some(field_trigger) = &field.field_trigger {
                        writeln!(
                            destination,
                            "  FIELD-TRIGGER {}",
                            field_trigger.trim_matches('\n')
                        )?;
                    }
                    writeln!(destination,)?;
                }
                for index in table.indices.iter().flatten() {
                    writeln!(
                        destination,
                        "ADD INDEX \"{}\" ON \"{}\"",
                        index.name, table.name
                    )?;
                    writeln!(destination, "  AREA \"{}\"", index.area)?;
                    if index.unique {
                        writeln!(destination, "  UNIQUE")?;
                    }
                    if index.inactive {
                        writeln!(destination, "  INACTIVE")?;
                    }
                    if index.primary {
                        writeln!(destination, "  PRIMARY")?;
                    }
                    if let Some(desc) = &index.description {
                        writeln!(destination, "  DESCRIPTION {}", desc.trim_matches('\n'))?;
                    }
                    if index.word {
                        writeln!(destination, "  WORD")?;
                    }
                    for field in &index.fields {
                        write!(
                            destination,
                            "  INDEX-FIELD \"{}\" {}",
                            field.name,
                            match field.order {
                                SortOrder::Ascending => "ASCENDING",
                                SortOrder::Descending => "DESCENDING",
                            }
                        )?;
                        if field.abbreviated {
                            write!(destination, " ABBREVIATED")?;
                        }
                        writeln!(destination,)?;
                    }
                    writeln!(destination,)?;
                }
            }
            Entity::Sequence(ref seq) => {
                writeln!(destination, "ADD SEQUENCE \"{}\"", seq.name)?;
                writeln!(destination, "  INITIAL {}", seq.initial)?;
                writeln!(destination, "  INCREMENT {}", seq.increment)?;
                writeln!(
                    destination,
                    "  CYCLE-ON-LIMIT {}",
                    if seq.cycle_on_limit { "yes" } else { "no" }
                )?;
                writeln!(destination, "  MIN-VAL {}", seq.min_val)?;
                writeln!(destination,)?;
            }
            Entity::End(ref end) => {
                writeln!(destination, ".")?;
                writeln!(destination, "PSC")?;
                writeln!(destination, "cpstream={}", end.codepage)?;
                writeln!(destination, ".")?;

                // DF Files have the length of the file at the end so we get the current position, and add 10
                let length = destination.seek(SeekFrom::Current(0))?;

                writeln!(destination, "{:0>10}", length + 10)?;
            }
            _ => {}
        }
    }

    Ok(destination)
}

pub fn write_fmt_df(entities: &Vec<Entity>) -> String {
    let mut output = Cursor::new(Vec::new());
    write_df(entities, &mut output).unwrap();
    String::from_utf8(output.into_inner()).unwrap()
}
