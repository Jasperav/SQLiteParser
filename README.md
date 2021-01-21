# SQLite parser

[![Latest Version](https://img.shields.io/crates/v/sqlite_parser.svg)](https://crates.io/crates/sqlite_parser)
[![Build Status](https://img.shields.io/github/workflow/status/jasperav/SQLiteParser/Rust/master)](https://github.com/jasperav/SQLiteParser/actions)

## Usage
This crate will make it easy to parse a SQLite database. This can be useful for code generation.

Add a dependency on this crate by adding this line under `[dependencies]` in your `Cargo.toml` file:

```sqlite_parser = "*"```

Than implement the `Parser` trait and call the `parse` function with the implementing
`struct` and the location of the SQLite file. There is a convenience method that doesn't require an implementing `Parser` trait
called `parse_no_parser`.

## Calling the parser
There are 2 ways of using this library
- Implement the `Parser` trait and call the `parse` function.
```
use sqlite_parser::{parse, Parser, Table};
use std::fs::File;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;

/// This is the location to the SQLite file
let my_sqlite_file_location = std::env::current_dir().unwrap().join("test_sqlite.sqlite3");

/// Create a parse struct to process the tables
/// Note: there is a convenience method `parse_no_parser` that doesn't require a parser.
struct Parse;
impl Parser for Parse {
    fn process_tables(&mut self, tables: HashMap<String, Table, RandomState>) {
        // Do something with the tables
    }
}

/// Start the parsing
parse(&my_sqlite_file_location, &mut Parse { });
```

- Don't implement the `Parser` trait and call the `parse_no_parser` function.
```
use sqlite_parser::parse_no_parser;
use std::fs::File;
/// This is the location to the SQLite file
let my_sqlite_file_location = std::env::current_dir().unwrap().join("test_sqlite.sqlite3");

/// Start the parsing
let tables = parse_no_parser(&my_sqlite_file_location);
/// Do stuff with the tables property!
```
## What will it parse?

- Tables -> represents a table in SQLite 
    - Table_name -> the table name
    - [Columns] -> the columns of the table 
        - Id -> the id of the column (starts with 0 and is incremented for each ever-created column)
        - Name -> the name of the column
        - Type of the column (Text, Numeric, Blob, Real, Integer)
        - Nullable -> checks if the column is nullable
        - Part of the primary key -> checks if this column is part of the primary key
    - [Foreign keys] -> the foreign keys of the table
        - Id -> the id of the foreign key
        - Table -> the table it refers to
        - [From_column] -> the columns it refers from (own table)
        - [To_column] -> the columns it refers to (referring to table)