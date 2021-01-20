# SQLite parser

[![Latest Version](https://img.shields.io/crates/v/sqlite_parser.svg)](https://crates.io/crates/sqlite_parser)
[![Build Status](https://img.shields.io/github/workflow/status/jasperav/SQLiteParser/Rust/master)](https://github.com/jasperav/SQLiteParser/actions)

## Usage
This crate will make it easy to parse a SQLite database. This can be useful for code generation.

Add a dependency on this crate by adding this line under `[dependencies]` in your `Cargo.toml` file:

```sqlite_parser = "*"```

Than implement the `Parser` trait and call the `parse` function with the implementing
`struct` and the location of the SQLite file.

## What will it parse?

- Tables -> represents a table in SQLite 
    - Table_name -> the table name
    - [Columns] -> the columns of the table 
        - Id -> the id of the column (starts with 0 and is incremented for each column)
        - Name -> the name of the column
        - Type of the column (Text, Numeric, Blob, Real, Integer)
        - Nullable -> checks if the column is nullable
        - Part of the primary key -> checks if this column is part of the primary key
    - [Foreign keys] -> the foreign keys of the table
        - Id -> the id of the foreign key
        - Table -> the table it refers to
        - [From_column] -> the columns it refers from (own table)
        - [To_column] -> the columns it refers to (referring to table)