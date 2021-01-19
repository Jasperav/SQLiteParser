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
- Tables 
    - Table_name
    - [Columns]
        - Id
        - Name
        - Type of the column (Text, Numeric, Blob, Real, Integer)
        - Nullable
        - Part of the primary key
    - [Foreign keys]
        - Id
        - Table
        - [From_column]
        - [To_column]