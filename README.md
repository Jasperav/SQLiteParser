# SQLite parser

[![Latest Version](https://img.shields.io/crates/v/sqlite_parser.svg)](https://crates.io/crates/sqlite_parser)
[![Build Status](https://img.shields.io/github/workflow/status/jasperav/sqlite_parser/Rust/master)](https://github.com/jasperav/sqlite_parser/actions)

## Usage
This crate will make it easy to parse a SQLite database. This is useful for code generation.

Add a dependency on this crate by adding this line under `[dependencies]` in your `Cargo.toml` file:

```sqlite_parser = "*"```

Than implement the `Parser` trait and call the `parse` function.