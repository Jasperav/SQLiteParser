use std::collections::HashMap;
use std::path::Path;

use rusqlite::{Connection, ToSql, NO_PARAMS};

pub fn parse<P: AsRef<Path>, Parse: Parser>(path: P, parser: Parse) {
    let (query, params) = parser.query_all_tables();
    let connection = Connection::open(&path).unwrap();

    // Get the tables
    let tables = query_tables(query, params, &connection);

    parser.process_tables(
        tables
            .into_iter()
            .map(|t| (t.table_name.clone(), t))
            .collect(),
    )
}

pub trait Parser {
    fn query_all_tables(&self) -> (&'static str, &'static [&'static dyn ToSql]) {
        (
            "SELECT name FROM sqlite_master WHERE type='table';",
            NO_PARAMS,
        )
    }

    fn process_tables(&self, tables: HashMap<String, Table>);
}

#[derive(Debug, PartialEq)]
pub struct Table {
    pub table_name: String,
    pub columns: Vec<Column>,
    /// [id, ForeignKey]
    pub foreign_keys: HashMap<i32, Vec<ForeignKey>>,
}

#[derive(Debug, PartialEq)]
pub struct Column {
    pub id: i32,
    pub name: String,
    pub the_type: Type,
    pub nullable: bool,
    pub part_of_pk: bool,
}

#[derive(Debug, PartialEq)]
pub struct ForeignKey {
    pub id: i32,
    pub table: String,
    pub from_column: String,
    pub to_column: String,
}

#[derive(Debug, PartialEq)]
pub enum Type {
    Text,
    Integer,
    String,
    Real,
    Blob,
}

impl From<String> for Type {
    fn from(s: String) -> Self {
        if s == "TEXT" {
            Type::Text
        } else if s == "INTEGER" {
            Type::Integer
        } else if s == "STRING" {
            Type::String
        } else if s == "REAL" {
            Type::Real
        } else if s == "BLOB" {
            Type::Blob
        } else {
            panic!("Unknown type: {}", s)
        }
    }
}

fn query_tables(query: &str, params: &[&dyn ToSql], connection: &Connection) -> Vec<Table> {
    let mut tables = vec![];
    let mut stmt = connection.prepare(query).unwrap();
    let mut rows = stmt.query(params).unwrap();

    while let Some(row) = rows.next().unwrap() {
        // The name is available here
        let table_name: String = row.get(0).unwrap();

        // Get the columns
        let columns = query_columns(&connection, &table_name);
        // Get the foreign keys
        let foreign_keys = query_fk(&connection, &table_name);

        tables.push(Table {
            table_name,
            columns,
            foreign_keys,
        });
    }

    tables
}

fn query_fk(connection: &Connection, table_name: &str) -> HashMap<i32, Vec<ForeignKey>> {
    let mut foreign_keys = HashMap::new();
    let mut stmt = connection
        .prepare("SELECT * FROM pragma_foreign_key_list(?);")
        .unwrap();
    let mut rows = stmt.query(&[&table_name]).unwrap();

    while let Some(row) = rows.next().unwrap() {
        let id = row.get(0).unwrap();

        let entry = foreign_keys.entry(id).or_insert_with(Vec::new);

        entry.push(ForeignKey {
            id,
            table: row.get(2).unwrap(),
            from_column: row.get(3).unwrap(),
            to_column: row.get(4).unwrap(),
        })
    }

    foreign_keys
}

fn query_columns(connection: &Connection, table_name: &str) -> Vec<Column> {
    let mut columns = vec![];
    let mut stmt = connection
        .prepare("SELECT * FROM pragma_table_info(?);")
        .unwrap();
    let mut rows = stmt.query(&[&table_name]).unwrap();

    while let Some(row) = rows.next().unwrap() {
        // Parse the type first
        let t: String = row.get(2).unwrap();

        columns.push(Column {
            id: row.get(0).unwrap(),
            name: row.get(1).unwrap(),
            the_type: Type::from(t),
            nullable: row.get(3).unwrap(),
            part_of_pk: row.get(5).unwrap(),
        })
    }

    columns
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::collections::HashMap;

    use rusqlite::{Connection, NO_PARAMS};

    use crate::Type::{Blob, Integer, Real, Text};
    use crate::{parse, Column, ForeignKey, Parser, Table, Type};

    macro_rules! hashmap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::std::collections::HashMap::new();
         $( map.insert($key, $val); )*
         map
        }}
    }

    #[test]
    fn test_parse() {
        let current = std::env::current_dir().unwrap().join("test_sqlite.sqlite3");

        // Create the sqlite3 file
        std::fs::File::create(&current).unwrap();

        // Connect and add some tables to assert the data on
        let connect = Connection::open(&current).unwrap();

        connect
            .execute(
                "CREATE TABLE user (
            user_id STRING NOT NULL PRIMARY KEY
        );",
                NO_PARAMS,
            )
            .unwrap();

        connect
            .execute(
                "CREATE TABLE contacts (
            contact_id INTEGER NOT NULL,
            first_name TEXT NOT NULL,
            user_id INTEGER,
            FOREIGN KEY(user_id) REFERENCES user(user_id),
            PRIMARY KEY (contact_id, first_name)
        );",
                NO_PARAMS,
            )
            .unwrap();

        connect
            .execute(
                "CREATE TABLE book (
            contact_id INTEGER NOT NULL,
            first_name TEXT NOT NULL,
            real REAL NOT NULL,
            blob BLOB NOT NULL,
            user_id INTEGER,
            FOREIGN KEY(contact_id, first_name) REFERENCES contacts(contact_id, first_name),
            FOREIGN KEY(user_id) REFERENCES user(user_id),
            PRIMARY KEY (contact_id, first_name)
        );",
                NO_PARAMS,
            )
            .unwrap();

        // Create a parser
        struct Parse;

        impl Parser for Parse {
            fn process_tables(&self, tables: HashMap<String, Table, RandomState>) {
                let contacts = Table {
                    table_name: "contacts".to_string(),
                    columns: vec![
                        Column {
                            id: 0,
                            name: "contact_id".to_string(),
                            the_type: Integer,
                            nullable: true,
                            part_of_pk: true,
                        },
                        Column {
                            id: 1,
                            name: "first_name".to_string(),
                            the_type: Text,
                            nullable: true,
                            part_of_pk: true,
                        },
                        Column {
                            id: 2,
                            name: "user_id".to_string(),
                            the_type: Integer,
                            nullable: false,
                            part_of_pk: false,
                        },
                    ],
                    foreign_keys: {
                        hashmap!(0 => vec![
                            ForeignKey {
                                id: 0,
                                table: "user".to_string(),
                                from_column: "user_id".to_string(),
                                to_column: "user_id".to_string()
                            }
                        ])
                    },
                };
                let user = Table {
                    table_name: "user".to_string(),
                    columns: vec![Column {
                        id: 0,
                        name: "user_id".to_string(),
                        the_type: Type::String,
                        nullable: true,
                        part_of_pk: true,
                    }],
                    foreign_keys: HashMap::default(),
                };
                let book = Table {
                    table_name: "book".to_string(),
                    columns: vec![
                        Column {
                            id: 0,
                            name: "contact_id".to_string(),
                            the_type: Integer,
                            nullable: true,
                            part_of_pk: true,
                        },
                        Column {
                            id: 1,
                            name: "first_name".to_string(),
                            the_type: Text,
                            nullable: true,
                            part_of_pk: true,
                        },
                        Column {
                            id: 2,
                            name: "real".to_string(),
                            the_type: Real,
                            nullable: true,
                            part_of_pk: false,
                        },
                        Column {
                            id: 3,
                            name: "blob".to_string(),
                            the_type: Blob,
                            nullable: true,
                            part_of_pk: false,
                        },
                        Column {
                            id: 4,
                            name: "user_id".to_string(),
                            the_type: Integer,
                            nullable: false,
                            part_of_pk: false,
                        },
                    ],
                    foreign_keys: {
                        hashmap!(0 => vec![
                            ForeignKey { id: 0,
                                table: "user".to_string(),
                                from_column: "user_id".to_string(),
                                to_column: "user_id".to_string()
                                }
                            ],
                        1 => vec![
                            ForeignKey { id: 1,
                                table: "contacts".to_string(),
                                from_column: "contact_id".to_string(),
                                to_column: "contact_id".to_string()
                            },
                            ForeignKey { id: 1,
                                table: "contacts".to_string(),
                                from_column: "first_name".to_string(),
                                to_column: "first_name".to_string()
                                }
                            ]
                        )
                    },
                };

                let map: HashMap<String, Table> = vec![contacts, user, book]
                    .into_iter()
                    .map(|v| (v.table_name.clone(), v))
                    .collect();

                assert_eq!(map, tables);
            }
        }

        parse(&current, Parse {});

        // Done testing, remove the file
        drop(connect);

        std::fs::remove_file(current).unwrap();
    }
}
