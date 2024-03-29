use std::collections::HashMap;
use std::path::Path;

use rusqlite::{Connection, ToSql};

#[derive(Debug, PartialEq, Clone, Eq)]
pub struct Metadata {
    pub tables: HashMap<String, Table>,
}

impl Metadata {
    pub fn table(&self, table_name: &str) -> Option<&Table> {
        self.tables
            .values()
            .find(|table| table.table_name == table_name)
    }
}

/// The method to call to start parsing the SQLite file
/// Example:
///
/// ```
/// use sqlite_parser::{parse, Parser, Table, Metadata};
/// use std::fs::File;
///
/// /// This is the location to the SQLite file
/// let my_sqlite_file_location = std::env::current_dir().unwrap().join("test_sqlite.sqlite3");
/// /// For the doc test, create the actual SQLite file
/// let sqlite_file = File::create(&my_sqlite_file_location).unwrap();
///
/// /// Create a parse struct to process the tables
/// /// Note: there is a convenience method `parse_no_parser` that doesn't require a parser.
/// struct Parse;
///
/// impl Parser for Parse {
///     fn process_tables(&mut self, meta_data: Metadata) {
///         // Do something with the tables
///     }
/// }
///
/// /// Start the parsing
/// parse(&my_sqlite_file_location, &mut Parse { });
///
/// /// Remove the SQLite file for the doc test
/// std::fs::remove_file(&my_sqlite_file_location).unwrap();
/// ```
pub fn parse<P: AsRef<Path>, Parse: Parser>(path: P, parser: &mut Parse) {
    let (query, params) = parser.query_all_tables();
    let connection = Connection::open(&path).unwrap();

    // Get the tables
    let tables = query_tables(query, params, &connection);

    parser.process_tables(Metadata {
        tables: tables
            .into_iter()
            .map(|t| (t.table_name.clone(), t))
            .collect(),
    });
}

/// Convenience method to get the tables
/// Example:
///
/// ```
/// use sqlite_parser::parse_no_parser;
/// use std::fs::File;
///
/// /// This is the location to the SQLite file
/// let my_sqlite_file_location = std::env::current_dir().unwrap().join("test_sqlite.sqlite3");
/// /// For the doc test, create the actual SQLite file
/// let sqlite_file = File::create(&my_sqlite_file_location).unwrap();
///
/// /// Start the parsing
/// let _tables = parse_no_parser(&my_sqlite_file_location);
/// /// Do stuff with the tables property!
///
/// /// Remove the SQLite file for the doc test
/// std::fs::remove_file(&my_sqlite_file_location).unwrap();
/// ```
pub fn parse_no_parser<P: AsRef<Path>>(path: P) -> Metadata {
    struct Parse {
        tables: Option<Metadata>,
    }

    impl Parser for Parse {
        fn process_tables(&mut self, tables: Metadata) {
            self.tables = Some(tables)
        }
    }

    let mut p = Parse { tables: None };

    parse(path, &mut p);

    p.tables.unwrap()
}

/// Implement this trait to parse your own types
pub trait Parser {
    fn query_all_tables(&self) -> (&'static str, &'static [&'static dyn ToSql]) {
        ("SELECT name FROM sqlite_master WHERE type='table';", &[])
    }

    fn process_tables(&mut self, tables: Metadata);
}

/// Represents a table in SQLite
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct Table {
    /// The table name
    pub table_name: String,
    /// The columns of the table
    pub columns: Vec<Column>,
    /// The foreign keys of the table
    pub foreign_keys: Vec<ForeignKey>,
    pub indexes: Vec<Index>,
}

/// Represents an index in SQLite
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct Index {
    pub name: String,
    pub columns: Vec<Column>,
    pub unique: bool,
}

impl Table {
    pub fn column(&self, column_name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|c| c.name.to_lowercase() == column_name.to_lowercase())
    }
}

/// Represents a column in SQLite
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct Column {
    /// The id of the column (starts with 0 and is incremented for each column)
    pub id: i32,
    /// The name of the column
    pub name: String,
    /// The type of the column
    pub the_type: Type,
    /// Checks if the column is nullable
    pub nullable: bool,
    /// Checks if the column is part of the primary key
    pub part_of_pk: bool,
}

/// Represents a foreign key in SQLite
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct ForeignKey {
    /// The id of the foreign key
    /// Starts with 0 and is incremented for each unique foreign key
    /// This means compound foreign key shares the same id
    pub id: i32,
    /// The table it refers to
    pub table: String,
    /// The columns it refers from (own table)
    pub from_column: Vec<Column>,
    /// The columns it refers to (referring to table)
    pub to_column: Vec<Column>,
    pub on_update: OnUpdateAndDelete,
    pub on_delete: OnUpdateAndDelete,
}

#[derive(Debug, PartialEq, Copy, Clone, Eq)]
pub enum OnUpdateAndDelete {
    NoAction,
    Restrict,
    SetNull,
    SetDefault,
    Cascade,
}

impl OnUpdateAndDelete {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "no action" => Self::NoAction,
            "restrict" => Self::Restrict,
            "set null" => Self::SetNull,
            "set default" => Self::SetDefault,
            "cascade" => Self::Cascade,
            _ => panic!("{}", "Unknown OnUpdateAndDelete: {s}"),
        }
    }
}

/// Represents a type in SQLite
#[derive(Debug, PartialEq, Copy, Clone, Eq)]
pub enum Type {
    Text,
    Integer,
    String,
    Real,
    Blob,
}

impl From<String> for Type {
    fn from(s: String) -> Self {
        let lower_cased = s.to_lowercase();

        if &lower_cased == "text" {
            Type::Text
        } else if &lower_cased == "integer" || &lower_cased == "int" {
            Type::Integer
        } else if &lower_cased == "string" {
            Type::String
        } else if &lower_cased == "real" {
            Type::Real
        } else if &lower_cased == "blob" {
            Type::Blob
        } else {
            panic!("Unknown type: {}", s)
        }
    }
}

/// Queries the tables from the SQLite file
fn query_tables(query: &str, params: &[&dyn ToSql], connection: &Connection) -> Vec<Table> {
    let mut tables = vec![];
    let mut stmt = connection.prepare(query).unwrap();
    let mut rows = stmt.query(params).unwrap();

    while let Some(row) = rows.next().unwrap() {
        // The name is available here
        let table_name: String = row.get(0).unwrap();

        // Get the columns
        let columns = query_columns(connection, &table_name);
        // Get the foreign keys
        let foreign_keys = query_fk(connection, &table_name);
        let indexes = query_indexes(connection, &table_name, &columns, &foreign_keys);

        tables.push(Table {
            table_name,
            columns,
            foreign_keys,
            indexes,
        });
    }

    tables
}

/// Queries the columns from the table name
fn query_columns(connection: &Connection, table_name: &str) -> Vec<Column> {
    let mut columns = vec![];
    let mut stmt = connection
        .prepare("SELECT * FROM pragma_table_info(?);")
        .unwrap();
    let mut rows = stmt.query([&table_name]).unwrap();

    while let Some(row) = rows.next().unwrap() {
        // Parse the type first
        let t: String = row.get(2).unwrap();
        let is_non_null: bool = row.get(3).unwrap();
        let name: String = row.get(1).unwrap();

        columns.push(Column {
            id: row.get(0).unwrap(),
            name,
            the_type: Type::from(t),
            nullable: !is_non_null,
            part_of_pk: row.get(5).unwrap(),
        });
    }

    columns
}

/// Queries the indexes from the table name
fn query_indexes(
    connection: &Connection,
    table_name: &str,
    columns: &[Column],
    foreign_keys: &[ForeignKey],
) -> Vec<Index> {
    let mut indexes = vec![];
    let mut stmt = connection
        .prepare(
            "SELECT
  name, sql
FROM sqlite_master
WHERE type = 'index' AND tbl_name = ? AND sql is not null;",
        )
        .unwrap();
    let mut rows = stmt.query([&table_name]).unwrap();

    while let Some(row) = rows.next().unwrap() {
        let name: String = row.get(0).unwrap();
        let sql: String = row.get(1).unwrap();
        let columns_used = sql
            .split('(')
            .collect::<Vec<_>>()
            .get(1)
            .unwrap()
            .split(')')
            .collect::<Vec<_>>()
            .first()
            .unwrap()
            .split(", ")
            .map(|c| {
                c.to_string()
                    .strip_suffix(" desc")
                    .map(|c| c.to_string())
                    .unwrap_or(c.to_string())
            });

        indexes.push(Index {
            name: name.clone(),
            columns: columns_used
                .clone()
                .map(|c| {
                    columns
                        .iter()
                        .find(|co| c.to_lowercase() == co.name.to_lowercase())
                        .unwrap_or_else(|| panic!("Could not find index with name {name} in columns_used: {:#?} columns: {:#?}", columns_used, columns))
                        .clone()
                })
                .collect(),
            unique: false,
        });
    }

    let mut stmt = connection
        .prepare(
            // https://stackoverflow.com/a/53629321/7715250
            &format!(
                "SELECT DISTINCT ii.name as column_name
FROM sqlite_master AS m,
     pragma_index_list(m.name) AS il,
     pragma_index_info(il.name) AS ii
WHERE m.type='table' AND il.[unique] = 1 and m.name = '{table_name}';"
            ),
        )
        .unwrap();
    let mut rows = stmt.query([]).unwrap();

    while let Some(row) = rows.next().unwrap() {
        let name: String = row.get(0).unwrap();
        let mut index_column = None;

        for column in columns {
            if column.name == name {
                if !column.part_of_pk {
                    index_column = Some(column.clone());
                }

                break;
            }
        }

        if index_column.is_none() {
            // PK's always have unique indexes, skip these
            continue;
        }

        let index_column = index_column.unwrap();
        let contains = foreign_keys
            .iter()
            .find(|f| f.from_column.iter().any(|f| f.name == index_column.name));

        if contains.is_some() {
            // Foreign keys always have unique constraints, ignore
            continue;
        }

        indexes.push(Index {
            name,
            columns: vec![index_column],
            unique: true,
        })
    }

    // Check for duplicates
    for (i, index) in indexes.iter().enumerate() {
        for (i_inner, index_inner) in indexes.iter().enumerate() {
            if i == i_inner {
                assert_eq!(index, index_inner);

                continue;
            }

            assert_ne!(
                index.columns, index_inner.columns,
                "Duplicate index: {:#?}",
                index_inner.columns
            );
        }
    }

    indexes
}

/// Queries the foreign keys from the table name
fn query_fk(connection: &Connection, table_name: &str) -> Vec<ForeignKey> {
    let mut foreign_keys: Vec<ForeignKey> = vec![];
    let mut stmt = connection
        .prepare("SELECT * FROM pragma_foreign_key_list(?);")
        .unwrap();
    let mut rows = stmt.query([&table_name]).unwrap();

    while let Some(row) = rows.next().unwrap() {
        let table: String = row.get(2).unwrap();
        let other_table_columns = query_columns(connection, &table);
        let from_column: String = row.get(3).unwrap();
        let to_column: String = row.get(4).unwrap();
        let on_update: String = row.get(5).unwrap();
        let on_delete: String = row.get(6).unwrap();
        let own_columns = query_columns(connection, table_name);

        let mut foreign_key = ForeignKey {
            id: row.get(0).unwrap(),
            table,
            from_column: vec![own_columns
                .clone()
                .into_iter()
                .find(|c| c.name.to_lowercase() == from_column.to_lowercase())
                .unwrap_or_else(|| {
                    panic!(
                        "Expected to find {} in {:#?}",
                        from_column.to_lowercase(),
                        own_columns
                            .iter()
                            .map(|c| c.name.to_lowercase())
                            .collect::<Vec<_>>()
                    )
                })],
            to_column: vec![other_table_columns
                .clone()
                .into_iter()
                .find(|c| c.name.to_lowercase() == to_column.to_lowercase())
                .unwrap()],
            on_update: OnUpdateAndDelete::from_str(&on_update),
            on_delete: OnUpdateAndDelete::from_str(&on_delete),
        };

        if let Some(fk) = foreign_keys
            .iter_mut()
            .find(|f| f.id == row.get(0).unwrap())
        {
            fk.from_column.push(foreign_key.from_column.remove(0));
            fk.to_column.push(foreign_key.to_column.remove(0));
        } else {
            foreign_keys.push(foreign_key);
        }
    }

    foreign_keys
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rusqlite::Connection;

    use crate::Type::{Blob, Integer, Real, Text};
    use crate::{
        parse, Column, ForeignKey, Index, Metadata, OnUpdateAndDelete, Parser, Table, Type,
    };

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
            user_id INTEGER NOT NULL PRIMARY KEY,
            parent_id INTEGER,
            FOREIGN KEY(parent_id) REFERENCES user(user_id)
        );",
                [],
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
                [],
            )
            .unwrap();

        connect
            .execute(
                "CREATE INDEX contacts_user_id on contacts(user_id, first_name);",
                [],
            )
            .unwrap();

        connect
            .execute(
                "CREATE INDEX contact_id_reversed on contacts(contact_id desc);",
                [],
            )
            .unwrap();

        connect
            .execute(
                "CREATE TABLE book (
            contact_id INTEGER NOT NULL,
            first_name TEXT NOT NULL,
            real REAL UNIQUE NOT NULL,
            blob BLOB NOT NULL,
            user_id INTEGER,
            FOREIGN KEY(contact_id, first_name) REFERENCES contacts(contact_id, first_name),
            FOREIGN KEY(user_id) REFERENCES user(user_id),
            PRIMARY KEY (contact_id, first_name)
        );",
                [],
            )
            .unwrap();

        // Create a parser
        struct Parse;

        impl Parser for Parse {
            fn process_tables(&mut self, tables: Metadata) {
                let user_id_column = Column {
                    id: 0,
                    name: "user_id".to_string(),
                    the_type: Type::Integer,
                    nullable: false,
                    part_of_pk: true,
                };

                let contacts = Table {
                    table_name: "contacts".to_string(),
                    columns: vec![
                        Column {
                            id: 0,
                            name: "contact_id".to_string(),
                            the_type: Integer,
                            nullable: false,
                            part_of_pk: true,
                        },
                        Column {
                            id: 1,
                            name: "first_name".to_string(),
                            the_type: Text,
                            nullable: false,
                            part_of_pk: true,
                        },
                        Column {
                            id: 2,
                            name: "user_id".to_string(),
                            the_type: Integer,
                            nullable: true,
                            part_of_pk: false,
                        },
                    ],
                    foreign_keys: vec![ForeignKey {
                        id: 0,
                        table: "user".to_string(),
                        from_column: vec![Column {
                            id: 2,
                            name: "user_id".to_string(),
                            the_type: Integer,
                            nullable: true,
                            part_of_pk: false,
                        }],
                        to_column: vec![user_id_column.clone()],
                        on_update: OnUpdateAndDelete::NoAction,
                        on_delete: OnUpdateAndDelete::NoAction,
                    }],
                    indexes: vec![
                        Index {
                            name: "contacts_user_id".to_string(),
                            columns: vec![
                                Column {
                                    id: 2,
                                    name: "user_id".to_string(),
                                    the_type: Integer,
                                    nullable: true,
                                    part_of_pk: false,
                                },
                                Column {
                                    id: 1,
                                    name: "first_name".to_string(),
                                    the_type: Text,
                                    nullable: false,
                                    part_of_pk: true,
                                },
                            ],
                            unique: false,
                        },
                        Index {
                            name: "contact_id_reversed".to_string(),
                            columns: vec![Column {
                                id: 0,
                                name: "contact_id".to_string(),
                                the_type: Integer,
                                nullable: false,
                                part_of_pk: true,
                            }],
                            unique: false,
                        },
                    ],
                };
                let user = Table {
                    table_name: "user".to_string(),
                    columns: vec![
                        user_id_column,
                        Column {
                            id: 1,
                            name: "parent_id".to_string(),
                            the_type: Integer,
                            nullable: true,
                            part_of_pk: false,
                        },
                    ],
                    foreign_keys: vec![ForeignKey {
                        id: 0,
                        table: "user".to_string(),
                        from_column: vec![Column {
                            id: 1,
                            name: "parent_id".to_string(),
                            the_type: Integer,
                            nullable: true,
                            part_of_pk: false,
                        }],
                        to_column: vec![Column {
                            id: 0,
                            name: "user_id".to_string(),
                            the_type: Integer,
                            nullable: false,
                            part_of_pk: true,
                        }],
                        on_update: OnUpdateAndDelete::NoAction,
                        on_delete: OnUpdateAndDelete::NoAction,
                    }],
                    indexes: vec![],
                };

                let book = Table {
                    table_name: "book".to_string(),
                    columns: vec![
                        Column {
                            id: 0,
                            name: "contact_id".to_string(),
                            the_type: Integer,
                            nullable: false,
                            part_of_pk: true,
                        },
                        Column {
                            id: 1,
                            name: "first_name".to_string(),
                            the_type: Text,
                            nullable: false,
                            part_of_pk: true,
                        },
                        Column {
                            id: 2,
                            name: "real".to_string(),
                            the_type: Real,
                            nullable: false,
                            part_of_pk: false,
                        },
                        Column {
                            id: 3,
                            name: "blob".to_string(),
                            the_type: Blob,
                            nullable: false,
                            part_of_pk: false,
                        },
                        Column {
                            id: 4,
                            name: "user_id".to_string(),
                            the_type: Integer,
                            nullable: true,
                            part_of_pk: false,
                        },
                    ],
                    foreign_keys: vec![
                        ForeignKey {
                            id: 0,
                            table: "user".to_string(),
                            from_column: vec![Column {
                                id: 4,
                                name: "user_id".to_string(),
                                the_type: Type::Integer,
                                nullable: true,
                                part_of_pk: false,
                            }],
                            to_column: vec![Column {
                                id: 0,
                                name: "user_id".to_string(),
                                the_type: Type::Integer,
                                nullable: false,
                                part_of_pk: true,
                            }],
                            on_update: OnUpdateAndDelete::NoAction,
                            on_delete: OnUpdateAndDelete::NoAction,
                        },
                        ForeignKey {
                            id: 1,
                            table: "contacts".to_string(),
                            from_column: vec![
                                Column {
                                    id: 0,
                                    name: "contact_id".to_string(),
                                    the_type: Type::Integer,
                                    nullable: false,
                                    part_of_pk: true,
                                },
                                Column {
                                    id: 1,
                                    name: "first_name".to_string(),
                                    the_type: Type::Text,
                                    nullable: false,
                                    part_of_pk: true,
                                },
                            ],
                            to_column: vec![
                                Column {
                                    id: 0,
                                    name: "contact_id".to_string(),
                                    the_type: Type::Integer,
                                    nullable: false,
                                    part_of_pk: true,
                                },
                                Column {
                                    id: 1,
                                    name: "first_name".to_string(),
                                    the_type: Type::Text,
                                    nullable: false,
                                    part_of_pk: true,
                                },
                            ],
                            on_update: OnUpdateAndDelete::NoAction,
                            on_delete: OnUpdateAndDelete::NoAction,
                        },
                    ],
                    indexes: vec![Index {
                        name: "real".to_string(),
                        columns: vec![Column {
                            id: 2,
                            name: "real".to_string(),
                            the_type: Real,
                            nullable: false,
                            part_of_pk: false,
                        }],
                        unique: true,
                    }],
                };

                let map: HashMap<String, Table> = vec![contacts, user, book]
                    .into_iter()
                    .map(|v| (v.table_name.clone(), v))
                    .collect();

                assert_eq!(map.get("user"), tables.table("user"));
                assert_eq!(map.get("book"), tables.table("book"));
                assert_eq!(map.get("contacts"), tables.table("contacts"));
                assert_eq!(map, tables.tables);
            }
        }

        parse(&current, &mut Parse {});

        // Done testing, remove the file
        drop(connect);

        std::fs::remove_file(current).unwrap();
    }
}
