/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Table formatting for CLI output.

use comfy_table::{Cell, Table, presets::NOTHING};
use std::fmt;

/// A trait for types that can be displayed as a table row.
pub trait TableRow {
    /// Returns the column headers for this row type.
    fn headers() -> Vec<&'static str>;

    /// Returns the values for this row as strings.
    fn values(&self) -> Vec<String>;
}

/// Write a table to stdout.
pub fn write_table<T: TableRow>(rows: &[T]) {
    if rows.is_empty() {
        return;
    }

    let mut table = Table::new();
    table.load_preset(NOTHING);

    // Add headers
    let headers: Vec<Cell> = T::headers().into_iter().map(Cell::new).collect();
    table.set_header(headers);

    // Add rows
    for row in rows {
        let cells: Vec<Cell> = row.values().into_iter().map(Cell::new).collect();
        table.add_row(cells);
    }

    println!("{table}");
}

/// A simple table output builder for ad-hoc tables.
pub struct TableOutput {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl TableOutput {
    /// Create a new table with the given column headers.
    pub fn new(headers: Vec<&str>) -> Self {
        Self {
            headers: headers.into_iter().map(String::from).collect(),
            rows: Vec::new(),
        }
    }

    /// Add a row to the table.
    pub fn add_row(&mut self, values: Vec<String>) {
        self.rows.push(values);
    }

    /// Print the table to stdout.
    pub fn print(&self) {
        println!("{self}");
    }

    /// Build the comfy-table Table.
    fn build_table(&self) -> Table {
        let mut table = Table::new();
        table.load_preset(NOTHING);

        // Add headers
        let headers: Vec<Cell> = self.headers.iter().map(Cell::new).collect();
        table.set_header(headers);

        // Add rows
        for row in &self.rows {
            let cells: Vec<Cell> = row.iter().map(Cell::new).collect();
            table.add_row(cells);
        }

        table
    }
}

impl fmt::Display for TableOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.build_table())
    }
}
