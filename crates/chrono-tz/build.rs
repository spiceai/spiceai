/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2016-2025 The chrono-tz contributors

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

//! Generates the time zone table from the IANA tz database source files in `tz/`.
//!
//! This is the code generator of chrono-tz-build 0.5.0 (the crate that produced the
//! tables chrono-tz 0.10.4 ships), without its `filter-by-regex` and `case-insensitive`
//! modes. It writes `timezones.rs` (the `Tz` enum, its name lookup and its transitions)
//! and `directory.rs` (the `Europe::London`-style constants and `IANA_TZDB_VERSION`, which
//! only the tests use) to `OUT_DIR`, where `src/lib.rs` includes them.

use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::fmt::{self, Write as _};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use parse_zoneinfo::FILES;
use parse_zoneinfo::line::Line;
use parse_zoneinfo::structure::{Child, Structure};
use parse_zoneinfo::table::{Table, TableBuilder};
use parse_zoneinfo::transitions::{FixedTimespan, TableTransitions};

// This function is needed until zoneinfo_parse handles comments correctly.
// Technically a '#' symbol could occur between double quotes and should be
// ignored in this case, however this never happens in the tz database as it
// stands.
fn strip_comments(mut line: String) -> String {
    if let Some(pos) = line.find('#') {
        line.truncate(pos);
    }
    line
}

// Generate a list of the time zone periods beyond the first that apply
// to this zone, as a string representation of a static slice.
fn format_rest(rest: Vec<(i64, FixedTimespan)>) -> Result<String, fmt::Error> {
    let mut slice = "&[\n".to_string();
    for (
        start,
        FixedTimespan {
            utc_offset,
            dst_offset,
            name,
        },
    ) in rest
    {
        let timespan_name = match name.as_ref() {
            "%z" => None,
            name => Some(name),
        };
        writeln!(
            slice,
            "                    ({start}, FixedTimespan {{ \
             utc_offset: {utc_offset}, dst_offset: {dst_offset}, name: {timespan_name:?} \
             }}),",
        )?;
    }
    slice.push_str("                ]");
    Ok(slice)
}

// Convert all '/' to '__', all '+' to 'Plus' and '-' to 'Minus', unless
// it's a hyphen, in which case remove it. This is so the names can be used
// as rust identifiers.
fn convert_bad_chars(name: &str) -> String {
    let name = name.replace('/', "__").replace('+', "Plus");
    if let Some(pos) = name.find('-') {
        if name[pos + 1..].chars().next().is_some_and(char::is_numeric) {
            name.replace('-', "Minus")
        } else {
            name.replace('-', "")
        }
    } else {
        name
    }
}

// The timezone file contains impls of `Timespans` for all timezones in the
// database. The `Wrap` wrapper in the `timezone_impl` module then implements
// TimeZone for any contained struct that implements `Timespans`.
fn write_timezone_file(
    timezone_file: &mut impl Write,
    table: &Table,
) -> Result<(), Box<dyn Error>> {
    let zones = table
        .zonesets
        .keys()
        .chain(table.links.keys())
        .collect::<BTreeSet<_>>();
    writeln!(
        timezone_file,
        "use core::fmt::{{self, Debug, Display, Formatter}};",
    )?;
    writeln!(timezone_file, "use core::str::FromStr;\n")?;
    writeln!(
        timezone_file,
        "use crate::timezone_impl::{{TimeSpans, FixedTimespanSet, FixedTimespan}};\n",
    )?;
    writeln!(
        timezone_file,
        "/// TimeZones built at compile time from the tz database
///
/// This implements [`chrono::TimeZone`] so that it may be used in and to
/// construct chrono's DateTime type. See the root module documentation
/// for details."
    )?;
    writeln!(timezone_file, "#[derive(Clone, Copy, PartialEq, Eq, Hash)]")?;
    writeln!(timezone_file, "pub enum Tz {{")?;
    for zone in &zones {
        let zone_name = convert_bad_chars(zone);
        writeln!(timezone_file, "    /// {zone}\n    {zone_name},")?;
    }
    writeln!(timezone_file, "}}")?;

    let mut map = phf_codegen::Map::new();
    for zone in &zones {
        map.entry(zone, format!("Tz::{}", convert_bad_chars(zone)));
    }
    writeln!(
        timezone_file,
        "static TIMEZONES: ::phf::Map<&'static str, Tz> = \n{};",
        map.build()
    )?;

    writeln!(
        timezone_file,
        r#"#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ParseError(());

impl Display for ParseError {{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {{
        f.write_str("failed to parse timezone")
    }}
}}

impl std::error::Error for ParseError {{}}

impl FromStr for Tz {{
    type Err = ParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {{
        TIMEZONES.get(s).cloned().ok_or(ParseError(()))
    }}
}}
"#
    )?;

    writeln!(
        timezone_file,
        "impl Tz {{
    pub fn name(self) -> &'static str {{
        match self {{"
    )?;
    for zone in &zones {
        let zone_name = convert_bad_chars(zone);
        writeln!(timezone_file, "            Tz::{zone_name} => \"{zone}\",")?;
    }
    writeln!(
        timezone_file,
        "        }}
    }}"
    )?;

    writeln!(timezone_file, "}}")?;

    writeln!(
        timezone_file,
        "impl Debug for Tz {{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {{
        f.write_str(self.name().as_ref())
    }}
}}\n"
    )?;
    writeln!(
        timezone_file,
        "impl Display for Tz {{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {{
        f.write_str(self.name().as_ref())
    }}
}}\n"
    )?;
    writeln!(
        timezone_file,
        "impl TimeSpans for Tz {{
    fn timespans(&self) -> FixedTimespanSet {{
        match *self {{"
    )?;
    for zone in &zones {
        let timespans = table
            .timespans(zone)
            .ok_or_else(|| format!("time zone '{zone}' has no timespans"))?;
        let zone_name = convert_bad_chars(zone);
        let timespan_name = match timespans.first.name.as_ref() {
            "%z" => None,
            name => Some(name),
        };
        writeln!(
            timezone_file,
            "            Tz::{zone} => {{
                const REST: &[(i64, FixedTimespan)] = {rest};
                FixedTimespanSet {{
                    first: FixedTimespan {{
                        utc_offset: {utc},
                        dst_offset: {dst},
                        name: {name:?},
                    }},
                    rest: REST
                }}
            }},\n",
            zone = zone_name,
            rest = format_rest(timespans.rest)?,
            utc = timespans.first.utc_offset,
            dst = timespans.first.dst_offset,
            name = timespan_name,
        )?;
    }
    writeln!(
        timezone_file,
        "         }}
    }}
}}"
    )?;
    write!(
        timezone_file,
        "/// Every zone, for the tests that compare all of them.
#[cfg(test)]
pub static TZ_VARIANTS: [Tz; {num}] = [
",
        num = zones.len()
    )?;
    for zone in &zones {
        writeln!(
            timezone_file,
            "    Tz::{zone},",
            zone = convert_bad_chars(zone)
        )?;
    }
    write!(timezone_file, "];")?;
    Ok(())
}

// Create a file containing nice-looking re-exports such as Europe::London
// instead of having to use chrono_tz::timezones::Europe__London
fn write_directory_file(
    directory_file: &mut impl Write,
    table: &Table,
    version: &str,
) -> io::Result<()> {
    // expose the underlying IANA TZDB version
    writeln!(
        directory_file,
        "pub const IANA_TZDB_VERSION: &str = \"{version}\";\n"
    )?;
    // add the `loose' zone definitions first
    writeln!(directory_file, "use crate::timezones::Tz;\n")?;
    let zones = table
        .zonesets
        .keys()
        .chain(table.links.keys())
        .filter(|zone| !zone.contains('/'))
        .collect::<BTreeSet<_>>();
    for zone in zones {
        let zone = convert_bad_chars(zone);
        writeln!(directory_file, "pub const {zone}: Tz = Tz::{zone};")?;
    }
    writeln!(directory_file)?;

    // now add the `structured' zone names in submodules
    let mut first = true;
    for entry in table.structure() {
        if entry.name.contains('/') {
            continue;
        }

        if first {
            first = false;
        } else {
            writeln!(directory_file)?;
        }

        let module_name = convert_bad_chars(entry.name);
        writeln!(directory_file, "pub mod {module_name} {{")?;
        writeln!(directory_file, "    use crate::timezones::Tz;\n")?;
        for child in entry.children {
            let name = match child {
                Child::Submodule(name) => name,
                Child::TimeZone(name) => {
                    let name = convert_bad_chars(name);
                    writeln!(
                        directory_file,
                        "    pub const {name}: Tz = Tz::{module_name}__{name};"
                    )?;
                    continue;
                }
            };

            let submodule_name = convert_bad_chars(name);
            writeln!(directory_file, "    pub mod {submodule_name} {{")?;
            writeln!(directory_file, "        use crate::timezones::Tz;\n")?;
            let full_name = entry.name.to_string() + "/" + name;
            for entry in table.structure() {
                if entry.name != full_name {
                    continue;
                }

                for child in entry.children {
                    let name = match child {
                        Child::Submodule(_) => {
                            return Err(io::Error::other(
                                "Depth of > 3 nested submodules not implemented!",
                            ));
                        }
                        Child::TimeZone(name) => name,
                    };

                    let converted_name = convert_bad_chars(name);
                    writeln!(
                        directory_file,
                        "        pub const {converted_name}: Tz = Tz::{module_name}__{submodule_name}__{converted_name};",
                    )?;
                }
            }
            writeln!(directory_file, "    }}\n")?;
        }
        writeln!(directory_file, "}}")?;
    }

    Ok(())
}

/// The tz database release, from the first `Release <version> - <date>` line of `tz/NEWS`.
fn detect_iana_db_version(tz_dir: &Path) -> Result<String, Box<dyn Error>> {
    let file = File::open(tz_dir.join("NEWS"))?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        let Some(line) = line.strip_prefix("Release ") else {
            continue;
        };
        if let Some((version, _)) = line.split_once(" - ") {
            return Ok(version.to_owned());
        }
    }
    Err("tz/NEWS names no release".into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let tz_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("tz");
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    println!("cargo:rerun-if-changed=tz");

    let mut lines = Vec::new();
    for fname in FILES {
        let path = tz_dir.join(fname);
        let file = File::open(&path).map_err(|e| format!("cannot open {}: {e}", path.display()))?;
        for line in BufReader::new(file).lines() {
            lines.push(strip_comments(line?));
        }
    }
    let mut table = TableBuilder::new();
    for line in &lines {
        let parsed = Line::new(line).map_err(|e| format!("cannot parse tz line '{line}': {e}"))?;
        table
            .add_line(parsed)
            .map_err(|e| format!("cannot add tz line '{line}': {e}"))?;
    }
    let table = table.build();

    let mut timezone_file = BufWriter::new(File::create(out_dir.join("timezones.rs"))?);
    write_timezone_file(&mut timezone_file, &table)?;
    timezone_file.flush()?;

    let mut directory_file = BufWriter::new(File::create(out_dir.join("directory.rs"))?);
    write_directory_file(
        &mut directory_file,
        &table,
        &detect_iana_db_version(&tz_dir)?,
    )?;
    directory_file.flush()?;
    Ok(())
}
