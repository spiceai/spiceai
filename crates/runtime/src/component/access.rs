/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use spicepod::component::access::AccessMode as SpicepodAccessMode;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum AccessMode {
    #[default]
    Read,
    ReadWrite,
    /// Full read-write access including DDL operations (CREATE TABLE, DROP TABLE, etc.)
    ReadWriteCreate,
}

impl AccessMode {
    /// Returns true if this access mode allows write operations (INSERT, UPDATE, DELETE).
    #[must_use]
    pub fn allows_write(&self) -> bool {
        matches!(self, AccessMode::ReadWrite | AccessMode::ReadWriteCreate)
    }

    /// Returns true if this access mode allows DDL operations (CREATE TABLE, DROP TABLE, etc.).
    #[must_use]
    pub fn allows_ddl(&self) -> bool {
        matches!(self, AccessMode::ReadWriteCreate)
    }
}

impl From<SpicepodAccessMode> for AccessMode {
    fn from(mode: SpicepodAccessMode) -> Self {
        match mode {
            SpicepodAccessMode::Read => AccessMode::Read,
            SpicepodAccessMode::ReadWrite => AccessMode::ReadWrite,
            SpicepodAccessMode::ReadWriteCreate => AccessMode::ReadWriteCreate,
        }
    }
}
