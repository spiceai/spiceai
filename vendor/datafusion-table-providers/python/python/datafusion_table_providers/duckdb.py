# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Python interface for DuckDB table provider."""

from typing import Any, List
from . import _internal
from enum import Enum

class AccessMode(Enum):
    """Python equivalent of rust duckdb::AccessMode Enum."""
    Automatic = "AUTOMATIC"
    ReadOnly = "READ_ONLY"
    ReadWrite = "READ_WRITE"

class DuckDBTableFactory:
    """DuckDB table factory."""

    def __init__(self, path: str, access_mode: AccessMode = AccessMode.Automatic) -> None:
        """Create a DuckDB table factory.

        If creating an in-memory table factory, then specify path to be :memory: or none
        and don't specify access_mode. If creating a file-based table factory, then
        specify path and access_mode.

        Args:
            path: Memory or file location
            access_mode: Access mode configuration
        """
        # TODO: think about the interface, restrict invalid combination of input
        # arguments, for example, if path is memory, then access_mode should not be
        # specified.
        if path == ":memory:" or path == "":
            self._raw = _internal.duckdb.RawDuckDBTableFactory.new_memory()
        else:
            self._raw = _internal.duckdb.RawDuckDBTableFactory.new_file(path, access_mode.value)

    def tables(self) -> List[str]:
        """Get all the table names."""
        return self._raw.tables()

    def get_table(self, table_reference: str) -> Any:
        """Return table provider for the table named `table_reference`.

        Args:
            table_reference (str): table name
        """
        return self._raw.get_table(table_reference)
