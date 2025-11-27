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
"""Python interface for sqlite table provider."""

from typing import Any, List
from . import _internal

class FlightTableFactory:
    """Flight table factory."""

    def __init__(self) -> None:
        """Create a Flight table factory."""
        self._raw = _internal.flight.RawFlightTableFactory()

    def get_table(self, entry_point: str, options: dict) -> Any:
        """Return the table provider for table.

        Args:
            entry_point: uri
            options: table information
        """
        return self._raw.get_table(entry_point, options)
