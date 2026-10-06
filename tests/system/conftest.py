# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared configuration for system tests.

By default, connector tests connect over private IP and every test runs.
This requires network access to the instance's VPC.

Pass --skip-private-ip to run from outside the VPC. Connector tests then
connect over public IP, and tests marked private_ip (PSC and direct
connection tests) are skipped.
"""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--skip-private-ip",
        action="store_true",
        default=False,
        help="Skip tests that need private IP access and connect over public IP.",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "private_ip: test needs network access to the instance's VPC",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if not config.getoption("--skip-private-ip"):
        return
    skip = pytest.mark.skip(reason="needs private IP access (--skip-private-ip)")
    for item in items:
        if "private_ip" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def ip_type(request: pytest.FixtureRequest) -> str:
    """IP type for connector tests, PUBLIC when --skip-private-ip is set."""
    return "PUBLIC" if request.config.getoption("--skip-private-ip") else "PRIVATE"
