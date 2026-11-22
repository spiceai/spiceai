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

//! An ODBC dataset never names its driver by file path, whatever else it
//! configures: the driver manager would load that library into `spiced`.
//!
//! Runs the real binary, built with the `odbc` feature and unixODBC installed.

use std::net::{SocketAddr, TcpListener};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const REFUSAL: &str = "Accessing an ODBC driver with a file path is not permitted.";

fn unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("read ephemeral address")
}

/// Start `spiced` on a Spicepod with one ODBC dataset `t` configured by
/// `params`, and return what it printed once dataset `t` has failed to load.
fn dataset_load_output(params: &str) -> String {
    let dir = tempfile::tempdir().expect("create the instance");
    std::fs::write(
        dir.path().join("spicepod.yaml"),
        format!(
            "version: v2\nkind: Spicepod\nname: odbc-driver-path\ndatasets:\n  - from: odbc:some_table\n    name: t\n    params:\n{params}"
        ),
    )
    .expect("write the spicepod");
    let log = dir.path().join("spiced.log");
    let file = std::fs::File::create(&log).expect("create the log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_spiced"))
        .current_dir(dir.path())
        .args(["--http", &unused_local_addr().to_string()])
        .args(["--flight", &unused_local_addr().to_string()])
        .args(["--metrics", &unused_local_addr().to_string()])
        .args(["--open_telemetry", &unused_local_addr().to_string()])
        .stdout(Stdio::from(file.try_clone().expect("share the log")))
        .stderr(Stdio::from(file))
        .spawn()
        .expect("start spiced");

    let deadline = Instant::now() + Duration::from_mins(2);
    let printed = loop {
        let printed = std::fs::read_to_string(&log).unwrap_or_default();
        if printed.contains("dataset t") || Instant::now() >= deadline {
            break printed;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let _ = child.kill();
    let _ = child.wait();
    printed
}

#[test]
fn a_driver_given_as_a_path_is_refused_for_every_configuration() {
    for (case, params) in [
        (
            "a braced path",
            "      odbc_connection_string: 'Driver={/opt/odbc/libDriver.so};'\n",
        ),
        (
            "a path with sql_dialect set",
            "      odbc_connection_string: 'Driver=/opt/odbc/libDriver.so;'\n      sql_dialect: postgresql\n",
        ),
        (
            "a file DSN",
            "      odbc_connection_string: 'FILEDSN=/opt/odbc/driver.dsn;'\n      sql_dialect: postgresql\n",
        ),
    ] {
        let printed = dataset_load_output(params);
        assert!(
            printed.contains(REFUSAL),
            "{case}: the connector must refuse it before the driver manager loads anything: {printed}"
        );
    }
}
