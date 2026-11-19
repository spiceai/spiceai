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

//! A cloud-managed instance starts only on a deployment file Spice wrote, from
//! a state directory only its own account can change.
//!
//! The persisted deployment has a fixed name in the instance's Cloud Connect
//! state directory (`.spice/cloud-connect`), known before any deployment
//! lands. A link, or a second hard link, put at that name must not choose the
//! configuration `spiced` serves: startup refuses it and names the file and
//! the fix. The state directory itself is created owner-only, repaired when it
//! is writable by others and owned by this account, and filled from the config
//! directory where an earlier version kept the same files. These run the real
//! binary, because the configuration is chosen before anything a library test
//! could observe.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

const DEPLOYMENT_FILE: &str = "spicepod-cloud-managed.yml";

const LOCAL_SPICEPOD: &str = "version: v1\nkind: Spicepod\nname: local-app\n";

/// A spicepod outside the config directory, standing for one somebody else chose.
const CHOSEN_ELSEWHERE: &str = "version: v1\nkind: Spicepod\nname: chosen-elsewhere\n";

/// The log line that says the instance selected its deployment file.
const SERVES_DEPLOYMENT: &str = "this instance serves the deployed spicepod at";

/// The log line that says an instance with no deployment is serving.
const LISTENING: &str = "Spice Runtime HTTP listening on";

const STATE_DIR: &str = "cloud-connect";

/// Mode bits, for a state directory's permissions.
#[cfg(unix)]
fn mode(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::metadata(path)
        .expect("stat the directory")
        .permissions()
        .mode()
        & 0o7777
}

fn unused_local_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("read ephemeral address")
}

/// An instance directory whose state directory holds an identity that counts
/// as observed (a malformed one does: it proves the instance was enrolled), and
/// a spicepod outside it. Returns the root, the instance directory and the
/// outside spicepod.
fn enrolled_instance() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let (root, instance, elsewhere) = instance_with_config_dir();
    let state = instance.join(".spice").join(STATE_DIR);
    std::fs::create_dir(&state).expect("create the state directory");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o700))
            .expect("make the state directory owner-only");
    }
    std::fs::rename(
        instance.join(".spice").join("identity.json"),
        state.join("identity.json"),
    )
    .expect("move the identity into the state directory");
    (root, instance, elsewhere)
}

/// [`enrolled_instance`] as an earlier version left it: the identity in the
/// config directory itself.
fn instance_with_config_dir() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let root = tempfile::tempdir().expect("create the test root");
    // `spiced` names files under its working directory as the OS resolves it
    // (`/private/var/…` for a macOS temp dir), so the expected paths must too.
    let instance = root
        .path()
        .canonicalize()
        .expect("resolve the test root")
        .join("instance");
    std::fs::create_dir_all(instance.join(".spice")).expect("create the config directory");
    std::fs::write(instance.join("spicepod.yaml"), LOCAL_SPICEPOD).expect("write the spicepod");
    std::fs::write(
        instance.join(".spice").join("identity.json"),
        "not valid identity JSON",
    )
    .expect("write the identity");
    let elsewhere = root.path().join("chosen.yaml");
    std::fs::write(&elsewhere, CHOSEN_ELSEWHERE).expect("write the outside spicepod");
    (root, instance, elsewhere)
}

/// Run `spiced` in `instance` until it exits or `SERVES_DEPLOYMENT` is
/// printed, then stop it. Returns how it ended (`None` if it was still running)
/// and everything it printed.
fn run_spiced(instance: &Path) -> (Option<ExitStatus>, String) {
    run_spiced_until(instance, SERVES_DEPLOYMENT)
}

/// [`run_spiced`], stopping once `until` is printed.
fn run_spiced_until(instance: &Path, until: &str) -> (Option<ExitStatus>, String) {
    let log_path = instance.join("spiced.log");
    let log = std::fs::File::create(&log_path).expect("create the log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_spiced"))
        .current_dir(instance)
        // The config directory is resolved from the working directory.
        .env_remove("SPICE_CONFIG_DIR")
        .args(["--http", &unused_local_addr().to_string()])
        .args(["--flight", &unused_local_addr().to_string()])
        .args(["--metrics", &unused_local_addr().to_string()])
        .args(["--open_telemetry", &unused_local_addr().to_string()])
        .stdout(Stdio::from(log.try_clone().expect("share the log")))
        .stderr(Stdio::from(log))
        .spawn()
        .expect("start spiced");

    // A default-feature `spiced` binary is large enough that cold macOS
    // loading and signature checks can take tens of seconds on a busy builder.
    let deadline = Instant::now() + Duration::from_mins(2);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll spiced") {
            break Some(status);
        }
        let printed = std::fs::read_to_string(&log_path).unwrap_or_default();
        if printed.contains(until) || Instant::now() >= deadline {
            child.kill().expect("stop spiced");
            child.wait().expect("reap spiced");
            break None;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    (
        status,
        std::fs::read_to_string(&log_path).unwrap_or_default(),
    )
}

fn assert_refused(instance: &Path, reason: &str) {
    let (status, printed) = run_spiced(instance);
    let deployment = instance
        .join(".spice")
        .join(STATE_DIR)
        .join(DEPLOYMENT_FILE);
    assert!(
        status.is_some_and(|status| !status.success()),
        "spiced must refuse to start ({status:?}): {printed}"
    );
    assert!(
        printed.contains(&format!(
            "Failed to read the persisted Spice Cloud deployment at {}: {reason}",
            deployment.display()
        )),
        "the error must name the file and why it was refused: {printed}"
    );
    assert!(
        printed.contains(&format!(
            "{reason}. Spice loads only the deployment file it wrote itself, so this one is not loaded. Delete it; the next Spice Cloud deployment writes it again, and `chmod go-w '{}'` keeps others from replacing it. See: https://spiceai.org/docs/cli/reference/cloud",
            instance.join(".spice").join(STATE_DIR).display()
        )),
        "the error must say how to recover: {printed}"
    );
    assert!(
        !printed.contains(SERVES_DEPLOYMENT),
        "the refused file must not be served: {printed}"
    );
}

/// A symbolic link at the deployment name is refused, not followed.
#[cfg(unix)]
#[test]
fn a_linked_deployment_file_is_refused_at_startup() {
    let (_root, instance, elsewhere) = enrolled_instance();
    std::os::unix::fs::symlink(
        &elsewhere,
        instance
            .join(".spice")
            .join(STATE_DIR)
            .join(DEPLOYMENT_FILE),
    )
    .expect("link the deployment file");

    assert_refused(&instance, "it is a symbolic link");
}

/// A second hard link to a file elsewhere is refused: the file a deployment
/// writes has exactly one name.
#[cfg(unix)]
#[test]
fn a_hard_linked_deployment_file_is_refused_at_startup() {
    let (_root, instance, elsewhere) = enrolled_instance();
    std::fs::hard_link(
        &elsewhere,
        instance
            .join(".spice")
            .join(STATE_DIR)
            .join(DEPLOYMENT_FILE),
    )
    .expect("hard-link the deployment file");

    assert_refused(&instance, "it is hard-linked");
}

/// The control: a deployment file like the one a deployment writes is still
/// selected, so the refusals above are about the file and not the setup, and
/// an owner-only state directory is used as it is.
#[test]
fn a_deployment_file_spice_wrote_is_served() {
    let (_root, instance, _elsewhere) = enrolled_instance();
    let deployment = instance
        .join(".spice")
        .join(STATE_DIR)
        .join(DEPLOYMENT_FILE);
    std::fs::write(&deployment, CHOSEN_ELSEWHERE).expect("write the deployment file");

    let (_status, printed) = run_spiced(&instance);
    assert!(
        printed.contains(&format!("{SERVES_DEPLOYMENT} '{}'", deployment.display())),
        "the deployment file must be selected: {printed}"
    );
    assert!(
        !printed.contains("removed group and other write access"),
        "an owner-only state directory needs no repair: {printed}"
    );
    #[cfg(unix)]
    assert_eq!(mode(&instance.join(".spice").join(STATE_DIR)), 0o700);
}

/// A state directory this account owns but others can write to (a umask of
/// `002`) loses group and other write access, says so once, and is used.
#[cfg(unix)]
#[test]
fn a_group_writable_state_directory_is_repaired_and_used() {
    use std::os::unix::fs::PermissionsExt as _;

    let (_root, instance, _elsewhere) = enrolled_instance();
    let state = instance.join(".spice").join(STATE_DIR);
    let deployment = state.join(DEPLOYMENT_FILE);
    std::fs::write(&deployment, CHOSEN_ELSEWHERE).expect("write the deployment file");
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o775))
        .expect("make the state directory group-writable");

    let (_status, printed) = run_spiced(&instance);
    let repaired = format!(
        "INFO spiced::cloud_connect: Spice Cloud Connect removed group and other write access from its state directory '{}' (mode 0775 → 0755), so that only this account can add files there. See: https://spiceai.org/docs/cli/reference/cloud",
        state.display()
    );
    assert_eq!(
        printed.matches(&repaired).count(),
        1,
        "the repair is logged exactly once: {printed}"
    );
    assert_eq!(mode(&state), 0o755);
    assert!(
        printed.contains(&format!("{SERVES_DEPLOYMENT} '{}'", deployment.display())),
        "the repaired state directory is used: {printed}"
    );
}

/// Files an earlier version kept in the config directory move into the state
/// directory on the first start, which then serves the deployment from there.
#[cfg(unix)]
#[test]
fn state_kept_in_the_config_directory_moves_into_the_state_directory() {
    let (_root, instance, _elsewhere) = instance_with_config_dir();
    let config_dir = instance.join(".spice");
    std::fs::write(config_dir.join(DEPLOYMENT_FILE), CHOSEN_ELSEWHERE)
        .expect("write the deployment file where an earlier version kept it");

    let (_status, printed) = run_spiced(&instance);
    let state = config_dir.join(STATE_DIR);
    let deployment = state.join(DEPLOYMENT_FILE);
    assert!(
        printed.contains(&format!("{SERVES_DEPLOYMENT} '{}'", deployment.display())),
        "the moved deployment is served from the state directory: {printed}"
    );
    assert_eq!(
        std::fs::read_to_string(&deployment).expect("read the moved deployment"),
        CHOSEN_ELSEWHERE
    );
    assert_eq!(
        std::fs::read_to_string(state.join("identity.json")).expect("read the moved identity"),
        "not valid identity JSON"
    );
    assert!(!config_dir.join(DEPLOYMENT_FILE).exists());
    assert!(!config_dir.join("identity.json").exists());
    assert_eq!(mode(&state), 0o700);
}

/// An instance that never enrolled starts as it always did: no state
/// directory is created and nothing about Cloud Connect is logged.
#[test]
fn an_instance_that_never_enrolled_creates_no_state_directory() {
    let root = tempfile::tempdir().expect("create the test root");
    let instance = root
        .path()
        .canonicalize()
        .expect("resolve the test root")
        .join("instance");
    std::fs::create_dir_all(&instance).expect("create the instance directory");
    std::fs::write(instance.join("spicepod.yaml"), LOCAL_SPICEPOD).expect("write the spicepod");

    let (_status, printed) = run_spiced_until(&instance, LISTENING);
    assert!(printed.contains(LISTENING), "spiced must start: {printed}");
    assert!(
        !instance.join(".spice").join(STATE_DIR).exists(),
        "an instance that never enrolled has no state directory"
    );
    assert!(
        !printed.contains("Cloud Connect"),
        "nothing about Cloud Connect is logged: {printed}"
    );
}
