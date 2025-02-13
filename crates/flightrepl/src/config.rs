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

pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[cfg(target_family = "unix")]
fn get_os_release() -> Result<String, GenericError> {
    // call uname -r to get release text
    use std::process::Command;
    let output = Command::new("uname").arg("-r").output()?;
    let release = String::from_utf8(output.stdout)?;

    Ok(release)
}

#[cfg(target_family = "windows")]
fn get_os_release() -> Result<String, GenericError> {
    use winver::WindowsVersion;
    if let Some(version) = WindowsVersion::detect() {
        Ok(version.to_string())
    } else {
        Ok("unknown".to_string())
    }
}

pub(crate) fn get_user_agent() -> String {
    let os_type = std::env::consts::OS;
    let os_type = match os_type {
        "" => "unknown".to_string(),
        "macos" => "Darwin".to_string(),
        "linux" => "Linux".to_string(),
        "windows" => "Windows".to_string(),
        "ios" => "iOS".to_string(),
        "android" => "Android".to_string(),
        "freebsd" => "FreeBSD".to_string(),
        "dragonfly" => "DragonFlyBSD".to_string(),
        "netbsd" => "NetBSD".to_string(),
        "openbsd" => "OpenBSD".to_string(),
        "solaris" => "Solaris".to_string(),
        _ => os_type.to_string(),
    };

    let os_arch = std::env::consts::ARCH;
    let os_arch = match os_arch {
        "" => "unknown".to_string(),
        "x86" => "i386".to_string(),
        _ => os_arch.to_string(),
    };

    let os_release = get_os_release()
        .unwrap_or_else(|_| "unknown".to_string())
        .trim()
        .to_string();

    format!(
        "flightrepl/{} ({os_type}/{os_release} {os_arch})",
        env!("CARGO_PKG_VERSION")
    )
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_user_agent() {
        let matching_regex = regex::Regex::new(
            r"flightrepl/\d+\.\d+\.\d+(-beta|-stable|-rc|-unstable)?(\.\d+)? \((Linux|Windows|Darwin)/[\d\w\.\-\_]+ (x86_64|aarch64|i386)\)",
        )
        .expect("regex should be constructed");

        let user_agent = get_user_agent();
        let agent_matches = matching_regex.is_match(&user_agent);
        assert!(
            agent_matches,
            "expected user agent to match regex, but got {user_agent}"
        );
    }
}
