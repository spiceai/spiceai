pub const SPICE_CLOUD_FLIGHT_ADDR: &str = "https://flight.spiceai.io";

// default address for local spice runtime
pub const SPICE_LOCAL_FLIGHT_ADDR: &str = "http://localhost:50051";

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
        "spice-rs/{} ({os_type}/{os_release} {os_arch})",
        env!("CARGO_PKG_VERSION")
    )
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_user_agent() {
        let matching_regex = regex::Regex::new(
            r"spice-rs/\d+\.\d+\.\d+ \((Linux|Windows|Darwin)/[\d\w\.\-\_]+ (x86_64|aarch64|i386)\)",
        )
        .expect("regex should be constructed");

        let user_agent = get_user_agent();
        let agent_matches = matching_regex.is_match(&user_agent);
        assert!(
            agent_matches,
            "expected user agent to match regex, but got {user_agent}"
        );
    }

    #[test]
    fn test_spice_cloud_flight_addr() {
        assert_eq!(SPICE_CLOUD_FLIGHT_ADDR, "https://flight.spiceai.io");
        assert!(SPICE_CLOUD_FLIGHT_ADDR.starts_with("https://"));
    }

    #[test]
    fn test_spice_local_flight_addr() {
        assert_eq!(SPICE_LOCAL_FLIGHT_ADDR, "http://localhost:50051");
        assert!(SPICE_LOCAL_FLIGHT_ADDR.starts_with("http://"));
        assert!(SPICE_LOCAL_FLIGHT_ADDR.contains("localhost"));
    }

    #[test]
    fn test_user_agent_contains_version() {
        let user_agent = get_user_agent();
        assert!(user_agent.starts_with("spice-rs/"));
        assert!(user_agent.contains(env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn test_user_agent_contains_os_info() {
        let user_agent = get_user_agent();
        // Should contain OS type (Darwin, Linux, Windows)
        assert!(
            user_agent.contains("Darwin")
                || user_agent.contains("Linux")
                || user_agent.contains("Windows")
        );
        // Should contain architecture (x86_64, aarch64, i386)
        assert!(
            user_agent.contains("x86_64")
                || user_agent.contains("aarch64")
                || user_agent.contains("i386")
        );
    }

    #[test]
    fn test_get_os_release_returns_string() {
        let result = get_os_release();
        assert!(result.is_ok());
        let release = result.expect("should get os release");
        assert!(!release.is_empty());
    }

    // Edge case tests

    #[test]
    fn test_user_agent_format_structure() {
        let user_agent = get_user_agent();

        // Should have format: spice-rs/VERSION (OS/RELEASE ARCH)
        assert!(user_agent.contains("spice-rs/"));
        assert!(user_agent.contains('('));
        assert!(user_agent.contains(')'));
        assert!(user_agent.contains('/'));
    }

    #[test]
    fn test_spice_cloud_flight_addr_is_valid_url() {
        // Ensure it's a valid URL format
        assert!(SPICE_CLOUD_FLIGHT_ADDR.starts_with("https://"));
        assert!(!SPICE_CLOUD_FLIGHT_ADDR.ends_with('/'));
        assert!(SPICE_CLOUD_FLIGHT_ADDR.contains('.'));
    }

    #[test]
    fn test_spice_local_flight_addr_port() {
        // Verify the local address has the expected port
        assert!(SPICE_LOCAL_FLIGHT_ADDR.contains(":50051"));
    }

    #[test]
    fn test_user_agent_no_newlines() {
        let user_agent = get_user_agent();
        assert!(!user_agent.contains('\n'));
        assert!(!user_agent.contains('\r'));
    }

    #[test]
    fn test_user_agent_not_empty() {
        let user_agent = get_user_agent();
        assert!(!user_agent.is_empty());
        assert!(user_agent.len() > 10); // Should have substantial content
    }
}
