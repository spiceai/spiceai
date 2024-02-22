use std::time::Duration;

use  tokio::process::Command;
use tempfile::tempdir;

use tracing_test::traced_test;
use crate::common::Process;

mod common;

pub struct TestUtils {
    app_dir: tempfile::TempDir
}

impl TestUtils {
    pub fn new(app_dir: tempfile::TempDir) -> Self {

        TestUtils {app_dir}
    }

    pub async fn run_spiced(&self) -> Process  {

        let mut cmd = Command::new("spiced");
        cmd.current_dir(self.app_dir.path());

        return Process::run(&mut cmd, false).await.expect("Should be able to run spiced");
    }

    pub async fn run_once(&self, cmd: &mut Command) -> Process  {
        return Process::run(cmd, true).await.expect("Should be able to run a CLI command");
    }

    pub async fn wait_for(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }

    pub fn spice_cmd(&self) -> Command {
        let mut cmd = Command::new("spice");
        cmd.current_dir(self.app_dir.path());
        return cmd;
    }

    pub async fn output_contains(&self, p: &Process, expected: &str) -> bool {
       
       let logs  = p.get_stdout_output().await;

       for log in logs {
            if log.contains(expected) {
                return true;
            }
        }
        return false;
    }
}

async fn setup() -> TestUtils {

    // Create a directory inside of `std::env::temp_dir()`
    let app_dir: tempfile::TempDir = tempdir().expect("Failed to create temporary directory");

    tracing::info!("Created temporary directory: {}", app_dir.path().to_string_lossy());
    
    return TestUtils::new(app_dir);    
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;


    #[tokio::test]
    #[traced_test]
    async fn  app_start() {
        
        let utils = setup().await;

        let res = utils.run_once(utils.spice_cmd().arg("init").arg("test-app")).await;
        assert_eq!(res.success(), true, "should successfully initialize a new spicepod");

        let mut spiced = utils.run_spiced().await;

        // allow app to initialize
        utils.wait_for(Duration::from_secs(5)).await;

        assert_eq!(spiced.is_running(), true, "should successfully start the spiced");

        assert_eq!(utils.output_contains(&spiced, "HTTP listening on").await, true, "should start HTTP server");
        assert_eq!(utils.output_contains(&spiced, "OpenTelemetry listening on").await, true, "should start OpenTelemetry server");
        assert_eq!(utils.output_contains(&spiced, "Flight listening on").await, true, "should start OpenTelemetry server");
    
        assert_eq!(utils.output_contains(&spiced, "Runtime error").await, false, "should not contain runtime errors");

        spiced.kill().await;
    }

    #[tokio::test]
    #[traced_test]
    async fn  dyn_spicepods_dataset() {
        
        let utils = setup().await;

        let res = utils.run_once(utils.spice_cmd().arg("init").arg("test-app")).await;
        assert_eq!(res.success(), true, "should successfully initialize a new spicepod");

        let mut spiced = utils.run_spiced().await;

        // allow app to initialize
        utils.wait_for(Duration::from_secs(5)).await;

        assert_eq!(spiced.is_running(), true, "should successfully start the spiced");

        let res = utils.run_once(utils.spice_cmd().arg("add").arg("lukekim/demo")).await;

        // allow app to initialize
        utils.wait_for(Duration::from_secs(3)).await;

        assert_eq!(res.success(), true, "should be able to add a new spicepod dynamically");

        assert_eq!(utils.output_contains(&spiced, "Loaded dataset: eth_recent_blocks").await, true, "should be able to load eth_recent_blocks dynamically");
    
        assert_eq!(utils.output_contains(&spiced, "Runtime error").await, false, "should not contain runtime errors");

        spiced.kill().await;
    }
}


