use tokio::process::{Command, Child};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::task::JoinHandle;
use std::error::Error;
use std::process::ExitStatus;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Process {
    pub child: Child,
    stdout_buffer: Arc<Mutex<Vec<String>>>,
    pub exit_status: Option<ExitStatus>,
    logs_task: JoinHandle<()>
}

impl Process {
    
    pub async fn run(command: &mut Command, should_wait: bool) -> Result<Self, Box<dyn Error>>  {
        
        let child = command
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;
        
        
        let mut child = child;

        // Create buffers to hold stdout and stderr output
        let stdout_buffer = Arc::new(Mutex::new(Vec::new()));
        let stdout_buffer_copy = stdout_buffer.clone();

        let std_out: tokio::process::ChildStdout = child.stdout.take().unwrap();

        let logs_task = tokio::spawn(async move {
            let mut reader = BufReader::new(std_out);
            let mut line = String::new();

            while let Ok(bytes_read) = reader.read_line(&mut line).await
            {
                if bytes_read == 0 {
                    break;
                }

                stdout_buffer_copy.lock().await.push(line.trim().to_string());

                tracing::debug!("stdout: {}", line.trim());

                line.clear();
            } 
        
        });

        let exit_status = if should_wait {
            Some(child.wait().await?)
        } else {
            None
        };

        Ok(Process {
            child,
            stdout_buffer,
            exit_status,
            logs_task,
        })
    }

    pub async fn get_stdout_output(&self) -> Vec<String> {
        self.stdout_buffer.lock().await.clone()
    }

    pub fn is_running(&self) -> bool {
        if let Some(_) = self.child.id() {
            return true;
        }
        return false;
    }

    pub fn success(&self) -> bool {
        if let Some(exit_status) = &self.exit_status {
            return exit_status.success();
        }
        return false;
    }

    pub async fn kill(&mut self) {
        self.logs_task.abort();
        self.child.kill().await.expect("Failed to kill child process");
    }
}
