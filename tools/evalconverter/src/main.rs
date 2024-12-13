mod eval;

use anyhow::{Context, Result};
use clap::Parser;
use std::{
    os,
    path::{Path, PathBuf},
    process::exit,
};

use eval::EvalSpecification;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Path to YAML file or directory containing YAML files
    #[arg(short, long)]
    input: PathBuf,

    /// Override base path for resolving relative paths
    #[arg(short, long)]
    base_path: Option<PathBuf>,

    /// Enable verbose logging
    #[arg(short, long, action)]
    verbose: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    env_logger::init_from_env(
        env_logger::Env::default()
            .filter_or("LOG_LEVEL", if cli.verbose { "debug" } else { "info" }),
    );

    // Order of precendence for determining where we expect data (referenced in input YAMLs) to exist.
    // 1. Explicitly provided base path in --base-path
    // 2. Directory of input YAML files (if input is directory)
    // 3. Parent directory of input YAML file (if input is file)
    let data_dir = cli.base_path.unwrap_or(if cli.input.is_dir() {
        cli.input.clone()
    } else {
        cli.input.parent().unwrap().to_path_buf()
    });

    // Determine input handling strategy
    let files = if cli.input.is_dir() {
        yaml_files_from(&cli.input)?
    } else if cli.input.is_file() {
        vec![cli.input]
    } else {
        anyhow::bail!("Input path must be a directory or a YAML file");
    };

    let output: Vec<_> = files
        .iter()
        .map(|f| {
            let e = match EvalSpecification::validate_from_file(f, data_dir.as_path()) {
                Ok(e) => {
                    println!("Eval '{}' is valid.", f.display());
                    e
                }
                Err(e) => {
                    eprintln!("Error validating {:?}: {}", f, e);
                    exit(1);
                }
            };
            e
        })
        .collect::<Vec<_>>();

    println!("{} evals found", output.len());
    Ok(())
}

fn yaml_files_from(dir: &Path) -> Result<Vec<PathBuf>> {
    Ok(std::fs::read_dir(dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();

            if path
                .extension()
                .map_or(false, |ext| ext == "yaml" || ext == "yml")
            {
                Some(path)
            } else {
                None
            }
        })
        .collect::<Vec<PathBuf>>())
}
