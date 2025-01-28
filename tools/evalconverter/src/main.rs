mod eval;

use anyhow::Result;
use clap::Parser;
use spicepod::{
    component::{dataset::Dataset, eval::Eval, ComponentOrReference},
    spec::{SpicepodDefinition, SpicepodKind, SpicepodVersion},
};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::exit,
};

use eval::{spice_components, EvalSpecification};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Path to YAML file or directory containing YAML files
    #[arg(short, long)]
    input: PathBuf,

    /// Override base path for resolving relative paths
    #[arg(short, long)]
    base_path: Option<PathBuf>,

    /// Path to write Spicepod YAML file
    #[arg(short, long, default_value = "spicepod.yml")]
    spicepod_output: PathBuf,

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
        let Some(parent) = cli.input.parent() else {
            anyhow::bail!("Input path must be a directory or a YAML file");
        };
        parent.to_path_buf()
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
        .flat_map(
            |f| match EvalSpecification::validate_from_file(f, data_dir.as_path()) {
                Ok(evals) => {
                    println!("Eval '{}' is valid.", f.display());
                    evals
                }
                Err(err) => {
                    eprintln!("Error validating {f:?}: {err}");
                    exit(1);
                }
            },
        )
        .collect::<Vec<_>>();
    println!("{} evals found", output.len());

    let (evals, datasets): (Vec<Eval>, Vec<Dataset>) = output
        .iter()
        .filter_map(|e| match spice_components(e, data_dir.as_path()) {
            Ok((e, d)) => Some((e, d)),
            Err(err) => {
                println!(
                    "  Eval '{}' cannot be converted to spicepod component: {err}",
                    e.name
                );
                None
            }
        })
        .unzip();

    let pod = spicepod_definition(datasets, evals);

    serde_yaml::to_writer(std::fs::File::create(cli.spicepod_output.as_path())?, &pod)?;
    println!("Spicepod written to {}", cli.spicepod_output.display());
    Ok(())
}

fn yaml_files_from(dir: &Path) -> Result<Vec<PathBuf>> {
    Ok(std::fs::read_dir(dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();

            if path
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml")
            {
                Some(path)
            } else {
                None
            }
        })
        .collect::<Vec<PathBuf>>())
}

fn spicepod_definition(datasets: Vec<Dataset>, evals: Vec<Eval>) -> SpicepodDefinition {
    SpicepodDefinition {
        version: SpicepodVersion::V1,
        kind: SpicepodKind::Spicepod,
        name: "spicepod".to_string(),
        datasets: datasets
            .into_iter()
            .map(ComponentOrReference::Component)
            .collect(),
        evals: evals
            .into_iter()
            .map(ComponentOrReference::Component)
            .collect(),
        runtime: spicepod::component::runtime::Runtime::default(),
        extensions: HashMap::default(),
        secrets: Vec::default(),
        metadata: HashMap::default(),
        catalogs: Vec::default(),
        views: Vec::default(),
        models: Vec::default(),
        tools: Vec::default(),
        embeddings: Vec::default(),
        dependencies: Vec::default(),
    }
}
