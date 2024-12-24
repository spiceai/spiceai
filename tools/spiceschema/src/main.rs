use clap::{ArgGroup, CommandFactory, Parser, Subcommand, ValueEnum};
use runtime::ApiDoc;
use serde_json::Value;
#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
    Json,
    Yaml,
}
use utoipa::OpenApi;

#[derive(Parser, Debug)]
#[command(
    name = "spiceschema",
    version = "0.1.0",
    about = "A CLI tool to generate API schemas in JSON or YAML.",
    long_about = None
)]
struct Cli {
    #[command(subcommand)]
    pub cmd: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Generate a schema for HTTP endpoints
    #[command(group(
        ArgGroup::new("format_group")
            .required(false)
            .args(["format", "json", "yaml"])
    ))]
    Http {
        /// `--format` takes a `ValueEnum`: json, yaml
        #[arg(
            long,
            value_enum,
            help = "Output format (json|yaml)",
            conflicts_with_all = ["json", "yaml"]
        )]
        format: Option<OutputFormat>,

        /// `--json` is an alias to format=json
        #[arg(long, help = "Output in JSON format", conflicts_with_all = ["format", "yaml"])]
        json: bool,

        /// `--yaml` is an alias to format=yaml
        #[arg(long, help = "Output in YAML format", conflicts_with_all = ["format", "json"])]
        yaml: bool,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.cmd {
        Some(Command::Http { format, json, yaml }) => {
            let Ok(json_str) = ApiDoc::openapi().to_json() else {
                return Err(Box::from("Failed to generate OpenAPI schema"));
            };

            let parsed: Value = serde_json::from_str(json_str.as_str())?;

            // 2) Determine final output format (defaults to JSON if none specified)
            let final_format = match (format, json, yaml) {
                // If user typed `--format=xyz`, use that
                (Some(f), _, _) => f,
                // If user typed `--yaml` directly
                (None, _, true) => &OutputFormat::Yaml,
                // Default to JSON
                _ => &OutputFormat::Json,
            };

            // 3) Re-serialize in the chosen format
            match final_format {
                OutputFormat::Json => {
                    // Print JSON pretty-printed
                    println!("{}", serde_json::to_string_pretty(&parsed)?);
                }
                OutputFormat::Yaml => {
                    println!("{}", serde_yaml::to_string(&parsed)?);
                }
            }
        }
        None => {
            // If no subcommand is given, print the help text
            Cli::command().print_help()?;
        }
    }

    Ok(())
}
