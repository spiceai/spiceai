use clap::{Parser, Subcommand};
use lopdf::{Document, Result};
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Extract text from PDF
    Extract {
        /// Input PDF file
        input: PathBuf,
        /// Page numbers to extract (comma-separated, e.g., "1,2,3")
        #[arg(short, long)]
        pages: Option<String>,
    },
    /// Replace text in PDF (exact match)
    Replace {
        /// Input PDF file
        input: PathBuf,
        /// Output PDF file
        output: PathBuf,
        /// Page number to replace text on
        #[arg(short, long)]
        page: u32,
        /// Text to search for (exact match required)
        #[arg(short, long)]
        search: String,
        /// Text to replace with
        #[arg(short, long)]
        replace: String,
        /// Default character for encoding issues
        #[arg(short, long)]
        default_char: Option<String>,
    },
    /// Replace partial text in PDF
    ReplacePartial {
        /// Input PDF file
        input: PathBuf,
        /// Output PDF file
        output: PathBuf,
        /// Page number to replace text on (0 for all pages)
        #[arg(short, long)]
        page: u32,
        /// Text to search for (partial match)
        #[arg(short, long)]
        search: String,
        /// Text to replace with
        #[arg(short, long)]
        replace: String,
        /// Default character for encoding issues
        #[arg(short, long)]
        default_char: Option<String>,
    },
    /// Get PDF information
    Info {
        /// Input PDF file
        input: PathBuf,
    },
    /// Compress PDF streams
    Compress {
        /// Input PDF file
        input: PathBuf,
        /// Output PDF file
        output: PathBuf,
    },
    /// Decompress PDF streams
    Decompress {
        /// Input PDF file
        input: PathBuf,
        /// Output PDF file
        output: PathBuf,
    },
    /// Delete pages from PDF
    Delete {
        /// Input PDF file
        input: PathBuf,
        /// Output PDF file
        output: PathBuf,
        /// Page numbers to delete (comma-separated, e.g., "1,3,5")
        #[arg(short, long)]
        pages: String,
    },
    /// Prune unused objects from PDF
    Prune {
        /// Input PDF file
        input: PathBuf,
        /// Output PDF file
        output: PathBuf,
    },
    /// Renumber PDF objects
    Renumber {
        /// Input PDF file
        input: PathBuf,
        /// Output PDF file
        output: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Extract { input, pages } => {
            let doc = Document::load(&input)?;
            let page_numbers = if let Some(pages) = pages {
                pages
                    .split(',')
                    .filter_map(|s| s.trim().parse::<u32>().ok())
                    .collect::<Vec<_>>()
            } else {
                doc.get_pages().keys().cloned().collect::<Vec<_>>()
            };

            let text = doc.extract_text(&page_numbers)?;
            println!("{}", text);
        }
        Commands::Replace {
            input,
            output,
            page,
            search,
            replace,
            default_char,
        } => {
            let mut doc = Document::load(&input)?;
            doc.replace_text(page, &search, &replace, default_char.as_deref())?;
            doc.save(&output)?;
            println!("Text replaced successfully. Saved to: {:?}", output);
        }
        Commands::ReplacePartial {
            input,
            output,
            page,
            search,
            replace,
            default_char,
        } => {
            let mut doc = Document::load(&input)?;
            let mut total_replacements = 0;

            if page == 0 {
                // Replace on all pages
                let pages = doc.get_pages();
                for page_num in pages.keys() {
                    match doc.replace_partial_text(*page_num, &search, &replace, default_char.as_deref()) {
                        Ok(count) => {
                            if count > 0 {
                                println!("Page {}: Replaced {} occurrences", page_num, count);
                                total_replacements += count;
                            }
                        }
                        Err(e) => eprintln!("Error on page {}: {}", page_num, e),
                    }
                }
            } else {
                // Replace on specific page
                match doc.replace_partial_text(page, &search, &replace, default_char.as_deref()) {
                    Ok(count) => {
                        println!("Page {}: Replaced {} occurrences", page, count);
                        total_replacements = count;
                    }
                    Err(e) => return Err(e),
                }
            }

            if total_replacements > 0 {
                doc.save(&output)?;
                println!("Total replacements: {}. Saved to: {:?}", total_replacements, output);
            } else {
                println!("No replacements made. File not saved.");
            }
        }
        Commands::Info { input } => {
            let doc = Document::load(&input)?;
            println!("PDF Information for: {:?}", input);
            println!("Version: {}", doc.version);
            println!("Pages: {}", doc.get_pages().len());
            println!("Objects: {}", doc.objects.len());
            println!("Max Object ID: {}", doc.max_id);

            if let Ok(info) = doc.trailer.get(b"Info").and_then(|id| {
                if let Ok(id) = id.as_reference() {
                    doc.get_dictionary(id)
                } else {
                    Err(lopdf::Error::ObjectNotFound((0, 0)))
                }
            }) {
                println!("\nDocument Info:");
                for (key, value) in info.iter() {
                    let key_str = String::from_utf8_lossy(key);
                    println!("  {}: {:?}", key_str, value);
                }
            }
        }
        Commands::Compress { input, output } => {
            let mut doc = Document::load(&input)?;
            doc.compress();
            doc.save(&output)?;
            println!("PDF compressed. Saved to: {:?}", output);
        }
        Commands::Decompress { input, output } => {
            let mut doc = Document::load(&input)?;
            doc.decompress();
            doc.save(&output)?;
            println!("PDF decompressed. Saved to: {:?}", output);
        }
        Commands::Delete { input, output, pages } => {
            let mut doc = Document::load(&input)?;
            let page_numbers: Vec<u32> = pages.split(',').filter_map(|s| s.trim().parse::<u32>().ok()).collect();

            doc.delete_pages(&page_numbers);
            doc.save(&output)?;
            println!("Deleted {} pages. Saved to: {:?}", page_numbers.len(), output);
        }
        Commands::Prune { input, output } => {
            let mut doc = Document::load(&input)?;
            let pruned = doc.prune_objects();
            doc.save(&output)?;
            println!("Pruned {} unused objects. Saved to: {:?}", pruned.len(), output);
        }
        Commands::Renumber { input, output } => {
            let mut doc = Document::load(&input)?;
            doc.renumber_objects();
            doc.save(&output)?;
            println!("Objects renumbered. Saved to: {:?}", output);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lopdf::{
        Object, Stream,
        content::{Content, Operation},
        dictionary,
    };

    #[test]
    fn test_replace_partial_command() -> Result<()> {
        // Create a test PDF
        let mut doc = Document::with_version("1.5");

        let pages_id = doc.new_object_id();
        let font_id = doc.add_object(dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => "Helvetica",
        });

        let resources_id = doc.add_object(dictionary! {
            "Font" => dictionary! {
                "F1" => font_id,
            },
        });

        let content = Content {
            operations: vec![
                Operation::new("BT", vec![]),
                Operation::new("Tf", vec!["F1".into(), 12.into()]),
                Operation::new("Td", vec![100.into(), 700.into()]),
                Operation::new("Tj", vec![Object::string_literal("Hello World! Hello Universe!")]),
                Operation::new("ET", vec![]),
            ],
        };

        let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode()?));

        let page_id = doc.add_object(dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "Contents" => content_id,
            "Resources" => resources_id,
        });

        doc.objects.insert(
            pages_id,
            Object::Dictionary(dictionary! {
                "Type" => "Pages",
                "Kids" => vec![page_id.into()],
                "Count" => 1,
                "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
            }),
        );

        let catalog_id = doc.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });

        doc.trailer.set("Root", catalog_id);

        // Save test PDF
        doc.save("test_input.pdf")?;

        // Test the utility would work with this PDF
        let mut doc = Document::load("test_input.pdf")?;
        let count = doc.replace_partial_text(1, "Hello", "Hi", None)?;
        assert_eq!(count, 2);

        // Clean up
        std::fs::remove_file("test_input.pdf").ok();

        Ok(())
    }
}
