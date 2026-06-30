use lopdf::Document;
use std::env;

fn main() {
    let pdf_path = env::args().nth(1).unwrap_or_else(|| "assets/example.pdf".to_string());

    let buffer = match std::fs::read(&pdf_path) {
        Ok(buf) => buf,
        Err(e) => {
            eprintln!("Error reading file: {}", e);
            std::process::exit(1);
        }
    };

    match Document::load_metadata_mem(&buffer) {
        Ok(metadata) => {
            println!("PDF Version: {}", metadata.version);
            println!("Title: {:?}", metadata.title);
            println!("Author: {:?}", metadata.author);
            println!("Subject: {:?}", metadata.subject);
            println!("Keywords: {:?}", metadata.keywords);
            println!("Creator: {:?}", metadata.creator);
            println!("Producer: {:?}", metadata.producer);
            println!("Creation Date: {:?}", metadata.creation_date);
            println!("Modification Date: {:?}", metadata.modification_date);
            println!("Page Count: {}", metadata.page_count);
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}
