use lopdf::{Document, SaveOptions};
use std::env;
use std::path::Path;

#[cfg(feature = "async")]
use tokio::runtime::Builder;

#[cfg(not(feature = "async"))]
fn load_document(path: &str) -> Result<Document, Box<dyn std::error::Error>> {
    Ok(Document::load(path)?)
}

#[cfg(feature = "async")]
fn load_document(path: &str) -> Result<Document, Box<dyn std::error::Error>> {
    Ok(Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { Document::load(path).await })?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get filename from command line or use default
    let args: Vec<String> = env::args().collect();
    let input_file = if args.len() > 1 { &args[1] } else { "assets/example.pdf" };

    // Check if file exists
    if !Path::new(input_file).exists() {
        eprintln!("Error: File '{}' not found", input_file);
        eprintln!("Usage: cargo run --example compress_existing_pdf [path/to/pdf]");
        return Ok(());
    }

    println!("Loading PDF: {}", input_file);

    // Load the PDF
    let mut doc = load_document(input_file)?;
    println!("PDF version: {}", doc.version);
    println!("Number of objects: {}", doc.objects.len());
    println!("Number of pages: {}", doc.get_pages().len());

    // Get original file size
    let original_size = std::fs::metadata(input_file)?.len();
    println!(
        "\nOriginal file size: {} bytes ({:.1} KB)",
        original_size,
        original_size as f64 / 1024.0
    );

    // Save without object streams (traditional format)
    let output_traditional = format!("{}_traditional.pdf", input_file.trim_end_matches(".pdf"));
    doc.save(&output_traditional)?;
    let traditional_size = std::fs::metadata(&output_traditional)?.len();
    println!(
        "\nTraditional save: {} bytes ({:.1} KB)",
        traditional_size,
        traditional_size as f64 / 1024.0
    );

    // Save with object streams (modern format)
    let output_modern = format!("{}_compressed.pdf", input_file.trim_end_matches(".pdf"));
    let mut modern_file = std::fs::File::create(&output_modern)?;
    doc.save_modern(&mut modern_file)?;
    drop(modern_file); // Ensure file is closed before reading metadata
    let modern_size = std::fs::metadata(&output_modern)?.len();
    println!(
        "Object streams save: {} bytes ({:.1} KB)",
        modern_size,
        modern_size as f64 / 1024.0
    );

    // Save with custom compression settings
    let output_custom = format!("{}_max_compressed.pdf", input_file.trim_end_matches(".pdf"));
    let mut custom_file = std::fs::File::create(&output_custom)?;
    let options = SaveOptions::builder()
        .use_object_streams(true)
        .use_xref_streams(true)
        .max_objects_per_stream(200) // More objects per stream
        .compression_level(9) // Maximum compression
        .build();
    doc.save_with_options(&mut custom_file, options)?;
    drop(custom_file);
    let custom_size = std::fs::metadata(&output_custom)?.len();
    println!(
        "Max compression save: {} bytes ({:.1} KB)",
        custom_size,
        custom_size as f64 / 1024.0
    );

    // Calculate savings
    println!("\n--- Compression Results ---");
    let modern_reduction = 100.0 - (modern_size as f64 / original_size as f64 * 100.0);
    let custom_reduction = 100.0 - (custom_size as f64 / original_size as f64 * 100.0);
    println!("Object streams reduction: {:.1}%", modern_reduction);
    println!("Max compression reduction: {:.1}%", custom_reduction);

    // Compare with traditional save
    let modern_vs_trad = 100.0 - (modern_size as f64 / traditional_size as f64 * 100.0);
    println!("\nVs traditional re-save: {:.1}% smaller", modern_vs_trad);

    println!("\nOutput files created:");
    println!("  - {}", output_traditional);
    println!("  - {}", output_modern);
    println!("  - {}", output_custom);

    Ok(())
}
