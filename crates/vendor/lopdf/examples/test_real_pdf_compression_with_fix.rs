use lopdf::{Document, SaveOptions};
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
    // Test with the PDF from the Downloads folder
    let pdf_path = "/Users/nicolasdao/Downloads/pdfs/pdf-demo.pdf";

    if !Path::new(pdf_path).exists() {
        eprintln!("Test PDF not found at: {}", pdf_path);
        return Ok(());
    }

    println!("Testing object stream compression with real PDF...");
    println!("Input: {}", pdf_path);

    // Load the PDF
    let mut doc = load_document(pdf_path)?;

    // Save without object streams
    let mut normal_output = Vec::new();
    doc.save_to(&mut normal_output)?;

    // Save with object streams
    let options = SaveOptions::builder()
        .use_object_streams(true)
        .use_xref_streams(true)
        .build();

    let mut compressed_output = Vec::new();
    doc.save_with_options(&mut compressed_output, options)?;

    // Calculate results
    let original_size = std::fs::metadata(pdf_path)?.len();
    let normal_size = normal_output.len();
    let compressed_size = compressed_output.len();

    println!("\nFile sizes:");
    println!("  Original file: {} bytes", original_size);
    println!("  After re-save (no compression): {} bytes", normal_size);
    println!("  With object streams: {} bytes", compressed_size);

    let reduction_from_original = (1.0 - (compressed_size as f64 / original_size as f64)) * 100.0;
    let reduction_from_resave = (1.0 - (compressed_size as f64 / normal_size as f64)) * 100.0;

    println!("\nSize reduction:");
    println!("  From original: {:.1}%", reduction_from_original);
    println!("  From re-saved: {:.1}%", reduction_from_resave);

    // Check compression details
    let content = String::from_utf8_lossy(&compressed_output);
    let objstm_count = content.matches("/ObjStm").count();

    println!("\nCompression details:");
    println!("  Object streams created: {}", objstm_count);

    // Count how many objects are in the PDF
    let total_objects = doc.objects.len();
    println!("  Total objects in PDF: {}", total_objects);

    // Save to file for manual inspection
    let output_path = "/Users/nicolasdao/Downloads/pdfs/pdf-demo_fixed_compression.pdf";
    std::fs::write(output_path, &compressed_output)?;
    println!("\nCompressed PDF saved to: {}", output_path);

    if reduction_from_original > 20.0 {
        println!("\n✅ SUCCESS: Achieved {:.1}% size reduction!", reduction_from_original);
    } else {
        println!(
            "\n⚠️  Size reduction is {:.1}% (expected 26-38%)",
            reduction_from_original
        );
    }

    Ok(())
}
