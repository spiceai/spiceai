use lopdf::{Document, Object, ObjectStream, SaveOptions, dictionary};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Verifying trailer-referenced object compression fix...\n");

    // Create a simple PDF with catalog and info
    let mut doc = Document::with_version("1.5");
    let pages_id = doc.new_object_id();

    let page_id = doc.add_object(dictionary! {
        "Type" => "Page",
        "Parent" => pages_id,
        "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
    });

    doc.objects.insert(
        pages_id,
        Object::Dictionary(dictionary! {
            "Type" => "Pages",
            "Kids" => vec![page_id.into()],
            "Count" => 1,
        }),
    );

    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id,
    });

    let info_id = doc.add_object(dictionary! {
        "Title" => "Test PDF",
        "Author" => "lopdf",
        "Creator" => "lopdf test",
        "Producer" => "lopdf",
        "CreationDate" => "D:20250807120000Z",
    });

    // Set in trailer
    doc.trailer.set("Root", catalog_id);
    doc.trailer.set("Info", info_id);

    // Test compression eligibility
    println!("Testing compression eligibility after trailer references:");

    let catalog_obj = doc.objects.get(&catalog_id).unwrap();
    let can_compress_catalog = ObjectStream::can_be_compressed(catalog_id, catalog_obj, &doc);
    println!("  Catalog (Root): can_be_compressed = {}", can_compress_catalog);

    let info_obj = doc.objects.get(&info_id).unwrap();
    let can_compress_info = ObjectStream::can_be_compressed(info_id, info_obj, &doc);
    println!("  Info dictionary: can_be_compressed = {}", can_compress_info);

    // Save with and without object streams
    let mut normal_output = Vec::new();
    doc.save_to(&mut normal_output)?;

    let options = SaveOptions::builder()
        .use_object_streams(true)
        .use_xref_streams(true)
        .build();

    let mut compressed_output = Vec::new();
    doc.save_with_options(&mut compressed_output, options)?;

    println!("\nFile sizes:");
    println!("  Without object streams: {} bytes", normal_output.len());
    println!("  With object streams: {} bytes", compressed_output.len());

    let reduction_pct = (1.0 - (compressed_output.len() as f64 / normal_output.len() as f64)) * 100.0;
    println!("  Size reduction: {:.1}%", reduction_pct);

    // Check if catalog is in object stream
    let content = String::from_utf8_lossy(&compressed_output);
    let has_objstm = content.contains("/ObjStm");
    let catalog_as_individual = content.contains(&format!("{} 0 obj\n<</Type/Catalog", catalog_id.0));
    let info_as_individual = content.contains(&format!("{} 0 obj\n<</Title", info_id.0));

    println!("\nCompression results:");
    println!("  Has object streams: {}", has_objstm);
    println!("  Catalog as individual object: {}", catalog_as_individual);
    println!("  Info as individual object: {}", info_as_individual);

    if can_compress_catalog && can_compress_info && has_objstm && !catalog_as_individual && !info_as_individual {
        println!("\n✅ SUCCESS: Trailer-referenced objects are properly compressed!");
    } else {
        println!("\n❌ FAILURE: Fix not working correctly");
    }

    Ok(())
}
