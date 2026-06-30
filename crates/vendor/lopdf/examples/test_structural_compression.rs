use lopdf::{Document, Object, SaveOptions, dictionary};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing structural object compression...\n");

    // Create a simple PDF with structural objects
    let mut doc = Document::with_version("1.4");

    // Create catalog (should be compressed)
    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "Pages" => Object::Reference((2, 0))
    });

    // Create pages tree (should be compressed)
    let _pages_id = doc.add_object(dictionary! {
        "Type" => "Pages",
        "Kids" => vec![Object::Reference((3, 0))],
        "Count" => 1
    });

    // Create page (should be compressed)
    let _page_id = doc.add_object(dictionary! {
        "Type" => "Page",
        "Parent" => Object::Reference((2, 0)),
        "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
        "Resources" => dictionary! {}
    });

    // Add some font objects for good measure
    for i in 10..15 {
        doc.add_object(dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => format!("Font{}", i)
        });
    }

    // Set up trailer
    doc.trailer.set("Root", catalog_id);
    doc.max_id = 15;
    doc.renumber_objects();

    // Save without object streams
    println!("Saving without object streams...");
    let mut no_objstm = Vec::new();
    doc.save_to(&mut no_objstm)?;

    // Save with object streams
    println!("Saving with object streams...");
    let mut with_objstm = Vec::new();
    let options = SaveOptions::builder()
        .use_object_streams(true)
        .use_xref_streams(true)
        .max_objects_per_stream(100)
        .compression_level(6)
        .build();
    doc.save_with_options(&mut with_objstm, options)?;

    // Analyze results
    let no_objstm_str = String::from_utf8_lossy(&no_objstm);
    let with_objstm_str = String::from_utf8_lossy(&with_objstm);

    println!("\n=== Analysis ===");
    println!("Without object streams: {} bytes", no_objstm.len());
    println!("With object streams: {} bytes", with_objstm.len());
    println!(
        "Size reduction: {:.1}%",
        (1.0 - with_objstm.len() as f64 / no_objstm.len() as f64) * 100.0
    );

    // Check for object stream markers
    let objstm_count = with_objstm_str.matches("/ObjStm").count();
    println!("\nObject streams created: {}", objstm_count);

    // Check that individual objects are NOT present in compressed version
    println!("\n=== Checking for individual object definitions ===");

    // These patterns should NOT appear in the compressed version
    let patterns = vec![
        "1 0 obj",  // Catalog
        "2 0 obj",  // Pages
        "3 0 obj",  // Page
        "10 0 obj", // Font
        "11 0 obj", // Font
    ];

    for pattern in patterns {
        let in_original = no_objstm_str.contains(pattern);
        let in_compressed = with_objstm_str.contains(pattern);

        println!("{}: original={}, compressed={}", pattern, in_original, in_compressed);

        if in_compressed {
            println!("  WARNING: Object {} should have been compressed!", pattern);
        }
    }

    // Save to files for manual inspection
    std::fs::write("test_no_objstm.pdf", &no_objstm)?;
    std::fs::write("test_with_objstm.pdf", &with_objstm)?;

    println!("\nSaved test files:");
    println!("  - test_no_objstm.pdf");
    println!("  - test_with_objstm.pdf");

    // Load and verify the compressed PDF
    println!("\n=== Verifying compressed PDF ===");
    let compressed_doc = Document::load_mem(&with_objstm[..])?;

    let mut objstm_found = 0;
    let mut compressed_objects = 0;

    for obj in compressed_doc.objects.values() {
        if let Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            objstm_found += 1;

            // Count objects in this stream
            if let Ok(n) = stream.dict.get(b"N")
                && let Ok(n_val) = n.as_i64()
            {
                compressed_objects += n_val as usize;
                println!("Object stream found with {} objects", n_val);
            }
        }
    }

    println!("\nTotal object streams: {}", objstm_found);
    println!("Total compressed objects: {}", compressed_objects);

    if compressed_objects < 5 {
        println!("\nERROR: Expected at least 5 objects to be compressed (Catalog, Pages, Page, and Fonts)");
        println!("Only {} objects were compressed!", compressed_objects);
    } else {
        println!("\n✓ SUCCESS: Structural objects are being compressed!");
    }

    Ok(())
}
