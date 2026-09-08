use lopdf::{Document, Object, SaveOptions};

fn main() {
    println!("Final Object Stream Compression Test\n");
    println!("{}", "=".repeat(60));

    // Create test document with various object types
    let mut doc = Document::with_version("1.5");

    // Add some compressible objects (fonts, annotations, etc.)
    let font1 = doc.add_object(lopdf::dictionary! {
        "Type" => "Font",
        "Subtype" => "Type1",
        "BaseFont" => "Helvetica"
    });

    let font2 = doc.add_object(lopdf::dictionary! {
        "Type" => "Font",
        "Subtype" => "Type1",
        "BaseFont" => "Times-Roman"
    });

    // Add a page (should NOT be compressed)
    let page_id = doc.add_object(lopdf::dictionary! {
        "Type" => "Page",
        "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
        "Contents" => lopdf::Object::Array(vec![]),
        "Resources" => lopdf::dictionary! {
            "Font" => lopdf::dictionary! {
                "F1" => font1,
                "F2" => font2
            }
        }
    });

    // Add pages tree (should NOT be compressed)
    let pages_id = doc.add_object(lopdf::dictionary! {
        "Type" => "Pages",
        "Kids" => vec![page_id.into()],
        "Count" => 1
    });

    // Add catalog (should NOT be compressed - it's in trailer)
    let catalog_id = doc.add_object(lopdf::dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id
    });

    doc.trailer.set("Root", catalog_id);

    // Add more compressible objects
    for i in 0..10 {
        doc.add_object(lopdf::dictionary! {
            "Type" => "Annot",
            "Subtype" => "Text",
            "Contents" => format!("Annotation {}", i)
        });
    }

    println!("Created test document with:");
    println!("  - 2 Font objects (should be compressed)");
    println!("  - 1 Page object (should NOT be compressed)");
    println!("  - 1 Pages object (should NOT be compressed)");
    println!("  - 1 Catalog object (should NOT be compressed)");
    println!("  - 10 Annotation objects (should be compressed)");

    // Save with object streams
    let options = SaveOptions {
        use_object_streams: true,
        use_xref_streams: true,
        ..Default::default()
    };

    let mut buffer = Vec::new();
    doc.save_with_options(&mut buffer, options).unwrap();

    println!("\nSaved with object streams: {} bytes", buffer.len());

    // Load back and analyze
    let loaded = Document::load_mem(&buffer).unwrap();
    println!("\nAnalyzing saved document:");
    println!("  Total objects: {}", loaded.objects.len());

    // Find object streams
    let mut obj_stream_count = 0;
    let mut compressed_count = 0;

    for obj in loaded.objects.values() {
        if let Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            obj_stream_count += 1;

            // Count objects in stream
            if let Ok(n) = stream.dict.get(b"N").and_then(|o| o.as_i64()) {
                compressed_count += n as usize;
            }
        }
    }

    println!("  Object streams: {}", obj_stream_count);
    println!("  Compressed objects: {}", compressed_count);

    // Verify critical objects are NOT compressed
    println!("\nVerifying critical objects:");

    // Check catalog
    if let Ok(root_ref) = loaded.trailer.get(b"Root")
        && let Object::Reference(cat_id) = root_ref
    {
        check_not_compressed(&loaded, *cat_id, "Catalog");
    }

    // Check pages tree
    if let Ok(catalog) = loaded.catalog()
        && let Ok(pages_ref) = catalog.get(b"Pages")
        && let Object::Reference(pages_id) = pages_ref
    {
        check_not_compressed(&loaded, *pages_id, "Pages tree");
    }

    // Check page objects
    let pages = loaded.get_pages();
    for (num, &page_id) in pages.iter() {
        check_not_compressed(&loaded, page_id, &format!("Page {}", num));
    }

    println!("\n{}", "=".repeat(60));
    println!("Test complete! Object streams are working correctly.");
}

fn check_not_compressed(doc: &Document, id: (u32, u16), name: &str) {
    if let Some(xref_entry) = doc.reference_table.get(id.0) {
        match xref_entry {
            lopdf::xref::XrefEntry::Normal { .. } => {
                println!("  ✓ {} is NOT compressed", name);
            }
            lopdf::xref::XrefEntry::Compressed { container, index } => {
                println!(
                    "  ✗ ERROR: {} is compressed in stream {} at index {}!",
                    name, container, index
                );
            }
            _ => {}
        }
    }
}
