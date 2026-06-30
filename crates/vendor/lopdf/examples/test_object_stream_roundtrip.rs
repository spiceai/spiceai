use lopdf::{Document, SaveOptions};
use std::fs::File;
use std::io::Write;

fn main() {
    println!("Testing object stream round-trip...\n");

    // First, create a simple PDF with object streams
    let mut doc = Document::with_version("1.5");

    // Add a simple page
    let pages_id = doc.new_object_id();
    let page_id = doc.new_object_id();
    let content_id = doc.new_object_id();
    let font_id = doc.new_object_id();

    // Page content
    doc.objects.insert(
        content_id,
        lopdf::Stream::new(
            lopdf::dictionary! {},
            b"BT /F1 12 Tf 100 700 Td (Hello World) Tj ET".to_vec(),
        )
        .into(),
    );

    // Font
    doc.objects.insert(
        font_id,
        lopdf::dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => "Helvetica"
        }
        .into(),
    );

    // Page
    doc.objects.insert(
        page_id,
        lopdf::dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
            "Contents" => content_id,
            "Resources" => lopdf::dictionary! {
                "Font" => lopdf::dictionary! {
                    "F1" => font_id
                }
            }
        }
        .into(),
    );

    // Pages
    doc.objects.insert(
        pages_id,
        lopdf::dictionary! {
            "Type" => "Pages",
            "Kids" => vec![page_id.into()],
            "Count" => 1
        }
        .into(),
    );

    // Catalog
    let catalog_id = doc.add_object(lopdf::dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id
    });

    doc.trailer.set("Root", catalog_id);

    // Save with object streams
    println!("Saving original PDF with object streams...");
    let options = SaveOptions {
        use_object_streams: true,
        ..Default::default()
    };

    let mut original_bytes = Vec::new();
    doc.save_with_options(&mut original_bytes, options.clone()).unwrap();
    println!("Original size: {} bytes", original_bytes.len());

    // Write to file for inspection
    let mut file = File::create("test_roundtrip_original.pdf").unwrap();
    file.write_all(&original_bytes).unwrap();

    // Now load it back
    println!("\nLoading PDF back...");
    let mut loaded_doc = Document::load_mem(&original_bytes).unwrap();
    println!("Loaded {} objects", loaded_doc.objects.len());

    // Check for object streams
    let mut obj_stream_count = 0;
    for (id, obj) in &loaded_doc.objects {
        if let lopdf::Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            obj_stream_count += 1;
            println!("Found object stream {} 0 R", id.0);
            if let Ok(filter) = stream.dict.get(b"Filter") {
                println!("  Has Filter: {:?}", filter);
            } else {
                println!("  WARNING: No Filter!");
            }
        }
    }
    println!("Total object streams found: {}", obj_stream_count);

    // Save again with object streams
    println!("\nSaving loaded PDF with object streams again...");
    let mut resaved_bytes = Vec::new();
    loaded_doc.save_with_options(&mut resaved_bytes, options).unwrap();
    println!("Resaved size: {} bytes", resaved_bytes.len());

    // Write to file for inspection
    let mut file = File::create("test_roundtrip_resaved.pdf").unwrap();
    file.write_all(&resaved_bytes).unwrap();

    println!("\nDone! Check test_roundtrip_original.pdf and test_roundtrip_resaved.pdf");
}
