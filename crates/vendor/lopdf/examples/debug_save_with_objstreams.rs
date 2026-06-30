use lopdf::{Document, Object, SaveOptions, dictionary};
use std::io::Write;

fn main() {
    println!("Debugging save_with_object_streams...\n");

    // Create a simple document
    let mut doc = Document::with_version("1.5");

    // Add some simple objects that can be compressed
    let obj1_id = doc.add_object(Object::Integer(42));
    let obj2_id = doc.add_object(Object::String(b"Hello".to_vec(), lopdf::StringFormat::Literal));
    let obj3_id = doc.add_object(dictionary! {
        "Type" => "TestObject",
        "Value" => 123
    });

    println!("Created objects:");
    println!("  {} 0 R: Integer", obj1_id.0);
    println!("  {} 0 R: String", obj2_id.0);
    println!("  {} 0 R: Dictionary", obj3_id.0);

    // Add required catalog structure
    let pages_id = doc.add_object(dictionary! {
        "Type" => "Pages",
        "Kids" => vec![],
        "Count" => 0
    });

    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id
    });

    doc.trailer.set("Root", catalog_id);

    println!("\nSaving with object streams...");

    // Save with object streams
    let options = SaveOptions {
        use_object_streams: true,
        ..Default::default()
    };

    let mut buffer = Vec::new();
    doc.save_with_options(&mut buffer, options).unwrap();

    println!("Saved {} bytes", buffer.len());

    // Write to file for inspection
    let mut file = std::fs::File::create("debug_objstream_save.pdf").unwrap();
    file.write_all(&buffer).unwrap();

    // Load it back and check
    println!("\nLoading back and analyzing...");
    let loaded = Document::load_mem(&buffer).unwrap();

    println!("Loaded {} objects", loaded.objects.len());

    // Find object streams
    for (id, obj) in &loaded.objects {
        if let Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            println!("\nFound object stream {} 0 R:", id.0);
            println!("  Dictionary: {:?}", stream.dict);
            println!("  Has Filter: {}", stream.dict.get(b"Filter").is_ok());
            println!("  Content size: {} bytes", stream.content.len());

            // Check if it looks compressed
            let looks_compressed = stream.content.iter().take(10).any(|&b| !(32..=127).contains(&b));
            println!("  Content looks compressed: {}", looks_compressed);

            if !looks_compressed {
                println!(
                    "  First 50 bytes: {:?}",
                    &stream.content[..stream.content.len().min(50)]
                );
            }
        }
    }

    println!("\nDone! Check debug_objstream_save.pdf");
}
