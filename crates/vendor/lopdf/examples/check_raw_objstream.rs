use lopdf::{Document, Object, SaveOptions};

#[cfg(feature = "async")]
use tokio::runtime::Builder;

#[cfg(not(feature = "async"))]
fn load_document(path: &str) -> Document {
    Document::load(path).unwrap()
}

#[cfg(feature = "async")]
fn load_document(path: &str) -> Document {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { Document::load(path).await.unwrap() })
}

fn main() {
    println!("Checking raw object stream in saved PDF...\n");

    // Create a simple document
    let mut doc = Document::with_version("1.5");

    // Add some objects that will be compressed
    for i in 1..=10 {
        doc.add_object(Object::Integer(i * 100));
    }

    // Add catalog
    let pages_id = doc.add_object(lopdf::dictionary! {
        "Type" => "Pages",
        "Kids" => vec![],
        "Count" => 0
    });

    let catalog_id = doc.add_object(lopdf::dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id
    });

    doc.trailer.set("Root", catalog_id);

    // Save with object streams
    let options = SaveOptions {
        use_object_streams: true,
        use_xref_streams: true,
        ..Default::default()
    };

    let filename = "test_raw_objstream.pdf";
    let mut buffer = Vec::new();
    doc.save_with_options(&mut buffer, options).unwrap();
    std::fs::write(filename, &buffer).unwrap();

    println!("Saved {} bytes to {}", buffer.len(), filename);

    // Now read the raw file and look for object streams
    println!("\nSearching for object streams in raw file...");

    let file_str = String::from_utf8_lossy(&buffer);

    // Find object streams by looking for "/Type/ObjStm"
    let mut pos = 0;
    while let Some(found) = file_str[pos..].find("/Type/ObjStm") {
        let abs_pos = pos + found;
        println!("\nFound /Type/ObjStm at position {}", abs_pos);

        // Find the start of this object (look backwards for "obj")
        if let Some(obj_start) = file_str[..abs_pos].rfind(" obj") {
            // Extract object number
            let obj_line_start = file_str[..obj_start].rfind('\n').unwrap_or(0) + 1;
            let obj_header = &file_str[obj_line_start..obj_start];
            println!("Object header: {}", obj_header.trim());

            // Look for dictionary end and stream content
            if let Some(dict_end) = file_str[abs_pos..].find(">>") {
                let dict_end_pos = abs_pos + dict_end + 2;
                let dict_content = &file_str[obj_start + 4..dict_end_pos];
                println!("Dictionary content: {}", dict_content.trim());

                // Check if there's a Filter
                if dict_content.contains("/Filter") {
                    println!("✓ Has Filter!");
                } else {
                    println!("✗ No Filter found!");
                }

                // Check stream content
                if let Some(stream_start) = file_str[dict_end_pos..].find("stream") {
                    let stream_pos = dict_end_pos + stream_start + 6;
                    if file_str.len() > stream_pos + 2 {
                        let next_char = file_str.as_bytes()[stream_pos + 1];
                        if next_char == b'\n' || next_char == b'\r' {
                            let stream_content_start = stream_pos + 2;
                            // Check first few bytes
                            let preview = &buffer[stream_content_start
                                ..stream_content_start.min(buffer.len()).min(stream_content_start + 20)];
                            println!("First 20 bytes of stream: {:?}", preview);

                            // Check if it looks compressed
                            let looks_compressed =
                                preview.iter().any(|&b| b > 127 || (b < 32 && b != b'\n' && b != b'\r'));
                            println!("Looks compressed: {}", looks_compressed);
                        }
                    }
                }
            }
        }

        pos = abs_pos + 10;
    }

    // Now load it back and check
    println!("\n\nLoading PDF back...");
    let loaded = load_document(filename);

    let mut found_objstream = false;
    for (id, obj) in &loaded.objects {
        if let Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            found_objstream = true;
            println!("\nLoaded object stream {} 0 R:", id.0);
            println!("  Has Filter: {}", stream.dict.get(b"Filter").is_ok());
            println!("  Dictionary: {:?}", stream.dict);
        }
    }

    if !found_objstream {
        println!("No object streams found in loaded PDF!");
    }
}
