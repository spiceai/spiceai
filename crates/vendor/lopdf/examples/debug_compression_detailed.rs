use lopdf::{Document, SaveOptions};

#[cfg(feature = "async")]
use tokio::runtime::Builder;

#[cfg(not(feature = "async"))]
fn load_document(path: &str) -> Result<Document, lopdf::Error> {
    Document::load(path)
}

#[cfg(feature = "async")]
fn load_document(path: &str) -> Result<Document, lopdf::Error> {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { Document::load(path).await })
}

fn main() {
    println!("Debugging object stream compression in detail...\n");

    // Load the user's PDF
    let pdf_path = "/Users/nicolasdao/Downloads/pdfs/RFQ - SDS WebApp.docx.pdf";
    println!("Loading PDF: {}", pdf_path);

    let mut doc = match load_document(pdf_path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Failed to load PDF: {}", e);
            return;
        }
    };

    println!("Loaded {} objects", doc.objects.len());

    // Count how many objects can be compressed
    let mut compressible_count = 0;
    for (id, obj) in &doc.objects {
        if id.1 == 0 && lopdf::ObjectStream::can_be_compressed(*id, obj, &doc) {
            compressible_count += 1;
        }
    }
    println!("Compressible objects: {}", compressible_count);

    // Save with object streams
    println!("\nSaving with object streams (compression level 6)...");
    let options = SaveOptions {
        use_object_streams: true,
        use_xref_streams: true, // MUST use xref streams with object streams!
        object_stream_config: lopdf::ObjectStreamConfig {
            max_objects_per_stream: 100,
            compression_level: 6,
        },
        ..Default::default()
    };

    let mut buffer = Vec::new();
    doc.save_with_options(&mut buffer, options).unwrap();

    println!("Saved {} bytes", buffer.len());

    // Write to file for inspection
    std::fs::write("debug_compression.pdf", &buffer).unwrap();

    // Load back and check
    println!("\nLoading back and checking object streams...");
    let loaded = Document::load_mem(&buffer).unwrap();

    let mut obj_stream_count = 0;
    let mut obj_stream_with_filter = 0;

    for (id, obj) in &loaded.objects {
        if let lopdf::Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            obj_stream_count += 1;
            let has_filter = stream.dict.get(b"Filter").is_ok();
            if has_filter {
                obj_stream_with_filter += 1;
            }

            println!("\nObject Stream {} 0 R:", id.0);
            println!("  Has Filter: {}", has_filter);
            if let Ok(n) = stream.dict.get(b"N").and_then(|o| o.as_i64()) {
                println!("  Contains {} objects", n);
            }
            if let Ok(first) = stream.dict.get(b"First").and_then(|o| o.as_i64()) {
                println!("  First offset: {}", first);
            }
            println!("  Content size: {} bytes", stream.content.len());

            // Check if content looks compressed
            let looks_compressed = stream.content.iter().take(20).any(|&b| b > 127);
            println!("  Content looks compressed: {}", looks_compressed);
        }
    }

    println!("\nSummary:");
    println!("  Total object streams: {}", obj_stream_count);
    println!("  Object streams with Filter: {}", obj_stream_with_filter);

    if obj_stream_with_filter < obj_stream_count {
        println!(
            "\nWARNING: {} object streams are missing Filter!",
            obj_stream_count - obj_stream_with_filter
        );
    }
}
