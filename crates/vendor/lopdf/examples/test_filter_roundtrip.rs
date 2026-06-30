use lopdf::{Document, Object, Stream, dictionary};

fn main() {
    println!("Testing Filter round-trip...\n");

    // Create a document with a compressed stream
    let mut doc = Document::with_version("1.5");

    // Create a stream with compressed content
    let content = b"This is test content that should be compressed";
    let mut stream = Stream::new(
        dictionary! {
            "Type" => "TestStream"
        },
        content.to_vec(),
    );

    // Compress it
    stream.compress().unwrap();

    println!("Created stream:");
    println!("  Dictionary: {:?}", stream.dict);
    println!("  Has Filter: {}", stream.dict.get(b"Filter").is_ok());
    println!("  Content size: {} bytes", stream.content.len());

    // Add to document
    let stream_id = doc.add_object(stream);

    // Add minimal structure
    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "TestStream" => stream_id
    });
    doc.trailer.set("Root", catalog_id);

    // Save to bytes
    println!("\nSaving...");
    let mut buffer = Vec::new();
    doc.save_to(&mut buffer).unwrap();

    // Load back
    println!("\nLoading back...");
    let loaded = Document::load_mem(&buffer).unwrap();

    // Find the stream
    if let Ok(Object::Stream(stream)) = loaded.get_object(stream_id) {
        println!("\nLoaded stream:");
        println!("  Dictionary: {:?}", stream.dict);
        println!("  Has Filter: {}", stream.dict.get(b"Filter").is_ok());
        println!("  Content size: {} bytes", stream.content.len());

        // Try to get decompressed content
        match stream.decompressed_content() {
            Ok(decompressed) => {
                println!("  Decompressed successfully!");
                println!("  Decompressed content: {:?}", String::from_utf8_lossy(&decompressed));
            }
            Err(e) => {
                println!("  Failed to decompress: {}", e);
            }
        }
    } else {
        println!("ERROR: Could not find stream!");
    }
}
