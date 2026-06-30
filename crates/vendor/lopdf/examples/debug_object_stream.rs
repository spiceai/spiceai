use lopdf::{Object, ObjectStream};

fn main() {
    println!("Testing Object Stream creation...\n");

    // Create a simple object stream with a few objects
    let mut obj_stream = ObjectStream::builder().max_objects(10).compression_level(6).build();

    // Add some simple objects
    obj_stream.add_object((1, 0), Object::Integer(42)).unwrap();
    obj_stream
        .add_object((2, 0), Object::String(b"Hello".to_vec(), lopdf::StringFormat::Literal))
        .unwrap();
    obj_stream.add_object((3, 0), Object::Boolean(true)).unwrap();

    println!("Added {} objects to stream", obj_stream.object_count());

    // Build the stream content
    match obj_stream.build_stream_content() {
        Ok(content) => {
            println!("\nStream content built successfully:");
            println!("Content size: {} bytes", content.len());

            // Show the content
            if let Ok(content_str) = std::str::from_utf8(&content) {
                println!("\nContent (first 200 chars):");
                println!("{}", &content_str[..content_str.len().min(200)]);
            } else {
                println!(
                    "\nRaw content (first 100 bytes): {:?}",
                    &content[..content.len().min(100)]
                );
            }

            // Try to create the stream object
            match obj_stream.to_stream_object() {
                Ok(stream) => {
                    println!("\n\nStream object created successfully!");
                    println!("Dictionary: {:?}", stream.dict);
                    println!("Compressed: {}", stream.dict.get(b"Filter").is_ok());
                    println!("Content length: {}", stream.content.len());

                    // Try to decompress and verify
                    match stream.decompressed_content() {
                        Ok(decompressed) => {
                            println!("\nDecompressed successfully!");
                            println!("Decompressed size: {} bytes", decompressed.len());
                            if let Ok(decomp_str) = std::str::from_utf8(&decompressed) {
                                println!("Decompressed content:\n{}", decomp_str);
                            }
                        }
                        Err(e) => {
                            println!("\nERROR: Failed to decompress: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("\nERROR: Failed to create stream object: {}", e);
                }
            }
        }
        Err(e) => {
            println!("ERROR: Failed to build stream content: {}", e);
        }
    }
}
