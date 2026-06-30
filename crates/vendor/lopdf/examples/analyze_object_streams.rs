use lopdf::{Document, Object};
use std::collections::HashMap;
use std::env;

#[cfg(feature = "async")]
use tokio::runtime::Builder;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <pdf_file>", args[0]);
        std::process::exit(1);
    }

    let pdf_path = &args[1];
    println!("Analyzing PDF: {}", pdf_path);
    println!("{}", "=".repeat(80));

    match load_document(pdf_path) {
        Ok(doc) => {
            analyze_document(&doc);
        }
        Err(e) => {
            eprintln!("Failed to load PDF: {}", e);
            std::process::exit(1);
        }
    }
}

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

fn analyze_document(doc: &Document) {
    println!("PDF Version: {:?}", doc.version);
    println!("Total objects: {}", doc.objects.len());

    // Find object streams
    let mut object_streams = Vec::new();
    let mut compressed_objects = HashMap::new();

    for (id, obj) in &doc.objects {
        if let Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            object_streams.push(id);

            // Get info about this object stream
            if let Ok(n) = stream.dict.get(b"N")
                && let Ok(count) = n.as_i64()
            {
                println!("\nObject Stream {} 0 R:", id.0);
                println!("  Contains {} objects", count);

                if let Ok(first) = stream.dict.get(b"First")
                    && let Ok(first_offset) = first.as_i64()
                {
                    println!("  First object at offset: {}", first_offset);
                }

                // Try to parse the stream content
                match stream.decompressed_content() {
                    Ok(decompressed) => {
                        println!("  Decompressed size: {} bytes", decompressed.len());

                        // Parse the offset table
                        if let Ok(first_offset) = stream.dict.get(b"First").and_then(|o| o.as_i64()) {
                            if first_offset as usize <= decompressed.len() {
                                let offset_table = &decompressed[..first_offset as usize];
                                if let Ok(offset_str) = std::str::from_utf8(offset_table) {
                                    let numbers: Vec<_> = offset_str.split_whitespace().collect();
                                    println!("  Objects in stream:");
                                    for chunk in numbers.chunks(2) {
                                        if chunk.len() == 2 {
                                            println!("    - Object {}: offset {}", chunk[0], chunk[1]);
                                            if let Ok(obj_num) = chunk[0].parse::<u32>() {
                                                compressed_objects.insert(obj_num, id.0);
                                            }
                                        }
                                    }
                                } else {
                                    println!("  ERROR: Could not parse offset table as UTF-8");
                                    println!("  First 100 bytes: {:?}", &offset_table[..offset_table.len().min(100)]);
                                }
                            } else {
                                println!(
                                    "  ERROR: First offset {} exceeds decompressed size {}",
                                    first_offset,
                                    decompressed.len()
                                );
                            }
                        }
                    }
                    Err(e) => {
                        println!("  ERROR: Could not decompress stream: {}", e);
                        println!("  Stream is compressed: {}", stream.allows_compression);
                        if let Ok(filter) = stream.dict.get(b"Filter") {
                            println!("  Filter: {:?}", filter);
                        } else {
                            println!("  No Filter found in dictionary!");
                            println!("  Dictionary: {:?}", stream.dict);
                        }
                        println!("  Raw content size: {} bytes", stream.content.len());
                        println!(
                            "  First 20 bytes of raw content: {:?}",
                            &stream.content[..stream.content.len().min(20)]
                        );
                    }
                }
            }
        }
    }

    println!("\nTotal object streams: {}", object_streams.len());
    println!("Total compressed objects: {}", compressed_objects.len());

    // Check pages
    println!("\n{}", "=".repeat(80));
    println!("Page Analysis:");

    let pages = doc.get_pages();
    println!("Total pages: {}", pages.len());

    for (page_num, &page_id) in pages.iter() {
        println!("\nPage {}:", page_num);
        match doc.get_object(page_id) {
            Ok(page_obj) => {
                if let Object::Dictionary(page_dict) = page_obj {
                    // Check if page is in object stream
                    if compressed_objects.contains_key(&page_id.0) {
                        println!(
                            "  ⚠️  Page object is compressed in object stream {}!",
                            compressed_objects[&page_id.0]
                        );
                    }

                    // Check page contents
                    if let Ok(contents) = page_dict.get(b"Contents") {
                        match contents {
                            Object::Reference(ref_id) => {
                                println!("  Contents: {} {} R", ref_id.0, ref_id.1);
                                if compressed_objects.contains_key(&ref_id.0) {
                                    println!(
                                        "  ⚠️  Contents is compressed in object stream {}!",
                                        compressed_objects[&ref_id.0]
                                    );
                                }
                            }
                            Object::Array(refs) => {
                                println!("  Contents array with {} elements", refs.len());
                                for (i, content_ref) in refs.iter().enumerate() {
                                    if let Object::Reference(ref_id) = content_ref
                                        && compressed_objects.contains_key(&ref_id.0)
                                    {
                                        println!("    ⚠️  Content[{}] {} {} R is compressed!", i, ref_id.0, ref_id.1);
                                    }
                                }
                            }
                            _ => println!("  Unexpected Contents type: {:?}", contents),
                        }
                    }

                    // Check resources
                    if let Ok(resources) = page_dict.get(b"Resources")
                        && let Object::Reference(ref_id) = resources
                        && compressed_objects.contains_key(&ref_id.0)
                    {
                        println!("  ⚠️  Resources {} {} R is compressed!", ref_id.0, ref_id.1);
                    }
                }
            }
            Err(e) => {
                println!("  Error getting page object: {}", e);
                if compressed_objects.contains_key(&page_id.0) {
                    println!("  Note: Page is in object stream {}", compressed_objects[&page_id.0]);
                }
            }
        }
    }

    // Check cross-reference
    println!("\n{}", "=".repeat(80));
    println!("Cross-reference Analysis:");
    println!("Cross-reference type: {:?}", doc.reference_table.cross_reference_type);

    // Check if any critical objects are compressed
    println!("\n{}", "=".repeat(80));
    println!("Critical Objects Check:");

    if let Ok(root) = doc.trailer.get(b"Root")
        && let Object::Reference(root_id) = root
    {
        if compressed_objects.contains_key(&root_id.0) {
            println!("⚠️  WARNING: Catalog (Root) object {} is compressed!", root_id.0);
        } else {
            println!("✓ Catalog (Root) object {} is not compressed", root_id.0);
        }
    }
}
