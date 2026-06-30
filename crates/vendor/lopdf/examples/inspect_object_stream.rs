use lopdf::{Document, Object, ObjectStream};

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
    let pdf_path = "/Users/nicolasdao/Downloads/pdfs/RFQ - SDS WebApp.docx_compressed.pdf";
    println!("Inspecting object stream in: {}", pdf_path);

    match load_document(pdf_path) {
        Ok(doc) => {
            // Find object stream 511
            if let Ok(Object::Stream(stream)) = doc.get_object((511, 0)) {
                println!("\nObject Stream 511 0 R found!");
                println!("Dictionary: {:?}", stream.dict);

                // Try to parse it
                let mut stream_clone = stream.clone();
                match ObjectStream::new(&mut stream_clone) {
                    Ok(obj_stream) => {
                        println!("\nSuccessfully parsed object stream!");
                        println!("Contains {} objects", obj_stream.objects.len());

                        // List first 20 objects
                        println!("\nFirst 20 objects in stream:");
                        for (count, ((id, generation), obj)) in obj_stream.objects.iter().enumerate() {
                            if count >= 20 {
                                break;
                            }
                            println!("  {} {} R: {:?}", id, generation, obj.type_name().unwrap_or(b"Unknown"));

                            // If it's a dictionary, show some keys
                            if let Object::Dictionary(dict) = obj {
                                let key_count = dict.len();
                                println!("    Dictionary with {} keys", key_count);
                            }
                        }

                        // Check if any page-related objects are in there
                        println!("\nChecking for page-related objects:");
                        for ((id, generation), obj) in &obj_stream.objects {
                            if let Object::Dictionary(dict) = obj {
                                if let Ok(type_obj) = dict.get(b"Type")
                                    && let Ok(type_name) = type_obj.as_name()
                                    && type_name == b"Page"
                                {
                                    println!("  WARNING: Page object {} {} R is in object stream!", id, generation);
                                }

                                // Check for font descriptors
                                if dict.has(b"FontDescriptor") || dict.has(b"BaseFont") {
                                    println!("  Font-related object {} {} R", id, generation);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("ERROR parsing object stream: {}", e);
                    }
                }
            } else {
                println!("Object stream 511 not found or not a stream!");
            }

            // Also check what critical objects might be compressed
            println!("\n\nChecking critical object locations:");

            // Check catalog
            if let Ok(root_ref) = doc.trailer.get(b"Root")
                && let Object::Reference(root_id) = root_ref
            {
                check_object_status(&doc, *root_id, "Catalog (Root)");
            }

            // Check pages tree
            if let Ok(catalog) = doc.catalog()
                && let Ok(pages_ref) = catalog.get(b"Pages")
                && let Object::Reference(pages_id) = pages_ref
            {
                check_object_status(&doc, *pages_id, "Pages tree root");
            }

            // Check specific page objects
            let pages = doc.get_pages();
            for (num, &id) in pages.iter().take(2) {
                check_object_status(&doc, id, &format!("Page {}", num));
            }
        }
        Err(e) => {
            eprintln!("Failed to load PDF: {}", e);
        }
    }
}

fn check_object_status(doc: &Document, id: (u32, u16), name: &str) {
    match doc.get_object(id) {
        Ok(_) => {
            // Check xref entry
            if let Some(xref_entry) = doc.reference_table.get(id.0) {
                match xref_entry {
                    lopdf::xref::XrefEntry::Normal { offset, generation: _ } => {
                        println!("{} ({} {} R): Normal entry at offset {}", name, id.0, id.1, offset);
                    }
                    lopdf::xref::XrefEntry::Compressed { container, index } => {
                        println!(
                            "{} ({} {} R): COMPRESSED in stream {} at index {}",
                            name, id.0, id.1, container, index
                        );
                    }
                    _ => {
                        println!("{} ({} {} R): Other xref type", name, id.0, id.1);
                    }
                }
            } else {
                println!("{} ({} {} R): No xref entry!", name, id.0, id.1);
            }
        }
        Err(e) => {
            println!("{} ({} {} R): ERROR - {}", name, id.0, id.1, e);
        }
    }
}
