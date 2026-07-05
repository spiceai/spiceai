use lopdf::{Document, Object};
use std::env;

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
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <pdf_file>", args[0]);
        std::process::exit(1);
    }

    let pdf_path = &args[1];
    println!("Analyzing page contents in: {}", pdf_path);
    println!("{}", "=".repeat(80));

    match load_document(pdf_path) {
        Ok(doc) => {
            analyze_pages(&doc);
        }
        Err(e) => {
            eprintln!("Failed to load PDF: {}", e);
            std::process::exit(1);
        }
    }
}

fn analyze_pages(doc: &Document) {
    let pages = doc.get_pages();
    println!("Total pages: {}", pages.len());

    // Check if we have object streams
    let mut obj_streams = Vec::new();
    for (id, obj) in &doc.objects {
        if let Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            obj_streams.push(id.0);
        }
    }

    if !obj_streams.is_empty() {
        println!("\nObject streams found: {:?}", obj_streams);
    }

    println!("\nAnalyzing pages:");
    for (page_num, &page_id) in pages.iter() {
        println!("\n{}", "-".repeat(60));
        println!("Page {}: Object {} 0 R", page_num, page_id.0);

        match doc.get_object(page_id) {
            Ok(Object::Dictionary(page_dict)) => {
                // Check page type
                if let Ok(type_obj) = page_dict.get(b"Type") {
                    println!("  Type: {:?}", type_obj);
                }

                // Check Contents
                match page_dict.get(b"Contents") {
                    Ok(Object::Reference(content_id)) => {
                        println!("  Contents: {} {} R", content_id.0, content_id.1);
                        check_content_stream(doc, *content_id);
                    }
                    Ok(Object::Array(contents)) => {
                        println!("  Contents array with {} elements:", contents.len());
                        for (i, content_ref) in contents.iter().enumerate() {
                            if let Object::Reference(content_id) = content_ref {
                                println!("    [{}] {} {} R", i, content_id.0, content_id.1);
                                check_content_stream(doc, *content_id);
                            }
                        }
                    }
                    Ok(other) => {
                        println!("  Contents: Unexpected type: {:?}", other);
                    }
                    Err(e) => {
                        println!("  Contents: ERROR - {}", e);
                    }
                }

                // Check Resources
                match page_dict.get(b"Resources") {
                    Ok(Object::Reference(res_id)) => {
                        println!("  Resources: {} {} R", res_id.0, res_id.1);
                        check_object_location(doc, *res_id);
                    }
                    Ok(Object::Dictionary(_)) => {
                        println!("  Resources: Inline dictionary");
                    }
                    Ok(other) => {
                        println!("  Resources: Unexpected type: {:?}", other);
                    }
                    Err(e) => {
                        println!("  Resources: ERROR - {}", e);
                    }
                }
            }
            Ok(other) => {
                println!("  ERROR: Page is not a dictionary! Type: {:?}", other);
            }
            Err(e) => {
                println!("  ERROR: Cannot get page object: {}", e);
                check_object_location(doc, page_id);
            }
        }
    }
}

fn check_content_stream(doc: &Document, content_id: (u32, u16)) {
    match doc.get_object(content_id) {
        Ok(Object::Stream(stream)) => {
            println!("      ✓ Stream exists");
            println!("      Length: {}", stream.content.len());
            if let Ok(filter) = stream.dict.get(b"Filter") {
                println!("      Filter: {:?}", filter);
            }

            // Try to get decompressed content
            match stream.decompressed_content() {
                Ok(content) => {
                    let preview = String::from_utf8_lossy(&content[..content.len().min(100)]);
                    println!("      Preview: {:?}", preview);
                }
                Err(e) => {
                    println!("      ERROR decompressing: {}", e);
                }
            }
        }
        Ok(other) => {
            println!("      ERROR: Content is not a stream! Type: {:?}", other);
        }
        Err(e) => {
            println!("      ERROR: Cannot get content stream: {}", e);
            check_object_location(doc, content_id);
        }
    }
}

fn check_object_location(doc: &Document, obj_id: (u32, u16)) {
    // Check if object exists
    if doc.objects.contains_key(&obj_id) {
        println!("      Note: Object exists in document");
    } else {
        println!(
            "      WARNING: Object {} {} R not found in document!",
            obj_id.0, obj_id.1
        );

        // Check xref
        if let Some(xref_entry) = doc.reference_table.get(obj_id.0) {
            println!("      Xref entry: {:?}", xref_entry);
        } else {
            println!("      No xref entry found!");
        }
    }
}
