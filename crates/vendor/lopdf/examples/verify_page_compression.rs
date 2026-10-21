use lopdf::{Document, Object};

#[cfg(feature = "async")]
use tokio::runtime::Builder;

#[cfg(not(feature = "async"))]
fn load_document(path: &str) -> Result<Document, Box<dyn std::error::Error>> {
    Ok(Document::load(path)?)
}

#[cfg(feature = "async")]
fn load_document(path: &str) -> Result<Document, Box<dyn std::error::Error>> {
    Ok(Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { Document::load(path).await })?)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let compressed_path = "/Users/nicolasdao/Downloads/pdfs/pdf-demo_compressed.pdf";

    println!("Checking if Page objects are in object streams...\n");

    // Load the compressed PDF
    let doc = load_document(compressed_path)?;

    // Check for object streams
    let mut objstm_count = 0;
    let mut total_compressed = 0;

    for (id, obj) in &doc.objects {
        if let Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            objstm_count += 1;

            // Get number of objects in this stream
            if let Ok(n) = stream.dict.get(b"N")
                && let Ok(n_val) = n.as_i64()
            {
                total_compressed += n_val;
                println!("Object stream {} 0 R contains {} objects", id.0, n_val);
            }
        }
    }

    println!("\nTotal object streams: {}", objstm_count);
    println!("Total compressed objects: {}", total_compressed);

    // Check if we can find Page objects as top-level objects
    let mut page_count = 0;
    let mut pages_count = 0;
    let mut catalog_count = 0;

    for obj in doc.objects.values() {
        if let Object::Dictionary(dict) = obj
            && let Ok(type_obj) = dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
        {
            match type_name {
                b"Page" => page_count += 1,
                b"Pages" => pages_count += 1,
                b"Catalog" => catalog_count += 1,
                _ => {}
            }
        }
    }

    println!("\nTop-level objects found:");
    println!("  Page objects: {}", page_count);
    println!("  Pages objects: {}", pages_count);
    println!("  Catalog objects: {}", catalog_count);

    if page_count == 0 && pages_count == 0 {
        println!("\n✓ SUCCESS: Page and Pages objects are compressed into object streams!");
    } else {
        println!("\n✗ WARNING: Some structural objects may not be compressed");
    }

    // Check the raw PDF content
    let content = std::fs::read_to_string(compressed_path)?;

    // Look for individual page definitions
    let page_defs = content.matches(" 0 obj\n<</Type/Page").count();
    println!("\nPage definitions found in raw PDF: {}", page_defs);

    if page_defs == 0 {
        println!("✓ No individual Page object definitions found - they're in object streams!");
    }

    Ok(())
}
