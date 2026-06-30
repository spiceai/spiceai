use lopdf::{Document, Object};
use std::collections::HashMap;
use std::env;

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
    let args: Vec<String> = env::args().collect();
    let input_file = if args.len() > 1 { &args[1] } else { "assets/example.pdf" };

    println!("Analyzing PDF: {}", input_file);
    let doc = load_document(input_file)?;

    // Count object types
    let mut type_counts: HashMap<String, usize> = HashMap::new();
    let mut stream_count = 0;
    let mut compressible_count = 0;

    for ((_id, _gen), obj) in &doc.objects {
        let type_name = match obj {
            Object::Null => "Null",
            Object::Boolean(_) => "Boolean",
            Object::Integer(_) => "Integer",
            Object::Real(_) => "Real",
            Object::Name(_) => "Name",
            Object::String(_, _) => "String",
            Object::Array(_) => "Array",
            Object::Dictionary(dict) => {
                // Try to get the Type entry to be more specific
                if let Ok(Object::Name(name)) = dict.get(b"Type") {
                    let type_str = String::from_utf8_lossy(name);
                    &format!("Dict/{}", type_str)
                } else {
                    "Dictionary"
                }
            }
            Object::Stream(_) => {
                stream_count += 1;
                "Stream"
            }
            Object::Reference(_) => "Reference",
        };

        // Check if object can be compressed
        if !matches!(obj, Object::Stream(_)) {
            compressible_count += 1;
        }

        *type_counts.entry(type_name.to_string()).or_insert(0) += 1;
    }

    println!("\nPDF Statistics:");
    println!("Version: {}", doc.version);
    println!("Total objects: {}", doc.objects.len());
    println!("Stream objects: {} (cannot be compressed)", stream_count);
    println!("Compressible objects: {}", compressible_count);
    println!(
        "Compression potential: {:.1}%",
        (compressible_count as f64 / doc.objects.len() as f64) * 100.0
    );

    println!("\nObject type breakdown:");
    let mut sorted_types: Vec<_> = type_counts.iter().collect();
    sorted_types.sort_by_key(|(_, count)| *count);
    sorted_types.reverse();

    for (type_name, count) in sorted_types {
        println!("  {}: {}", type_name, count);
    }

    // Check if already using object streams
    let has_objstm = doc.objects.values().any(|obj| {
        if let Object::Dictionary(dict) = obj
            && let Ok(Object::Name(name)) = dict.get(b"Type")
        {
            return name == b"ObjStm";
        }
        false
    });

    println!(
        "\nAlready using object streams: {}",
        if has_objstm { "Yes" } else { "No" }
    );

    Ok(())
}
