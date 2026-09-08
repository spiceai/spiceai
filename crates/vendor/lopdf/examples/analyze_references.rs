use lopdf::{Document, Object, ObjectId};
use std::collections::{HashMap, HashSet};

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
    let pdf_path = "/Users/nicolasdao/Downloads/pdfs/RFQ - SDS WebApp.docx.pdf";
    println!("Analyzing references in: {}", pdf_path);

    let doc = load_document(pdf_path)?;

    // Build reference graph
    let mut references: HashMap<ObjectId, HashSet<ObjectId>> = HashMap::new();
    let mut referenced_by: HashMap<ObjectId, HashSet<ObjectId>> = HashMap::new();

    // Collect all references
    for (&id, obj) in &doc.objects {
        let refs = collect_references_from_object(obj);
        references.insert(id, refs.clone());

        // Build reverse mapping
        for ref_id in refs {
            referenced_by.entry(ref_id).or_default().insert(id);
        }
    }

    // Also check trailer references
    let trailer_refs = collect_references_from_dict(&doc.trailer);
    for ref_id in &trailer_refs {
        referenced_by.entry(*ref_id).or_default().insert((0, 0));
    }

    // Analyze specific problematic objects
    println!("\nAnalyzing problematic objects:");

    // Object 427 (referenced by catalog 431)
    if let Some(refs_to_427) = referenced_by.get(&(427, 0)) {
        println!("\nObject 427 0 R is referenced by:");
        for &ref_id in refs_to_427 {
            println!("  {} {} R", ref_id.0, ref_id.1);
        }
    }

    // Check what object 427 is
    if let Ok(obj_427) = doc.get_object((427, 0)) {
        println!("Object 427 is: {:?}", describe_object(obj_427));
    }

    // Check fonts that reference orphaned objects
    println!("\nChecking font references:");
    for &font_id in &[(8, 0), (9, 0), (10, 0), (11, 0), (12, 0)] {
        if let Ok(Object::Dictionary(font_dict)) = doc.get_object(font_id) {
            println!("\nFont {} {} R:", font_id.0, font_id.1);
            for (key, value) in font_dict.iter() {
                if let Object::Reference(ref_id) = value {
                    let exists = doc.objects.contains_key(ref_id);
                    println!(
                        "  {} -> {} {} R (exists: {})",
                        String::from_utf8_lossy(key),
                        ref_id.0,
                        ref_id.1,
                        exists
                    );
                }
            }
        }
    }

    // Analyze which objects would be compressed
    println!("\n\nAnalyzing compression eligibility:");
    let mut would_compress = Vec::new();
    let mut would_not_compress = Vec::new();

    for (&id, obj) in &doc.objects {
        if lopdf::ObjectStream::can_be_compressed(id, obj, &doc) {
            would_compress.push(id);
        } else {
            would_not_compress.push(id);
        }
    }

    // Check if any compressed objects are referenced by non-compressed objects
    println!("\nChecking for problematic compressions:");
    let mut problems = Vec::new();

    for &compressed_id in &would_compress {
        if let Some(referencers) = referenced_by.get(&compressed_id) {
            for &referencer_id in referencers {
                if referencer_id == (0, 0) {
                    // Referenced from trailer
                    problems.push((compressed_id, referencer_id, "trailer"));
                } else if would_not_compress.contains(&referencer_id) {
                    // Referenced by a non-compressed object
                    problems.push((compressed_id, referencer_id, "non-compressed object"));
                }
            }
        }
    }

    if !problems.is_empty() {
        println!("\nFOUND {} PROBLEMATIC COMPRESSIONS:", problems.len());
        for (compressed_id, referencer_id, reason) in problems.iter().take(20) {
            println!(
                "  {} {} R would be compressed but is referenced by {} {} R ({})",
                compressed_id.0, compressed_id.1, referencer_id.0, referencer_id.1, reason
            );
        }
        if problems.len() > 20 {
            println!("  ... and {} more", problems.len() - 20);
        }
    }

    Ok(())
}

fn collect_references_from_object(obj: &Object) -> HashSet<ObjectId> {
    let mut refs = HashSet::new();

    match obj {
        Object::Reference(id) => {
            refs.insert(*id);
        }
        Object::Array(array) => {
            for item in array {
                refs.extend(collect_references_from_object(item));
            }
        }
        Object::Dictionary(dict) => {
            refs.extend(collect_references_from_dict(dict));
        }
        Object::Stream(stream) => {
            refs.extend(collect_references_from_dict(&stream.dict));
        }
        _ => {}
    }

    refs
}

fn collect_references_from_dict(dict: &lopdf::Dictionary) -> HashSet<ObjectId> {
    let mut refs = HashSet::new();

    for (_key, value) in dict.iter() {
        refs.extend(collect_references_from_object(value));
    }

    refs
}

fn describe_object(obj: &Object) -> String {
    match obj {
        Object::Dictionary(d) => {
            let type_info = d
                .get(b"Type")
                .ok()
                .and_then(|t| t.as_name().ok())
                .map(|n| String::from_utf8_lossy(n).to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            format!("Dictionary (Type: {})", type_info)
        }
        _ => format!("{:?}", obj),
    }
}
