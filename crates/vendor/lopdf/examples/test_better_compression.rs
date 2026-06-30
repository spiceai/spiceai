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
    let pdf_path = "/Users/nicolasdao/Downloads/poor.pdf";

    println!("Testing improved compression algorithm...\n");

    let doc = load_document(pdf_path)?;

    // Build complete reference graph
    let (non_compressible, reason_map) = find_all_non_compressible_objects(&doc);

    println!("Analysis complete:");
    println!("  Total objects: {}", doc.objects.len());
    println!("  Non-compressible: {}", non_compressible.len());
    println!("  Compressible: {}", doc.objects.len() - non_compressible.len());

    // Show some examples
    println!("\nSample non-compressible objects:");
    for (id, reason) in reason_map.iter().take(20) {
        println!("  {} {} R: {}", id.0, id.1, reason);
    }

    // Count by reason
    let mut reason_counts: HashMap<&str, usize> = HashMap::new();
    for reason in reason_map.values() {
        *reason_counts.entry(reason.as_str()).or_insert(0) += 1;
    }

    println!("\nReasons for non-compressibility:");
    for (reason, count) in reason_counts {
        println!("  {}: {}", reason, count);
    }

    // Simulate compression
    let mut would_compress = Vec::new();
    for &id in doc.objects.keys() {
        if !non_compressible.contains(&id) {
            would_compress.push(id);
        }
    }

    println!("\nWould compress {} objects", would_compress.len());

    // Check for orphaned references
    let orphans = check_for_orphans(&doc, &non_compressible, &would_compress);

    if orphans.is_empty() {
        println!("\n✓ No orphaned references would be created!");
    } else {
        println!("\n✗ WARNING: {} orphaned references would be created:", orphans.len());
        for (compressed_id, referencer_id) in orphans.iter().take(10) {
            println!(
                "  {} {} R would be compressed but is referenced by {} {} R",
                compressed_id.0, compressed_id.1, referencer_id.0, referencer_id.1
            );
        }
    }

    Ok(())
}

fn find_all_non_compressible_objects(doc: &Document) -> (HashSet<ObjectId>, HashMap<ObjectId, String>) {
    let mut non_compressible = HashSet::new();
    let mut reason_map = HashMap::new();

    // Phase 1: Mark inherently non-compressible objects
    for (&id, obj) in &doc.objects {
        let mut reason = None;

        // Streams cannot be compressed
        if matches!(obj, Object::Stream(_)) {
            reason = Some("Is a stream object");
        }

        // Check object type
        if let Object::Dictionary(dict) = obj
            && let Ok(type_obj) = dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
        {
            match type_name {
                b"Page" => reason = Some("Is a Page object"),
                b"Pages" => reason = Some("Is a Pages object"),
                b"Catalog" => reason = Some("Is a Catalog object"),
                b"XRef" => reason = Some("Is a cross-reference stream"),
                b"ObjStm" => reason = Some("Is an object stream"),
                _ => {}
            }
        }

        // Check if referenced in trailer
        for (_key, value) in doc.trailer.iter() {
            if value == &Object::Reference(id) {
                reason = Some("Referenced in trailer");
                break;
            }
        }

        if let Some(r) = reason {
            non_compressible.insert(id);
            reason_map.insert(id, r.to_string());
        }
    }

    // Phase 2: Iteratively mark objects referenced by non-compressible objects
    let mut changed = true;
    let mut iteration = 0;

    while changed {
        changed = false;
        iteration += 1;
        let mut newly_non_compressible = Vec::new();

        for &nc_id in &non_compressible {
            if let Ok(nc_obj) = doc.get_object(nc_id) {
                let refs = collect_all_references(nc_obj);

                for ref_id in refs {
                    if !non_compressible.contains(&ref_id) && doc.objects.contains_key(&ref_id) {
                        newly_non_compressible.push((
                            ref_id,
                            format!("Referenced by non-compressible {} {} R", nc_id.0, nc_id.1),
                        ));
                        changed = true;
                    }
                }
            }
        }

        for (id, reason) in newly_non_compressible {
            non_compressible.insert(id);
            reason_map.insert(id, reason);
        }
    }

    println!("Transitive closure computed in {} iterations", iteration);

    (non_compressible, reason_map)
}

fn collect_all_references(obj: &Object) -> HashSet<ObjectId> {
    let mut refs = HashSet::new();

    match obj {
        Object::Reference(id) => {
            refs.insert(*id);
        }
        Object::Array(array) => {
            for item in array {
                refs.extend(collect_all_references(item));
            }
        }
        Object::Dictionary(dict) => {
            for (_key, value) in dict.iter() {
                refs.extend(collect_all_references(value));
            }
        }
        Object::Stream(stream) => {
            for (_key, value) in stream.dict.iter() {
                refs.extend(collect_all_references(value));
            }
        }
        _ => {}
    }

    refs
}

fn check_for_orphans(
    doc: &Document, non_compressible: &HashSet<ObjectId>, would_compress: &[ObjectId],
) -> Vec<(ObjectId, ObjectId)> {
    let mut orphans = Vec::new();

    for &nc_id in non_compressible {
        if let Ok(nc_obj) = doc.get_object(nc_id) {
            let refs = collect_all_references(nc_obj);

            for ref_id in refs {
                if would_compress.contains(&ref_id) {
                    orphans.push((ref_id, nc_id));
                }
            }
        }
    }

    orphans
}
