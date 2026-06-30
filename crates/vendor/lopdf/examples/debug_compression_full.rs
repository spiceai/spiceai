use lopdf::{Document, Object, ObjectStream, SaveOptions};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Write;

type ObjectId = (u32, u16);
type ReferenceLocations = HashMap<ObjectId, (String, ObjectId)>;

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
    let input_file = if args.len() > 1 {
        &args[1]
    } else {
        eprintln!("Usage: cargo run --example debug_compression_full <pdf_file>");
        std::process::exit(1);
    };

    println!("=== COMPREHENSIVE PDF COMPRESSION DEBUG ===\n");

    // Load original PDF
    println!("1. Loading original PDF: {}", input_file);
    let mut doc = load_document(input_file)?;
    let _original_objects = doc.objects.len();

    // Analyze original structure
    println!("\n2. Analyzing original PDF structure:");
    let original_analysis = analyze_document(&doc);
    print_analysis(&original_analysis);

    // Create debug log file
    let debug_log_path = format!("{}_debug_log.txt", input_file.trim_end_matches(".pdf"));
    let mut debug_log = File::create(&debug_log_path)?;
    writeln!(debug_log, "PDF Compression Debug Log for: {}", input_file)?;
    writeln!(debug_log, "{}", "=".repeat(80))?;

    // Save with object streams - with detailed logging
    println!("\n3. Saving with object streams (with detailed logging)...");
    let options = SaveOptions {
        use_object_streams: true,
        use_xref_streams: true,
        ..Default::default()
    };

    // Simulate the compression process to log what would happen
    let mut compressible_objects = Vec::new();
    let mut non_compressible_objects = Vec::new();

    for (&id, obj) in &doc.objects {
        let can_compress = ObjectStream::can_be_compressed(id, obj, &doc);
        let obj_info = format!(
            "{} {} R: {} - {}",
            id.0,
            id.1,
            obj.type_name()
                .map(|n| String::from_utf8_lossy(n).to_string())
                .unwrap_or("Unknown".to_string()),
            describe_object(obj)
        );

        if can_compress {
            compressible_objects.push(obj_info.clone());
            writeln!(debug_log, "✓ COMPRESSIBLE: {}", obj_info)?;
        } else {
            non_compressible_objects.push(obj_info.clone());
            writeln!(debug_log, "✗ NOT COMPRESSIBLE: {}", obj_info)?;

            // Log why it's not compressible
            if matches!(obj, Object::Stream(_)) {
                writeln!(debug_log, "    Reason: Is a stream object")?;
            } else if let Object::Dictionary(dict) = obj
                && let Ok(type_obj) = dict.get(b"Type")
                && let Ok(type_name) = type_obj.as_name()
                && (type_name == b"Page" || type_name == b"Pages")
            {
                writeln!(debug_log, "    Reason: Is a Page/Pages object")?;
            }

            // Check if in trailer
            for (key, value) in doc.trailer.iter() {
                if value == &Object::Reference(id) {
                    writeln!(
                        debug_log,
                        "    Reason: Referenced in trailer[{}]",
                        String::from_utf8_lossy(key)
                    )?;
                }
            }
        }
    }

    writeln!(debug_log, "\nSummary:")?;
    writeln!(debug_log, "  Compressible objects: {}", compressible_objects.len())?;
    writeln!(
        debug_log,
        "  Non-compressible objects: {}",
        non_compressible_objects.len()
    )?;

    // Actually save
    let output_file = format!("{}_compressed.pdf", input_file.trim_end_matches(".pdf"));
    let mut buffer = Vec::new();
    doc.save_with_options(&mut buffer, options)?;
    std::fs::write(&output_file, &buffer)?;

    println!("Saved compressed PDF: {} ({} bytes)", output_file, buffer.len());

    // Load compressed PDF back
    println!("\n4. Loading compressed PDF back for analysis...");
    match Document::load_mem(&buffer) {
        Ok(compressed_doc) => {
            let compressed_analysis = analyze_document(&compressed_doc);

            println!("\n5. Compressed PDF structure:");
            print_analysis(&compressed_analysis);

            // Compare structures
            println!("\n6. Comparing structures:");
            compare_analyses(&original_analysis, &compressed_analysis, &mut debug_log)?;

            // Verify critical objects
            println!("\n7. Verifying critical objects in compressed PDF:");
            verify_critical_objects(&compressed_doc, &mut debug_log)?;

            // Check for orphaned references
            println!("\n8. Checking for orphaned references:");
            check_orphaned_references(&compressed_doc, &mut debug_log)?;
        }
        Err(e) => {
            eprintln!("ERROR: Failed to load compressed PDF: {}", e);
            writeln!(debug_log, "\nERROR: Failed to load compressed PDF: {}", e)?;
        }
    }

    println!("\n9. Debug log written to: {}", debug_log_path);

    // Try to use external PDF validator if available
    println!("\n10. Attempting external validation...");
    validate_with_external_tools(&output_file);

    Ok(())
}

#[derive(Debug)]
struct DocumentAnalysis {
    total_objects: usize,
    pages: usize,
    streams: usize,
    dictionaries: usize,
    arrays: usize,
    object_streams: usize,
    compressed_objects: usize,
    page_objects: Vec<(u32, u16)>,
    content_streams: Vec<(u32, u16)>,
    fonts: Vec<(u32, u16)>,
}

fn analyze_document(doc: &Document) -> DocumentAnalysis {
    let mut analysis = DocumentAnalysis {
        total_objects: doc.objects.len(),
        pages: doc.get_pages().len(),
        streams: 0,
        dictionaries: 0,
        arrays: 0,
        object_streams: 0,
        compressed_objects: 0,
        page_objects: Vec::new(),
        content_streams: Vec::new(),
        fonts: Vec::new(),
    };

    // Count compressed objects
    for xref_entry in doc.reference_table.entries.values() {
        if let lopdf::xref::XrefEntry::Compressed { .. } = xref_entry {
            analysis.compressed_objects += 1;
        }
    }

    // Analyze objects
    for (&id, obj) in &doc.objects {
        match obj {
            Object::Stream(stream) => {
                analysis.streams += 1;
                if let Ok(type_obj) = stream.dict.get(b"Type")
                    && let Ok(type_name) = type_obj.as_name()
                    && type_name == b"ObjStm"
                {
                    analysis.object_streams += 1;
                }
            }
            Object::Dictionary(dict) => {
                analysis.dictionaries += 1;
                if let Ok(type_obj) = dict.get(b"Type")
                    && let Ok(type_name) = type_obj.as_name()
                {
                    match type_name {
                        b"Page" => analysis.page_objects.push(id),
                        b"Font" => analysis.fonts.push(id),
                        _ => {}
                    }
                }
            }
            Object::Array(_) => analysis.arrays += 1,
            _ => {}
        }
    }

    // Find content streams
    for &page_id in &analysis.page_objects {
        if let Ok(page_obj) = doc.get_object(page_id)
            && let Object::Dictionary(page_dict) = page_obj
        {
            match page_dict.get(b"Contents") {
                Ok(Object::Reference(content_id)) => {
                    analysis.content_streams.push(*content_id);
                }
                Ok(Object::Array(contents)) => {
                    for content_ref in contents {
                        if let Object::Reference(content_id) = content_ref {
                            analysis.content_streams.push(*content_id);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    analysis
}

fn print_analysis(analysis: &DocumentAnalysis) {
    println!("  Total objects: {}", analysis.total_objects);
    println!("  Pages: {}", analysis.pages);
    println!("  Streams: {}", analysis.streams);
    println!("  Dictionaries: {}", analysis.dictionaries);
    println!("  Arrays: {}", analysis.arrays);
    println!("  Object streams: {}", analysis.object_streams);
    println!("  Compressed objects: {}", analysis.compressed_objects);
    println!("  Page objects: {:?}", analysis.page_objects);
    println!("  Content streams: {} found", analysis.content_streams.len());
    println!("  Fonts: {} found", analysis.fonts.len());
}

fn describe_object(obj: &Object) -> String {
    match obj {
        Object::Null => "Null".to_string(),
        Object::Boolean(b) => format!("Boolean({})", b),
        Object::Integer(i) => format!("Integer({})", i),
        Object::Real(r) => format!("Real({})", r),
        Object::Name(n) => format!("Name({})", String::from_utf8_lossy(n)),
        Object::String(_, _) => "String".to_string(),
        Object::Array(a) => format!("Array[{}]", a.len()),
        Object::Dictionary(d) => {
            let keys: Vec<_> = d.iter().take(3).map(|(k, _)| String::from_utf8_lossy(k)).collect();
            format!("Dict{{{}...}}", keys.join(", "))
        }
        Object::Stream(s) => {
            let keys: Vec<_> = s.dict.iter().take(3).map(|(k, _)| String::from_utf8_lossy(k)).collect();
            format!("Stream{{dict: {{{}...}}, len: {}}}", keys.join(", "), s.content.len())
        }
        Object::Reference(id) => format!("Ref({} {} R)", id.0, id.1),
    }
}

fn compare_analyses(original: &DocumentAnalysis, compressed: &DocumentAnalysis, log: &mut File) -> std::io::Result<()> {
    writeln!(log, "\n=== STRUCTURE COMPARISON ===")?;

    if original.pages != compressed.pages {
        writeln!(
            log,
            "WARNING: Page count changed! {} -> {}",
            original.pages, compressed.pages
        )?;
    }

    if original.page_objects.len() != compressed.page_objects.len() {
        writeln!(
            log,
            "WARNING: Page object count changed! {} -> {}",
            original.page_objects.len(),
            compressed.page_objects.len()
        )?;
    }

    if original.content_streams.len() != compressed.content_streams.len() {
        writeln!(
            log,
            "WARNING: Content stream count changed! {} -> {}",
            original.content_streams.len(),
            compressed.content_streams.len()
        )?;
    }

    writeln!(
        log,
        "Object count: {} -> {} (compressed {} objects)",
        original.total_objects, compressed.total_objects, compressed.compressed_objects
    )?;

    writeln!(log, "Object streams created: {}", compressed.object_streams)?;

    Ok(())
}

fn verify_critical_objects(doc: &Document, log: &mut File) -> std::io::Result<()> {
    writeln!(log, "\n=== CRITICAL OBJECT VERIFICATION ===")?;

    // Check all page objects
    let pages = doc.get_pages();
    for (num, &page_id) in pages.iter() {
        match doc.get_object(page_id) {
            Ok(obj) => {
                writeln!(
                    log,
                    "Page {} ({} {} R): OK - {}",
                    num,
                    page_id.0,
                    page_id.1,
                    describe_object(obj)
                )?;

                // Check if it's compressed
                if let Some(xref) = doc.reference_table.get(page_id.0)
                    && let lopdf::xref::XrefEntry::Compressed { container, index } = xref
                {
                    writeln!(
                        log,
                        "  ERROR: Page is compressed in stream {} at index {}!",
                        container, index
                    )?;
                }

                // Check page contents
                if let Object::Dictionary(page_dict) = obj {
                    match page_dict.get(b"Contents") {
                        Ok(Object::Reference(content_id)) => match doc.get_object(*content_id) {
                            Ok(_) => writeln!(log, "  Contents {} {} R: OK", content_id.0, content_id.1)?,
                            Err(e) => writeln!(log, "  Contents {} {} R: ERROR - {}", content_id.0, content_id.1, e)?,
                        },
                        Ok(Object::Array(contents)) => {
                            writeln!(log, "  Contents array with {} elements", contents.len())?;
                            for (i, content_ref) in contents.iter().enumerate() {
                                if let Object::Reference(content_id) = content_ref {
                                    match doc.get_object(*content_id) {
                                        Ok(_) => writeln!(log, "    [{}] {} {} R: OK", i, content_id.0, content_id.1)?,
                                        Err(e) => writeln!(
                                            log,
                                            "    [{}] {} {} R: ERROR - {}",
                                            i, content_id.0, content_id.1, e
                                        )?,
                                    }
                                }
                            }
                        }
                        _ => writeln!(log, "  Contents: Unexpected type")?,
                    }
                }
            }
            Err(e) => {
                writeln!(log, "Page {} ({} {} R): ERROR - {}", num, page_id.0, page_id.1, e)?;
            }
        }
    }

    Ok(())
}

fn check_orphaned_references(doc: &Document, log: &mut File) -> std::io::Result<()> {
    writeln!(log, "\n=== ORPHANED REFERENCE CHECK ===")?;

    let mut all_references = HashMap::new();

    // Collect all references
    for (id, obj) in &doc.objects {
        collect_references(obj, &mut all_references, *id);
    }

    // Check trailer references
    for (_key, value) in doc.trailer.iter() {
        if let Object::Reference(ref_id) = value {
            all_references.insert(*ref_id, ("trailer".to_string(), (0, 0)));
        }
    }

    // Check if all referenced objects exist
    let mut orphaned_count = 0;
    for (ref_id, (location, from_id)) in &all_references {
        match doc.get_object(*ref_id) {
            Ok(_) => {
                // Check if it's in a compressed stream
                if let Some(xref) = doc.reference_table.get(ref_id.0)
                    && let lopdf::xref::XrefEntry::Compressed { container, .. } = xref
                {
                    // Make sure the container exists
                    if doc.get_object((*container, 0)).is_err() {
                        writeln!(
                            log,
                            "ERROR: Reference {} {} R from {} ({} {} R) points to compressed object in non-existent stream {}!",
                            ref_id.0, ref_id.1, location, from_id.0, from_id.1, container
                        )?;
                        orphaned_count += 1;
                    }
                }
            }
            Err(_) => {
                writeln!(
                    log,
                    "ERROR: Orphaned reference {} {} R from {} ({} {} R)",
                    ref_id.0, ref_id.1, location, from_id.0, from_id.1
                )?;
                orphaned_count += 1;
            }
        }
    }

    writeln!(log, "\nTotal references checked: {}", all_references.len())?;
    writeln!(log, "Orphaned references: {}", orphaned_count)?;

    Ok(())
}

fn collect_references(obj: &Object, refs: &mut ReferenceLocations, from_id: ObjectId) {
    match obj {
        Object::Reference(ref_id) => {
            refs.insert(*ref_id, ("direct".to_string(), from_id));
        }
        Object::Array(array) => {
            for (i, item) in array.iter().enumerate() {
                if let Object::Reference(ref_id) = item {
                    refs.insert(*ref_id, (format!("array[{}]", i), from_id));
                }
                collect_references(item, refs, from_id);
            }
        }
        Object::Dictionary(dict) => {
            for (key, value) in dict.iter() {
                if let Object::Reference(ref_id) = value {
                    refs.insert(*ref_id, (format!("dict[{}]", String::from_utf8_lossy(key)), from_id));
                }
                collect_references(value, refs, from_id);
            }
        }
        Object::Stream(stream) => {
            for (key, value) in stream.dict.iter() {
                if let Object::Reference(ref_id) = value {
                    refs.insert(
                        *ref_id,
                        (format!("stream.dict[{}]", String::from_utf8_lossy(key)), from_id),
                    );
                }
                collect_references(value, refs, from_id);
            }
        }
        _ => {}
    }
}

fn validate_with_external_tools(pdf_path: &str) {
    // Try to use pdfinfo if available
    println!("Trying pdfinfo...");
    match std::process::Command::new("pdfinfo").arg(pdf_path).output() {
        Ok(output) => {
            if output.status.success() {
                println!("✓ pdfinfo validation passed");
            } else {
                println!("✗ pdfinfo validation failed:");
                println!("{}", String::from_utf8_lossy(&output.stderr));
            }
        }
        Err(_) => {
            println!("  pdfinfo not available");
        }
    }

    // Try to use qpdf if available
    println!("\nTrying qpdf --check...");
    match std::process::Command::new("qpdf").args(["--check", pdf_path]).output() {
        Ok(output) => {
            if output.status.success() {
                println!("✓ qpdf validation passed");
            } else {
                println!("✗ qpdf validation failed:");
                println!("{}", String::from_utf8_lossy(&output.stderr));
            }
        }
        Err(_) => {
            println!("  qpdf not available");
        }
    }

    // Try to use mutool if available
    println!("\nTrying mutool info...");
    match std::process::Command::new("mutool").args(["info", pdf_path]).output() {
        Ok(output) => {
            if output.status.success() {
                println!("✓ mutool validation passed");
            } else {
                println!("✗ mutool validation failed:");
                println!("{}", String::from_utf8_lossy(&output.stderr));
            }
        }
        Err(_) => {
            println!("  mutool not available");
        }
    }
}
