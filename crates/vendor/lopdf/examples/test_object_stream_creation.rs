use lopdf::{Dictionary, Document, Object, SaveOptions};

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
    // Create a simple PDF with objects that can be compressed
    let mut doc = Document::with_version("1.4");

    // Create catalog
    let mut catalog = Dictionary::new();
    catalog.set("Type", "Catalog");
    catalog.set("Pages", Object::Reference((2, 0)));
    doc.objects.insert((1, 0), Object::Dictionary(catalog));

    // Create pages root
    let mut pages = Dictionary::new();
    pages.set("Type", "Pages");
    pages.set("Kids", vec![Object::Reference((3, 0))]);
    pages.set("Count", 1);
    doc.objects.insert((2, 0), Object::Dictionary(pages));

    // Create a page
    let mut page = Dictionary::new();
    page.set("Type", "Page");
    page.set("Parent", Object::Reference((2, 0)));
    page.set("MediaBox", vec![0.into(), 0.into(), 612.into(), 792.into()]);
    doc.objects.insert((3, 0), Object::Dictionary(page));

    // Create some metadata objects that should be compressible
    for i in 10..20 {
        let mut meta = Dictionary::new();
        meta.set("Type", "Metadata");
        meta.set("ID", i);
        meta.set("Data", format!("This is metadata object {}", i));
        doc.objects.insert((i as u32, 0), Object::Dictionary(meta));
    }

    // Create some annotation objects that should be compressible
    for i in 20..30 {
        let mut annot = Dictionary::new();
        annot.set("Type", "Annot");
        annot.set("Subtype", "Text");
        annot.set("Contents", format!("Annotation {}", i));
        annot.set("Rect", vec![100.into(), 100.into(), 200.into(), 200.into()]);
        doc.objects.insert((i as u32, 0), Object::Dictionary(annot));
    }

    // Set up trailer
    doc.trailer.set("Root", Object::Reference((1, 0)));
    doc.max_id = 30;
    doc.renumber_objects();

    // Save without object streams
    println!("Saving without object streams...");
    doc.save("test_no_objstm.pdf")?;

    // Save with object streams
    println!("Saving with object streams...");
    let options = SaveOptions {
        use_object_streams: true,
        use_xref_streams: true,
        ..Default::default()
    };
    doc.save_with_options(&mut std::fs::File::create("test_with_objstm.pdf")?, options)?;

    // Check file sizes
    let no_objstm_size = std::fs::metadata("test_no_objstm.pdf")?.len();
    let with_objstm_size = std::fs::metadata("test_with_objstm.pdf")?.len();

    println!("\nFile sizes:");
    println!("  Without object streams: {} bytes", no_objstm_size);
    println!("  With object streams: {} bytes", with_objstm_size);
    println!(
        "  Reduction: {:.1}%",
        (1.0 - with_objstm_size as f64 / no_objstm_size as f64) * 100.0
    );

    // Load the compressed PDF and check for object streams
    println!("\nChecking compressed PDF...");
    let compressed_doc = load_document("test_with_objstm.pdf")?;

    let mut objstm_count = 0;
    let mut compressed_count = 0;

    for obj in compressed_doc.objects.values() {
        if let Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            objstm_count += 1;
            // Count objects in this stream
            if let Ok(n) = stream.dict.get(b"N")
                && let Ok(n_val) = n.as_i64()
            {
                compressed_count += n_val as usize;
            }
        }
    }

    println!("  Object streams found: {}", objstm_count);
    println!("  Total compressed objects: {}", compressed_count);

    // Also check the raw file content
    println!("\nChecking raw file for /ObjStm...");
    let content = std::fs::read_to_string("test_with_objstm.pdf").unwrap_or_default();
    let objstm_occurrences = content.matches("/ObjStm").count();
    println!("  /ObjStm occurrences in file: {}", objstm_occurrences);

    Ok(())
}
