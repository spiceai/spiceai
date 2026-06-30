use lopdf::content::{Content, Operation};
use lopdf::dictionary;
use lopdf::{Document, Object, Stream};

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
    println!("Creating a text-heavy PDF with many objects...");

    let mut doc = Document::with_version("1.5");
    let pages_id = doc.new_object_id();

    // Create many font objects
    let mut font_ids = Vec::new();
    for i in 0..20 {
        let font_id = doc.add_object(dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => format!("Font{}", i),
            "Encoding" => "WinAnsiEncoding"
        });
        font_ids.push(font_id);
    }

    // Create many metadata objects
    for i in 0..50 {
        doc.add_object(dictionary! {
            "Type" => "Metadata",
            "Version" => i,
            "Author" => format!("Author {}", i),
            "Title" => format!("Document Section {}", i),
            "Subject" => format!("This is metadata object number {} with some additional text to make it larger", i),
            "Keywords" => format!("keyword{}, test{}, pdf{}, compression{}", i, i, i, i),
            "Creator" => "lopdf object streams demo",
            "Producer" => "lopdf library with object stream support",
            "CreationDate" => format!("D:2024010{}120000", i % 10)
        });
    }

    // Create annotation objects
    for i in 0..30 {
        doc.add_object(dictionary! {
            "Type" => "Annot",
            "Subtype" => "Text",
            "Rect" => vec![100.into(), (100 + i * 20).into(), 200.into(), (120 + i * 20).into()],
            "Contents" => format!("This is annotation number {} with some descriptive text", i),
            "Open" => false,
            "Name" => "Comment",
            "C" => vec![1.0.into(), 0.0.into(), 0.0.into()]
        });
    }

    // Create outline objects (bookmarks)
    for i in 0..25 {
        doc.add_object(dictionary! {
            "Title" => format!("Chapter {}: Introduction to Section {}", i, i),
            "Count" => 0,
            "Dest" => format!("section_{}", i)
        });
    }

    let resources_id = doc.add_object(dictionary! {
        "Font" => dictionary! {
            "F1" => font_ids[0],
        },
    });

    // Create page content with lots of text
    let mut operations = vec![
        Operation::new("BT", vec![]),
        Operation::new("Tf", vec!["F1".into(), 12.into()]),
        Operation::new("Td", vec![50.into(), 750.into()]),
        Operation::new("Tj", vec![Object::string_literal("Text-Heavy PDF Compression Demo")]),
        Operation::new("ET", vec![]),
    ];

    // Add many text operations
    for i in 0..50 {
        operations.extend(vec![
            Operation::new("BT", vec![]),
            Operation::new("Tf", vec!["F1".into(), 10.into()]),
            Operation::new("Td", vec![50.into(), (700 - i * 10).into()]),
            Operation::new(
                "Tj",
                vec![Object::string_literal(format!(
                    "Line {}: This is a sample text line to demonstrate compression",
                    i
                ))],
            ),
            Operation::new("ET", vec![]),
        ]);
    }

    let content = Content { operations };
    let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode()?));

    let page_id = doc.add_object(dictionary! {
        "Type" => "Page",
        "Parent" => pages_id,
        "Contents" => content_id,
        "Resources" => resources_id,
        "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
    });

    doc.objects.insert(
        pages_id,
        Object::Dictionary(dictionary! {
            "Type" => "Pages",
            "Kids" => vec![page_id.into()],
            "Count" => 1,
        }),
    );

    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id,
    });

    doc.trailer.set("Root", catalog_id);

    // Save and compare
    doc.save("text_heavy_traditional.pdf")?;
    let traditional_size = std::fs::metadata("text_heavy_traditional.pdf")?.len();

    let mut modern_file = std::fs::File::create("text_heavy_compressed.pdf")?;
    doc.save_modern(&mut modern_file)?;
    drop(modern_file);
    let modern_size = std::fs::metadata("text_heavy_compressed.pdf")?.len();

    println!("\nCreated PDF with {} objects", doc.objects.len());
    println!("\nFile sizes:");
    println!(
        "Traditional format: {} bytes ({:.1} KB)",
        traditional_size,
        traditional_size as f64 / 1024.0
    );
    println!(
        "With object streams: {} bytes ({:.1} KB)",
        modern_size,
        modern_size as f64 / 1024.0
    );

    let reduction = 100.0 - (modern_size as f64 / traditional_size as f64 * 100.0);
    println!("\nCompression achieved: {:.1}%", reduction);

    println!("\nFiles created:");
    println!("  - text_heavy_traditional.pdf");
    println!("  - text_heavy_compressed.pdf");

    // Analyze the compressed PDF
    println!("\nAnalyzing compressed PDF:");
    let compressed_doc = load_document("text_heavy_compressed.pdf")?;
    let has_objstm = compressed_doc.objects.values().any(|obj| {
        if let Object::Dictionary(dict) = obj
            && let Ok(Object::Name(name)) = dict.get(b"Type")
        {
            return name == b"ObjStm";
        }
        false
    });
    println!("Uses object streams: {}", has_objstm);

    Ok(())
}
