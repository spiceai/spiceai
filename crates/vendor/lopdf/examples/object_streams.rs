use lopdf::content::{Content, Operation};
use lopdf::dictionary;
use lopdf::{Document, Object, SaveOptions, Stream};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a simple PDF document
    let mut doc = Document::with_version("1.5");
    let pages_id = doc.new_object_id();

    // Add many objects to demonstrate object stream compression
    let font_id = doc.add_object(dictionary! {
        "Type" => "Font",
        "Subtype" => "Type1",
        "BaseFont" => "Helvetica"
    });

    let bold_font_id = doc.add_object(dictionary! {
        "Type" => "Font",
        "Subtype" => "Type1",
        "BaseFont" => "Helvetica-Bold"
    });

    let italic_font_id = doc.add_object(dictionary! {
        "Type" => "Font",
        "Subtype" => "Type1",
        "BaseFont" => "Helvetica-Oblique"
    });

    // Add some metadata objects
    for i in 0..20 {
        doc.add_object(dictionary! {
            "Type" => format!("CustomData{}", i),
            "Value" => i,
            "Description" => format!("This is custom data object number {}", i)
        });
    }

    let resources_id = doc.add_object(dictionary! {
        "Font" => dictionary! {
            "F1" => font_id,
            "F2" => bold_font_id,
            "F3" => italic_font_id,
        },
    });

    // Create page content
    let content = Content {
        operations: vec![
            Operation::new("BT", vec![]),
            Operation::new("Tf", vec!["F1".into(), 24.into()]),
            Operation::new("Td", vec![50.into(), 700.into()]),
            Operation::new("Tj", vec![Object::string_literal("Object Streams Demo")]),
            Operation::new("ET", vec![]),
            Operation::new("BT", vec![]),
            Operation::new("Tf", vec!["F2".into(), 16.into()]),
            Operation::new("Td", vec![50.into(), 650.into()]),
            Operation::new("Tj", vec![Object::string_literal("This PDF uses object streams!")]),
            Operation::new("ET", vec![]),
            Operation::new("BT", vec![]),
            Operation::new("Tf", vec!["F1".into(), 12.into()]),
            Operation::new("Td", vec![50.into(), 600.into()]),
            Operation::new(
                "Tj",
                vec![Object::string_literal(
                    "Multiple non-stream objects are compressed together.",
                )],
            ),
            Operation::new("ET", vec![]),
        ],
    };

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

    // Save with traditional format
    let mut traditional_buffer = Vec::new();
    doc.save_to(&mut traditional_buffer)?;
    let traditional_size = traditional_buffer.len();

    // Save with object streams
    let mut modern_buffer = Vec::new();
    doc.save_modern(&mut modern_buffer)?;
    let modern_size = modern_buffer.len();

    // Save with custom options
    let mut custom_buffer = Vec::new();
    let options = SaveOptions::builder()
        .use_object_streams(true)
        .use_xref_streams(true)
        .max_objects_per_stream(10)
        .compression_level(9)
        .build();
    doc.save_with_options(&mut custom_buffer, options)?;
    let custom_size = custom_buffer.len();

    // Compare sizes
    println!("PDF Size Comparison:");
    println!("Traditional format: {} bytes", traditional_size);
    println!("With object streams: {} bytes", modern_size);
    println!("With custom options: {} bytes", custom_size);

    let reduction = 100.0 - (modern_size as f64 / traditional_size as f64 * 100.0);
    println!("\nSize reduction: {:.1}%", reduction);

    // Save the modern version to a file
    std::fs::write("object_streams_demo.pdf", &modern_buffer)?;
    println!("\nSaved modern PDF to: object_streams_demo.pdf");

    Ok(())
}
