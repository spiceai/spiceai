use lopdf::{Document, Object, ObjectStream, dictionary};

#[cfg(feature = "async")]
use tokio::runtime::Builder;

#[cfg(not(feature = "async"))]
fn load_document(path: &str) -> Document {
    Document::load(path).unwrap()
}

#[cfg(feature = "async")]
fn load_document(path: &str) -> Document {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { Document::load(path).await.unwrap() })
}

fn main() {
    println!("Testing direct object stream creation and saving...\n");

    // Create a simple document
    let mut doc = Document::with_version("1.5");

    // Create an object stream manually
    let mut obj_stream = ObjectStream::builder().max_objects(10).compression_level(6).build();

    // Add some objects
    obj_stream.add_object((1, 0), Object::Integer(42)).unwrap();
    obj_stream
        .add_object((2, 0), Object::String(b"Test".to_vec(), lopdf::StringFormat::Literal))
        .unwrap();

    println!("Object stream has {} objects", obj_stream.object_count());

    // Convert to stream object
    let stream_obj = obj_stream.to_stream_object().unwrap();
    println!("Stream dict: {:?}", stream_obj.dict);
    println!("Stream has Filter: {}", stream_obj.dict.get(b"Filter").is_ok());

    // Add it to the document
    let stream_id = doc.add_object(stream_obj);
    println!("Added object stream as {} 0 R", stream_id.0);

    // Add a simple catalog
    let pages_id = doc.add_object(dictionary! {
        "Type" => "Pages",
        "Kids" => vec![],
        "Count" => 0
    });

    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id
    });

    doc.trailer.set("Root", catalog_id);

    // Save the document
    println!("\nSaving document...");
    doc.save("test_direct_objstream.pdf").unwrap();

    println!("Saved to test_direct_objstream.pdf");

    // Load it back and check
    println!("\nLoading back...");
    let loaded = load_document("test_direct_objstream.pdf");

    for (id, obj) in &loaded.objects {
        if let Object::Stream(stream) = obj
            && let Ok(type_obj) = stream.dict.get(b"Type")
            && let Ok(type_name) = type_obj.as_name()
            && type_name == b"ObjStm"
        {
            println!("Found object stream {} 0 R", id.0);
            println!("  Dict: {:?}", stream.dict);
            println!("  Has Filter: {}", stream.dict.get(b"Filter").is_ok());
            if let Ok(filter) = stream.dict.get(b"Filter") {
                println!("  Filter value: {:?}", filter);
            }
        }
    }
}
