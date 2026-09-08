use lopdf::Document;

#[cfg(not(feature = "async"))]
fn main() {
    // Collect command line arguments: input_file angle output_file
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() >= 3,
        "Not enough arguments: input_file output_file <password>"
    );
    let input_file = &args[1];
    let output_file = &args[2];
    let password = if args.len() >= 4 { &args[3] } else { "" };

    let mut doc = Document::load(input_file).unwrap();

    // Check if the document is actually encrypted.
    if doc.encryption_state.is_none() && !doc.is_encrypted() {
        println!("nothing to be done");
        return;
    }

    // Decrypt the document.
    if doc.is_encrypted() {
        doc.decrypt(password).unwrap();
    }

    // Store file in current working directory.
    doc.save(output_file).unwrap();
}

#[cfg(feature = "async")]
#[tokio::main]
async fn main() {
    // Collect command line arguments: input_file angle output_file
    let args: Vec<String> = std::env::args().collect();
    assert!(
        args.len() >= 3,
        "Not enough arguments: input_file output_file <password>"
    );
    let input_file = &args[1];
    let output_file = &args[2];
    let password = if args.len() >= 4 { &args[3] } else { "" };

    let mut doc = Document::load(input_file).await.unwrap();

    // Check if the document is actually encrypted.
    if doc.encryption_state.is_none() && !doc.is_encrypted() {
        println!("nothing to be done");
        return;
    }

    // Decrypt the document.
    if doc.is_encrypted() {
        doc.decrypt(password).unwrap();
    }

    // Store file in current working directory.
    doc.save(output_file).unwrap();
}
