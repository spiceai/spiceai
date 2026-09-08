use lopdf::Document;

#[cfg(not(feature = "async"))]
fn main() {
    // Test loading the encrypted.pdf file
    println!("Loading encrypted.pdf...");
    let doc = match Document::load("assets/encrypted.pdf") {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Failed to load encrypted.pdf: {:?}", e);
            return;
        }
    };

    println!("Document loaded successfully!");
    println!("Is encrypted: {}", doc.is_encrypted());

    // Try to get pages
    let pages = doc.get_pages();
    println!("Number of pages: {}", pages.len());

    // Try to extract text
    if pages.len() > 0 {
        let page_numbers: Vec<u32> = pages.keys().cloned().collect();
        match doc.extract_text(&page_numbers) {
            Ok(text) => {
                println!("Text extraction successful!");
                println!(
                    "Text preview (first 200 chars): {}",
                    text.chars().take(200).collect::<String>()
                );
            }
            Err(e) => {
                println!("Text extraction failed: {:?}", e);
            }
        }
    }

    // Check if we can access objects
    println!("\nChecking object access:");
    let max_check = 10;
    for i in 1..=max_check {
        if let Ok(_) = doc.get_object((i, 0)) {
            println!("  Object ({}, 0) found", i);
        }
    }

    // Check trailer
    println!("\nTrailer entries:");
    if let Ok(root) = doc.trailer.get(b"Root") {
        println!("  Root: {:?}", root);
    }
    if let Ok(encrypt) = doc.trailer.get(b"Encrypt") {
        println!("  Encrypt: {:?}", encrypt);
    }
    if let Ok(info) = doc.trailer.get(b"Info") {
        println!("  Info: {:?}", info);
    }
}

#[cfg(feature = "async")]
#[tokio::main]
async fn main() {
    // Test loading the encrypted.pdf file
    println!("Loading encrypted.pdf...");
    let doc = match Document::load("assets/encrypted.pdf").await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Failed to load encrypted.pdf: {:?}", e);
            return;
        }
    };

    println!("Document loaded successfully!");
    println!("Is encrypted: {}", doc.is_encrypted());

    // Try to get pages
    let pages = doc.get_pages();
    println!("Number of pages: {}", pages.len());

    // Try to extract text
    if !pages.is_empty() {
        let page_numbers: Vec<u32> = pages.keys().cloned().collect();
        match doc.extract_text(&page_numbers) {
            Ok(text) => {
                println!("Text extraction successful!");
                println!(
                    "Text preview (first 200 chars): {}",
                    text.chars().take(200).collect::<String>()
                );
            }
            Err(e) => {
                println!("Text extraction failed: {:?}", e);
            }
        }
    }

    // Check if we can access objects
    println!("\nChecking object access:");
    let max_check = 10;
    for i in 1..=max_check {
        if let Ok(_obj) = doc.get_object((i, 0)) {
            println!("  Object ({}, 0) found", i);
        }
    }

    // Check trailer
    println!("\nTrailer entries:");
    if let Ok(root) = doc.trailer.get(b"Root") {
        println!("  Root: {:?}", root);
    }
    if let Ok(encrypt) = doc.trailer.get(b"Encrypt") {
        println!("  Encrypt: {:?}", encrypt);
    }
    if let Ok(info) = doc.trailer.get(b"Info") {
        println!("  Info: {:?}", info);
    }
}
