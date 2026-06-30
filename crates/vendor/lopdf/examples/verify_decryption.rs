// Example demonstrating PDF decryption capabilities
// This verifies the modifications to src/reader.rs for handling encrypted PDFs

use lopdf::{Document, EncryptionState, EncryptionVersion, Permissions};

#[cfg(not(feature = "async"))]
fn main() {
    println!("=== PDF Decryption Verification ===\n");

    // Test 1: Load an encrypted PDF from assets
    println!("Test 1: Loading encrypted PDF from assets/encrypted.pdf");
    match Document::load("assets/encrypted.pdf") {
        Ok(doc) => {
            println!("✓ Successfully loaded encrypted PDF");
            println!("  - Is encrypted: {}", doc.is_encrypted());
            println!("  - Number of pages: {}", doc.get_pages().len());
            println!("  - Has encryption state: {}", doc.encryption_state.is_some());

            // Try to extract text
            let pages = doc.get_pages();
            let page_nums: Vec<u32> = pages.keys().cloned().collect();
            match doc.extract_text(&page_nums) {
                Ok(text) => {
                    println!("  - Text extraction successful");
                    println!("  - Text length: {} characters", text.len());
                }
                Err(e) => println!("  - Text extraction failed: {:?}", e),
            }
        }
        Err(e) => println!("✗ Failed to load encrypted PDF: {:?}", e),
    }

    println!();

    // Test 2: Create and encrypt a new PDF, then verify it can be loaded
    println!("Test 2: Creating, encrypting, and re-loading a PDF");

    // Create a simple PDF
    let mut doc = Document::with_version("1.5");

    // Add ID (required for encryption)
    doc.trailer.set(
        "ID",
        lopdf::Object::Array(vec![
            lopdf::Object::String(vec![1u8; 16], lopdf::StringFormat::Literal),
            lopdf::Object::String(vec![2u8; 16], lopdf::StringFormat::Literal),
        ]),
    );

    // Add minimal structure
    let catalog = doc.add_object(lopdf::dictionary! {
        "Type" => "Catalog",
        "Pages" => lopdf::Object::Reference((2, 0))
    });
    doc.trailer.set("Root", lopdf::Object::Reference(catalog));

    doc.objects.insert(
        (2, 0),
        lopdf::Object::Dictionary(lopdf::dictionary! {
            "Type" => "Pages",
            "Count" => 0,
            "Kids" => Vec::<lopdf::Object>::new()
        }),
    );

    // Encrypt the document
    let encryption_version = EncryptionVersion::V2 {
        document: &doc,
        owner_password: "",
        user_password: "",
        key_length: 128,
        permissions: Permissions::all(),
    };

    match EncryptionState::try_from(encryption_version) {
        Ok(state) => {
            doc.encrypt(&state).unwrap();
            println!("✓ Document encrypted successfully");

            // Save to temporary location
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("test_encrypted.pdf");
            doc.save(&path).unwrap();
            println!("✓ Encrypted document saved");

            // Try to load it back
            match Document::load(&path) {
                Ok(loaded_doc) => {
                    println!("✓ Encrypted document re-loaded successfully");
                    println!("  - Is encrypted: {}", loaded_doc.is_encrypted());
                    println!("  - Has encryption state: {}", loaded_doc.encryption_state.is_some());
                }
                Err(e) => println!("✗ Failed to re-load encrypted document: {:?}", e),
            }
        }
        Err(e) => println!("✗ Failed to create encryption state: {:?}", e),
    }

    println!();

    // Test 3: Verify object access in encrypted PDF
    println!("Test 3: Verifying object access in encrypted PDF");
    if let Ok(doc) = Document::load("assets/encrypted.pdf") {
        let mut accessible_objects = 0;
        let mut total_checked = 0;

        for i in 1..=20 {
            total_checked += 1;
            if doc.get_object((i, 0)).is_ok() {
                accessible_objects += 1;
            }
        }

        println!("✓ Object access test completed");
        println!("  - Objects checked: {}", total_checked);
        println!("  - Objects accessible: {}", accessible_objects);
        println!(
            "  - Success rate: {:.1}%",
            (accessible_objects as f64 / total_checked as f64) * 100.0
        );
    }

    println!("\n=== All decryption tests completed ===");
}

#[cfg(feature = "async")]
#[tokio::main]
async fn main() {
    println!("=== PDF Decryption Verification (Async) ===\n");

    // Test 1: Load an encrypted PDF from assets
    println!("Test 1: Loading encrypted PDF from assets/encrypted.pdf");
    match Document::load("assets/encrypted.pdf").await {
        Ok(doc) => {
            println!("✓ Successfully loaded encrypted PDF");
            println!("  - Is encrypted: {}", doc.is_encrypted());
            println!("  - Number of pages: {}", doc.get_pages().len());
            println!("  - Has encryption state: {}", doc.encryption_state.is_some());

            // Try to extract text
            let pages = doc.get_pages();
            let page_nums: Vec<u32> = pages.keys().cloned().collect();
            match doc.extract_text(&page_nums) {
                Ok(text) => {
                    println!("  - Text extraction successful");
                    println!("  - Text length: {} characters", text.len());
                }
                Err(e) => println!("  - Text extraction failed: {:?}", e),
            }
        }
        Err(e) => println!("✗ Failed to load encrypted PDF: {:?}", e),
    }

    println!();

    // Test 2: Create and encrypt a new PDF, then verify it can be loaded
    println!("Test 2: Creating, encrypting, and re-loading a PDF");

    // Create a simple PDF
    let mut doc = Document::with_version("1.5");

    // Add ID (required for encryption)
    doc.trailer.set(
        "ID",
        lopdf::Object::Array(vec![
            lopdf::Object::String(vec![1u8; 16], lopdf::StringFormat::Literal),
            lopdf::Object::String(vec![2u8; 16], lopdf::StringFormat::Literal),
        ]),
    );

    // Add minimal structure
    let catalog = doc.add_object(lopdf::dictionary! {
        "Type" => "Catalog",
        "Pages" => lopdf::Object::Reference((2, 0))
    });
    doc.trailer.set("Root", lopdf::Object::Reference(catalog));

    doc.objects.insert(
        (2, 0),
        lopdf::Object::Dictionary(lopdf::dictionary! {
            "Type" => "Pages",
            "Count" => 0,
            "Kids" => Vec::<lopdf::Object>::new()
        }),
    );

    // Encrypt the document
    let encryption_version = EncryptionVersion::V2 {
        document: &doc,
        owner_password: "",
        user_password: "",
        key_length: 128,
        permissions: Permissions::all(),
    };

    match EncryptionState::try_from(encryption_version) {
        Ok(state) => {
            doc.encrypt(&state).unwrap();
            println!("✓ Document encrypted successfully");

            // Save to temporary location
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("test_encrypted.pdf");
            doc.save(&path).unwrap();
            println!("✓ Encrypted document saved");

            // Try to load it back
            match Document::load(&path).await {
                Ok(loaded_doc) => {
                    println!("✓ Encrypted document re-loaded successfully");
                    println!("  - Is encrypted: {}", loaded_doc.is_encrypted());
                    println!("  - Has encryption state: {}", loaded_doc.encryption_state.is_some());
                }
                Err(e) => println!("✗ Failed to re-load encrypted document: {:?}", e),
            }
        }
        Err(e) => println!("✗ Failed to create encryption state: {:?}", e),
    }

    println!();

    // Test 3: Verify object access in encrypted PDF
    println!("Test 3: Verifying object access in encrypted PDF");
    if let Ok(doc) = Document::load("assets/encrypted.pdf").await {
        let mut accessible_objects = 0;
        let mut total_checked = 0;

        for i in 1..=20 {
            total_checked += 1;
            if doc.get_object((i, 0)).is_ok() {
                accessible_objects += 1;
            }
        }

        println!("✓ Object access test completed");
        println!("  - Objects checked: {}", total_checked);
        println!("  - Objects accessible: {}", accessible_objects);
        println!(
            "  - Success rate: {:.1}%",
            (accessible_objects as f64 / total_checked as f64) * 100.0
        );
    }

    println!("\n=== All decryption tests completed ===");
}
