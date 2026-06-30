use lopdf::{Document, Result};

#[cfg(feature = "async")]
use tokio::runtime::Builder;

#[cfg(not(feature = "async"))]
fn load_document(path: &str) -> Result<Document> {
    Document::load(path)
}

#[cfg(feature = "async")]
fn load_document(path: &str) -> Result<Document> {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move { Document::load(path).await })
}

fn main() -> Result<()> {
    // Load a PDF document
    let mut doc = load_document("example.pdf")?;

    println!("Loaded PDF document");

    // Get all pages
    let pages = doc.get_pages();
    let mut total_replacements = 0;

    // Example 1: Replace "Hello" with "Hi" on all pages
    println!("\n=== Replacing 'Hello' with 'Hi' ===");
    for page_num in pages.keys() {
        match doc.replace_partial_text(*page_num, "Hello", "Hi", Some("?")) {
            Ok(count) => {
                if count > 0 {
                    println!("Page {}: Replaced {} occurrences", page_num, count);
                    total_replacements += count;
                }
            }
            Err(e) => eprintln!("Error on page {}: {}", page_num, e),
        }
    }

    // Example 2: Replace "World" with "Earth" on page 1
    println!("\n=== Replacing 'World' with 'Earth' on page 1 ===");
    match doc.replace_partial_text(1, "World", "Earth", None) {
        Ok(count) => {
            println!("Replaced {} occurrences of 'World' with 'Earth'", count);
            total_replacements += count;
        }
        Err(e) => eprintln!("Error during replacement: {}", e),
    }

    // Example 3: Replace partial text in a specific pattern
    println!("\n=== Replacing email domains ===");
    match doc.replace_partial_text(1, "@example.com", "@company.org", Some("?")) {
        Ok(count) => {
            println!("Replaced {} email domains", count);
            total_replacements += count;
        }
        Err(e) => eprintln!("Error during replacement: {}", e),
    }

    println!("\n=== Summary ===");
    println!("Total replacements made: {}", total_replacements);

    // Save the modified document if any replacements were made
    if total_replacements > 0 {
        doc.save("example_modified.pdf")?;
        println!("Saved modified document to example_modified.pdf");
    } else {
        println!("No replacements were made, document not saved");
    }

    // Example 4: Demonstrating the difference between replace_text and replace_partial_text
    println!("\n=== Comparison with original replace_text ===");

    // This would fail with replace_text because it requires exact match
    let mut doc2 = load_document("example.pdf")?;

    // Original replace_text - needs exact match
    match doc2.replace_text(1, "Hello World!", "Hi Earth!", None) {
        Ok(_) => println!("replace_text: Successfully replaced exact text"),
        Err(e) => println!("replace_text: Failed - {}", e),
    }

    // New replace_partial_text - can replace partial matches
    match doc2.replace_partial_text(1, "Hello", "Hi", None) {
        Ok(count) => println!("replace_partial_text: Replaced {} partial matches", count),
        Err(e) => println!("replace_partial_text: Failed - {}", e),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lopdf::{
        Object, Stream,
        content::{Content, Operation},
        dictionary,
    };

    #[test]
    fn test_partial_replacement() -> Result<()> {
        // Create a test document
        let mut doc = Document::with_version("1.5");

        // Create a simple page with text
        let pages_id = doc.new_object_id();
        let font_id = doc.add_object(dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => "Helvetica",
        });

        let resources_id = doc.add_object(dictionary! {
            "Font" => dictionary! {
                "F1" => font_id,
            },
        });

        let content = Content {
            operations: vec![
                Operation::new("BT", vec![]),
                Operation::new("Tf", vec!["F1".into(), 12.into()]),
                Operation::new("Td", vec![100.into(), 700.into()]),
                Operation::new("Tj", vec![Object::string_literal("Hello World! Hello Universe!")]),
                Operation::new("ET", vec![]),
            ],
        };

        let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode()?));

        let page_id = doc.add_object(dictionary! {
            "Type" => "Page",
            "Parent" => pages_id,
            "Contents" => content_id,
            "Resources" => resources_id,
        });

        doc.objects.insert(
            pages_id,
            Object::Dictionary(dictionary! {
                "Type" => "Pages",
                "Kids" => vec![page_id.into()],
                "Count" => 1,
                "MediaBox" => vec![0.into(), 0.into(), 612.into(), 792.into()],
            }),
        );

        let catalog_id = doc.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });

        doc.trailer.set("Root", catalog_id);

        // Test partial replacement
        let replacements = doc.replace_partial_text(1, "Hello", "Greetings", None)?;
        assert_eq!(replacements, 2); // Should replace both occurrences

        // Extract text to verify
        let text = doc.extract_text(&[1])?;
        assert!(text.contains("Greetings World! Greetings Universe!"));

        Ok(())
    }
}
