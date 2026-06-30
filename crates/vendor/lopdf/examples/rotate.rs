use lopdf::Document;
use std::str::FromStr;

#[cfg(not(feature = "async"))]
fn main() {
    // Collect command line arguments: input_file angle output_file
    let args: Vec<String> = std::env::args().collect();
    assert!(args.len() == 4, "Not enough arguments: input_file angle output_file");
    let input_file = &args[1];
    let angle = i64::from_str(&args[2]).expect("error in parsing angle argument");
    assert!(angle % 90 == 0, "angle must be a multiple of 90");
    let output_file = &args[3];
    let mut doc = Document::load(input_file).unwrap();

    // Note: this example sets Rotate on each page individually for flexibility,
    //  but you can also set it on any node in the page tree and child pages will
    //  inherit the value.
    for (_, page_id) in doc.get_pages() {
        let page_dict = doc
            .get_object_mut(page_id)
            .and_then(|obj| obj.as_dict_mut())
            .expect("Missing page!");

        // Get the current rotation if any; the default is 0
        let current_rotation = page_dict.get(b"Rotate").and_then(|obj| obj.as_i64()).unwrap_or(0);

        // Add the angle and update
        page_dict.set("Rotate", (current_rotation + angle) % 360);
    }
    // Store file in current working directory.
    doc.save(output_file).unwrap();
}

#[cfg(feature = "async")]
#[tokio::main]
async fn main() {
    // Collect command line arguments: input_file angle output_file
    let args: Vec<String> = std::env::args().collect();
    assert!(args.len() == 4, "Not enough arguments: input_file angle output_file");
    let input_file = &args[1];
    let angle = i64::from_str(&args[2]).expect("error in parsing angle argument");
    assert!(angle % 90 == 0, "angle must be a multiple of 90");
    let output_file = &args[3];
    let mut doc = Document::load(input_file).await.unwrap();

    // Note: this example sets Rotate on each page individually for flexibility,
    //  but you can also set it on any node in the page tree and child pages will
    //  inherit the value.
    for (_, page_id) in doc.get_pages() {
        let page_dict = doc
            .get_object_mut(page_id)
            .and_then(|obj| obj.as_dict_mut())
            .expect("Missing page!");

        // Get the current rotation if any; the default is 0
        let current_rotation = page_dict.get(b"Rotate").and_then(|obj| obj.as_i64()).unwrap_or(0);

        // Add the angle and update
        page_dict.set("Rotate", (current_rotation + angle) % 360);
    }
    // Store file in current working directory.
    doc.save(output_file).unwrap();
}
