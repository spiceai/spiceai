// if you use nightly then you can enable this feature to gain a boost in read speed of PDF's"
//#![feature(extend_one)]

#[macro_use]
extern crate lopdf;

use std::collections::BTreeMap;

use lopdf::content::{Content, Operation};
use lopdf::{Bookmark, Document, Object, ObjectId, Stream};

pub fn generate_fake_document() -> Document {
    let mut doc = Document::with_version("1.5");
    let pages_id = doc.new_object_id();
    let font_id = doc.add_object(dictionary! {
        "Type" => "Font",
        "Subtype" => "Type1",
        "BaseFont" => "Courier",
    });
    let resources_id = doc.add_object(dictionary! {
        "Font" => dictionary! {
            "F1" => font_id,
        },
    });
    let content = Content {
        operations: vec![
            Operation::new("BT", vec![]),
            Operation::new("Tf", vec!["F1".into(), 48.into()]),
            Operation::new("Td", vec![100.into(), 600.into()]),
            Operation::new("Tj", vec![Object::string_literal("Hello World!")]),
            Operation::new("ET", vec![]),
        ],
    };
    let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
    let page_id = doc.add_object(dictionary! {
        "Type" => "Page",
        "Parent" => pages_id,
        "Contents" => content_id,
        "Resources" => resources_id,
        "MediaBox" => vec![0.into(), 0.into(), 595.into(), 842.into()],
    });
    let pages = dictionary! {
        "Type" => "Pages",
        "Kids" => vec![page_id.into()],
        "Count" => 1,
    };
    doc.objects.insert(pages_id, Object::Dictionary(pages));
    let catalog_id = doc.add_object(dictionary! {
        "Type" => "Catalog",
        "Pages" => pages_id,
    });
    doc.trailer.set("Root", catalog_id);

    doc
}

fn main() {
    // Generate a stack of Documents to merge
    // (The Bookmark layer,  Document to merge)
    let documents = vec![
        (1u32, generate_fake_document()),
        (2u32, generate_fake_document()),
        (2u32, generate_fake_document()),
        (3u32, generate_fake_document()),
    ];

    // We use this to keep track of the last Parent per layer depth.
    let mut layer_parent: [Option<u32>; 4] = [None; 4];

    // This is the last layer ran.
    let mut last_layer = 0;

    // Define a starting max_id (will be used as start index for object_ids)
    let mut max_id = 1;
    let mut pagenum = 1;
    // Collect all Documents Objects grouped by a map
    let mut documents_pages = BTreeMap::new();
    let mut documents_objects = BTreeMap::new();
    let mut document = Document::with_version("1.5");

    // Lets try to set these to be bigger to avoid multi allocations for faster handling of files.
    // We are just saying each Document it about 1000 objects in size. can be adjusted for better speeds.
    // This can only be used if you use nightly or the #![feature(extend_one)] is stablized.
    // documents_pages.extend_reserve(documents.len() * 1000);
    // documents_objects.extend_reserve(documents.len() * 1000);

    // Add a Table of Contents
    // We set the object page to (0,0) which means it will point to the first object after it.
    layer_parent[0] = Some(document.add_bookmark(
        Bookmark::new("Table of Contents".to_string(), [0.0, 0.0, 0.0], 0, (0, 0)),
        None,
    ));

    // Can set bookmark formatting and color per report bookmark added.
    // Formating is 1 for italic 2 for bold 3 for bold and italic
    // Color is RGB 0.0..255.0
    for (layer, mut doc) in documents {
        let color = [0.0, 0.0, 0.0];
        let format = 0;
        let mut display = String::new();

        doc.renumber_objects_with(max_id);

        max_id = doc.max_id + 1;

        let mut first_object = None;

        let pages = doc.get_pages();

        // This is actually better than extend as we use less allocations and cloning then.
        pages
            .into_values()
            .map(|object_id| {
                // We use this as the return object for Bookmarking to deturmine what it points too.
                // We only want to do this for the first page though.
                if first_object.is_none() {
                    first_object = Some(object_id);
                    display = format!("Page {}", pagenum);
                    pagenum += 1;
                }

                (object_id, doc.get_object(object_id).unwrap().to_owned())
            })
            .for_each(|(key, value)| {
                documents_pages.insert(key, value);
            });

        documents_objects.extend(doc.objects);

        // Lets shadow our pointer back if nothing then set to (0,0) tto point to the next page
        let object = first_object.unwrap_or((0, 0));

        // This will use the layering to implement children under Parents in the bookmarks
        // Example as we are generating it here.
        // Table of Contents
        // - Page 1
        // -- Page 2
        // -- Page 3
        // --- Page 4

        if layer == 0 {
            layer_parent[0] = Some(document.add_bookmark(Bookmark::new(display, color, format, object), None));
            last_layer = 0;
        } else if layer == 1 {
            layer_parent[1] =
                Some(document.add_bookmark(Bookmark::new(display, color, format, object), layer_parent[0]));
            last_layer = 1;
        } else if last_layer >= layer || last_layer == layer - 1 {
            layer_parent[layer as usize] = Some(document.add_bookmark(
                Bookmark::new(display, color, format, object),
                layer_parent[(layer - 1) as usize],
            ));
            last_layer = layer;
        } else if last_layer > 0 {
            layer_parent[last_layer as usize] = Some(document.add_bookmark(
                Bookmark::new(display, color, format, object),
                layer_parent[(last_layer - 1) as usize],
            ));
        } else {
            layer_parent[1] =
                Some(document.add_bookmark(Bookmark::new(display, color, format, object), layer_parent[0]));
            last_layer = 1;
        }
    }

    // Catalog and Pages are mandatory
    let mut catalog_object: Option<(ObjectId, Object)> = None;
    let mut pages_object: Option<(ObjectId, Object)> = None;

    // Process all objects except "Page" type
    for (object_id, object) in documents_objects.into_iter() {
        // We have to ignore "Page" (as are processed later), "Outlines" and "Outline" objects
        // All other objects should be collected and inserted into the main Document
        match object.type_name().unwrap_or(b"") {
            b"Catalog" => {
                // Collect a first "Catalog" object and use it for the future "Pages"
                catalog_object = Some((
                    if let Some((id, _)) = catalog_object {
                        id
                    } else {
                        object_id
                    },
                    object,
                ));
            }
            b"Pages" => {
                // Collect and update a first "Pages" object and use it for the future "Catalog"
                // We have also to merge all dictionaries of the old and the new "Pages" object
                if let Ok(dictionary) = object.as_dict() {
                    let mut dictionary = dictionary.clone();
                    if let Some((_, ref object)) = pages_object
                        && let Ok(old_dictionary) = object.as_dict()
                    {
                        dictionary.extend(old_dictionary);
                    }

                    pages_object = Some((
                        if let Some((id, _)) = pages_object {
                            id
                        } else {
                            object_id
                        },
                        Object::Dictionary(dictionary),
                    ));
                }
            }
            b"Page" => {}     // Ignored, processed later and separately
            b"Outlines" => {} // Ignored, not supported yet
            b"Outline" => {}  // Ignored, not supported yet
            _ => {
                document.objects.insert(object_id, object);
            }
        }
    }

    // If no "Pages" found abort
    if pages_object.is_none() {
        println!("Pages root not found.");

        return;
    }

    // Iter over all "Page" and collect with the parent "Pages" created before
    for (object_id, object) in documents_pages.iter() {
        if let Ok(dictionary) = object.as_dict() {
            let mut dictionary = dictionary.clone();
            dictionary.set("Parent", pages_object.as_ref().unwrap().0);

            document.objects.insert(*object_id, Object::Dictionary(dictionary));
        }
    }

    // If no "Catalog" found abort
    if catalog_object.is_none() {
        println!("Catalog root not found.");

        return;
    }

    let (catalog_id, catalog_object) = catalog_object.unwrap();
    let (page_id, page_object) = pages_object.unwrap();

    // Build a new "Pages" with updated fields
    if let Ok(dictionary) = page_object.as_dict() {
        let mut dictionary = dictionary.clone();

        // Set new pages count
        dictionary.set("Count", documents_pages.len() as u32);

        // Set new "Kids" list (collected from documents pages) for "Pages"
        dictionary.set(
            "Kids",
            documents_pages.into_keys().map(Object::Reference).collect::<Vec<_>>(),
        );

        document.objects.insert(page_id, Object::Dictionary(dictionary));
    }

    // Build a new "Catalog" with updated fields
    if let Ok(dictionary) = catalog_object.as_dict() {
        let mut dictionary = dictionary.clone();
        dictionary.set("Pages", page_id);
        dictionary.set("PageMode", "UseOutlines");
        dictionary.remove(b"Outlines"); // Outlines not supported in merged PDFs

        document.objects.insert(catalog_id, Object::Dictionary(dictionary));
    }

    document.trailer.set("Root", catalog_id);

    // Update the max internal ID as wasn't updated before due to direct objects insertion
    document.max_id = document.objects.len() as u32;

    // Reorder all new Document objects
    document.renumber_objects();

    //Set any Bookmarks to the First child if they are not set to a page
    document.adjust_zero_pages();

    //Set all bookmarks to the PDF Object tree then set the Outlines to the Bookmark content map.
    if let Some(outline_id) = document.build_outline()
        && let Ok(Object::Dictionary(dict)) = document.get_object_mut(catalog_id)
    {
        dict.set("Outlines", Object::Reference(outline_id));
    }

    // Most of the time this does nothing unless there are a lot of streams
    // Can be disabled to speed up the process.
    // document.compress();

    // Save the merged PDF
    // Store file in current working directory.
    document.save("merged.pdf").unwrap();
}
