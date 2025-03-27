

#[cfg(test)]
use mail_parser::MessageParser;
use super::mime::{AttachmentInfo, AttachmentRecords,ContentSectionRecords};





#[test]
fn eml_utf8() {
    let raw_email = include_str!("test_data/utf8_attachment_names.txt").to_string(); // Load an email file

    // Seems to be a bug in the mail-parser crate.
    // If the Content-Disposition header has a filename= AND a filename*0*= field the fallback field
    // is used as the first component of the reconstructed long filename.

    println!("\nutf8 check");

    let eml_msg = MessageParser::default().parse(&raw_email);

    if let Some(message) = eml_msg {
        let _content_records = ContentSectionRecords::try_from( &message ).expect("Failed to parse message");


        let attachment_records = AttachmentRecords::try_from( &message ).unwrap();

        let extracted_set = attachment_records.attachments;

        let correct_set = vec!(

            //filenames contains \" escape strings
            AttachmentInfo{
                filename: Some("report \"Q1 2024\" (final).pdf".to_string()),
                mime_type: Some("application/pdf".to_string()),
                blob: None
            },

            //filename has an embedded ; char
            AttachmentInfo{
                filename: Some("data; Q1-2024.png".to_string()),
                mime_type: Some("image/png".to_string()),
                blob: None
            },

            //filename is multibyte
            AttachmentInfo{
                filename: Some("résumé.txt".to_string()),
                mime_type: Some("text/plain".to_string()),
                blob: None
            },

            //FIXME: the mail-parser crate has a bug and this test relies on it's broken behaviour.
            AttachmentInfo{
                filename: Some("BROKEN_PARSE_BUG_a_b_c.txt".to_string()),
                mime_type: Some("text/plain".to_string()),
                blob: None
            },

            AttachmentInfo{
                filename: Some("CORRECT_a_b_c.txt".to_string()),
                mime_type: Some("text/plain".to_string()),
                blob: None
            },
        );

        assert_eq!( extracted_set, correct_set, "mismatched sets" );


    };


}


#[test]
fn eml_no_attachments(){
    let raw_email = include_str!("test_data/no_attachments.txt").to_string(); // Load an email file

    println!("\nno attachments");

    let eml_msg = MessageParser::default().parse(&raw_email);

    if let Some(message) = eml_msg {
        let _content_records = ContentSectionRecords::try_from(&message).expect("Failed to parse message");

        let attachment_records = AttachmentRecords::try_from(&message);

        //reconstruct the email headers into a string blob
        let headers_blob: String = message.headers_raw()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect::<Vec<_>>()
            .join("");
        println!("headers blob: [{}]", headers_blob);

        assert!(attachment_records.is_err());

    }
}


#[test]
fn eml_three_attachments(){
    let raw_email = include_str!("test_data/three_attachments.txt").to_string(); // Load an email file

    println!("\nthree attachments");

    let eml_msg = MessageParser::default().parse(&raw_email);

    if let Some(message) = eml_msg {
        let _content_records = ContentSectionRecords::try_from( &message ).expect("Failed to parse message");


        let attachment_records = AttachmentRecords::try_from(&message).unwrap();

        let extracted_set = attachment_records.attachments;
        for rec in &extracted_set {
            let blob = rec.blob.clone().unwrap();

            let blob_str = if let Ok(blob) = String::from_utf8(blob) {
                blob
            } else {
                format!("[{}] Could not decode into utf8 for display", rec.mime_type.clone().expect("mime type not set"))
            };
            println!("-rec: {:?}", &blob_str);
            println!("");
        }


        //let image_bytes = include_bytes!("test_data/Mark - Orange on White.png");
        let image_bytes = include_bytes!("../../../../media/Mark - Orange on White.png");


        let correct_set = vec!(
            AttachmentInfo {
                filename: Some("document.pdf".to_string()),
                mime_type: Some("application/pdf".to_string()),
                blob: Some("Test content encoded as b64".as_bytes().to_vec())
            },
            AttachmentInfo {
                filename: Some("image.png".to_string()),
                mime_type: Some("image/png".to_string()),
                blob: Some(image_bytes.to_vec())
            },
            AttachmentInfo {
                filename: Some("notes.txt".to_string()),
                mime_type: Some("text/plain".to_string()),
                blob: Some("Sample text content in the file.\n".as_bytes().to_vec())
            },
        );

        assert_eq!(extracted_set, correct_set, "mismatched sets");
    }
}


#[test]
fn eml_malformed(){

    // This tests some awkward corner cases for weird headers.

    let raw_email = include_str!("test_data/malformed_headers.txt").to_string(); // Load an email file

    println!("\nmalformed headers");

    let eml_msg = MessageParser::default().parse(&raw_email);

    if let Some(message) = eml_msg {
        //println!("message html_body_count: {}", message.html_body_count());
        let _content_records = ContentSectionRecords::try_from( &message ).unwrap();
        //println!("\ncontent_records = {:#?}", content_records);


        let attachment_records = AttachmentRecords::try_from(&message).unwrap();

        let extracted_set = attachment_records.attachments;
        // for rec in &collated{
        //     println!("-rec: {:?}", rec);
        // }

        let correct_set = vec!(
            AttachmentInfo {
                filename: Some("document.pdf".to_string()),
                mime_type: Some("application/pdf".to_string()),
                blob: None
            },
            AttachmentInfo {
                filename: None,
                mime_type: Some("application/pdf".to_string()),
                blob: None
            },
            // AttachmentInfo{
            //     filename: None,
            //     mime_type: None,
            // },
            AttachmentInfo {
                filename: Some("no_mime_type.png".to_string()),
                mime_type: None,
                blob: None
            },
            AttachmentInfo {
                filename: Some("notes.tx_t".to_string()),
                mime_type: Some("text/plain".to_string()),
                blob: None
            },
        );

        assert_eq!(extracted_set, correct_set, "mismatched sets");
    }
}

