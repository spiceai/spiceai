/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


// RFC for MIME
// https://www.rfc-editor.org/rfc/rfc2045
// re: Content-Type headers
// https://www.rfc-editor.org/rfc/rfc2045#page-10

// RFC for Content-Disposition header:
// https://www.rfc-editor.org/rfc/rfc2183
// Some discussion: https://stackoverflow.com/questions/938005/mime-header-content-disposition

// RFC for multibyte filenames (MIME Parameter Value and Encoded Word Extensions:Character Sets, Languages, and Continuations)
// https://www.rfc-editor.org/rfc/rfc2231


use mail_parser::{MessageParser, MimeHeaders};


#[derive(Debug)]
#[derive(PartialEq)]
pub struct AttachmentInfo {
    pub mime_type: Option<String>,
    pub filename: Option<String>,
}


#[derive(Debug)]
#[derive(PartialEq)]
pub struct AttachmentRecords{
    pub attachments: Vec<AttachmentInfo>,
}


impl AttachmentRecords {
    //FIXME: empty impl
}


impl TryFrom<&String> for AttachmentRecords{
    type Error = String;

    fn try_from(raw_email: &String) -> Result<Self, Self::Error> {

        let mut attachments = Vec::<AttachmentInfo>::new();

        if let Some(message) = MessageParser::default().parse(raw_email) {

            for attachment in message.attachments() {

                //extract filename
                let filename = if let Some(attachment_name) = attachment.attachment_name() {
                    Some(attachment_name.to_string())
                }else{
                    None
                };

                //extract mime type
                let mime_type = if let Some(content_type_struct) = attachment.content_type() {
                    //reconstruct the mime type as a string
                    //returns either "{major_type_val}" or "{major_type_val}/{subtype_val}"

                    let major_type_val = content_type_struct.c_type.to_string();

                    let mime_type = match &content_type_struct.c_subtype {
                        Some(subtype_val) => {
                            // concat major type and subtype into a string like "text/plain"
                            format!("{}/{}", major_type_val, subtype_val)
                        }
                        None => {
                            // no subtype: returns eg "text"
                            major_type_val
                        }
                    };

                    Some(mime_type)

                }else{
                    //unable to extract mime type
                    None
                };


                // Deal with None,None case where there's no filename AND no mime_type
                match (filename, mime_type) {
                    (None, None) => {
                        // We have encountered the rare MIME record where there is no filename and no mime_type data.
                        // invalid_attachment_action::warn

                        // FIXME: Is this attachment probably text?
                        // FIXME: Further research?
                        // FIXME: Cite exact RFC passage?
                    },
                    (filename, mime_type) => {
                        // Any other combo of filename and mime_type gives us a useful decode.
                        attachments.push( AttachmentInfo{ filename, mime_type } );
                    }
                };

            } //loop attachments for email

        } //can we parse this email?


        if attachments.is_empty() {
            Err("No attachments found.".to_string())
        }else{

            //sane parse
            Ok(
                AttachmentRecords{
                    attachments
                }
            )

        }

    } //try_from(..)

} // impl ... AttachmentRecords





#[test]
fn eml_utf8() {
    let raw_email = include_str!("test_data/utf8_attachment_names.txt").to_string(); // Load an email file

    // Seems to be a bug in the mail-parser crate.
    // If the Content-Disposition header has a filename= AND a filename*0*= field the fallback field
    // is used as the first component of the reconstructed long filename.

    // println!("\nutf8 check");
    let attachment_records = AttachmentRecords::try_from( &raw_email ).unwrap();

    let extracted_set = attachment_records.attachments;
    // for rec in &collated{
    //     println!("-rec: {:?}", rec);
    // }

    let correct_set = vec!(

        //filenames contains \" escape strings
        AttachmentInfo{
            filename: Some("report \"Q1 2024\" (final).pdf".to_string()),
            mime_type: Some("application/pdf".to_string()),
        },

        //filename has an embedded ; char
        AttachmentInfo{
            filename: Some("data; Q1-2024.png".to_string()),
            mime_type: Some("image/png".to_string()),
        },

        //filename is multibyte
        AttachmentInfo{
            filename: Some("résumé.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
        },

        //FIXME: the mail-parser crate has a bug and this test relies on it's broken behaviour.
        AttachmentInfo{
            filename: Some("BROKEN_PARSE_BUG_a_b_c.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
        },

        AttachmentInfo{
            filename: Some("CORRECT_a_b_c.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
        },
    );

    assert_eq!( extracted_set, correct_set, "mismatched sets" );

}


#[test]
fn eml_no_attachments(){
    let raw_email = include_str!("test_data/no_attachments.txt").to_string(); // Load an email file

    // println!("\nno attachments");
    let attachment_records = AttachmentRecords::try_from( &raw_email );

    assert!( attachment_records.is_err() );

}


#[test]
fn eml_three_attachments(){
    let raw_email = include_str!("test_data/three_attachments.txt").to_string(); // Load an email file

    // println!("\nthree attachments");
    let attachment_records = AttachmentRecords::try_from( &raw_email ).unwrap();

    let extracted_set = attachment_records.attachments;
    // for rec in &collated{
    //     println!("-rec: {:?}", rec);
    // }

    let correct_set = vec!(
        AttachmentInfo{
            filename: Some("document.pdf".to_string()),
            mime_type: Some("application/pdf".to_string()),
        },
        AttachmentInfo{
            filename: Some("image.png".to_string()),
            mime_type: Some("image/png".to_string()),
        },
        AttachmentInfo{
            filename: Some("notes.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
        },
    );

    assert_eq!( extracted_set, correct_set, "mismatched sets" );

}


#[test]
fn eml_malformed(){

    // This tests some awkward corner cases for weird headers.

    let raw_email = include_str!("test_data/malformed_headers.txt").to_string(); // Load an email file

    //println!("\nmalformed headers");
    let attachment_records = AttachmentRecords::try_from( &raw_email ).unwrap();

    let extracted_set = attachment_records.attachments;
    // for rec in &collated{
    //     println!("-rec: {:?}", rec);
    // }

    let correct_set = vec!(
        AttachmentInfo{
            filename: Some("document.pdf".to_string()),
            mime_type: Some("application/pdf".to_string()),
        },
        AttachmentInfo{
            filename: None,
            mime_type: Some("application/pdf".to_string()),
        },
        // AttachmentInfo{
        //     filename: None,
        //     mime_type: None,
        // },
        AttachmentInfo{
            filename: Some("no_mime_type.png".to_string()),
            mime_type: None,
        },
        AttachmentInfo{
            filename: Some("notes.tx_t".to_string()),
            mime_type: Some("text/plain".to_string()),
        },
    );

    assert_eq!( extracted_set, correct_set, "mismatched sets" );

}




//
// #[test]
// fn eml_integration_tuple(){
//     let raw_email = include_str!("test_data/three_attachments.txt").to_string(); // Load an email file
//
//     //println!("\nintegration tuple");
//     let attachment_records = AttachmentRecords::try_from( &raw_email ).unwrap();
//
//     let (filenames,mime_types) = attachment_records.into_tuple();
//
//     let correct_set_filenames = Some(vec!(
//         Some(String::from("document.pdf")),
//         Some(String::from("image.png")),
//         Some(String::from("notes.txt")),
//     ));
//     assert_eq!( filenames, correct_set_filenames, "filename set mismatch" );
//
//     let correct_set_mime_types = Some(vec!(
//         Some(String::from("application/pdf")),
//         Some(String::from("image/png")),
//         Some(String::from("text/plain")),
//     ));
//     assert_eq!( mime_types, correct_set_mime_types, "mime_type set mismatch" );
//
// }


