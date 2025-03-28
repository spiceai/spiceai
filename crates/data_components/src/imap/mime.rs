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
// https://www.rfc-editor.org/rfc/rfc2046
// re: Content-Type headers
// https://www.rfc-editor.org/rfc/rfc2045#page-10

// RFC for Content-Disposition header:
// https://www.rfc-editor.org/rfc/rfc2183
// Some discussion: https://stackoverflow.com/questions/938005/mime-header-content-disposition

// RFC for multibyte filenames (MIME Parameter Value and Encoded Word Extensions:Character Sets, Languages, and Continuations)
// https://www.rfc-editor.org/rfc/rfc2231


use snafu::Snafu;
use mail_parser::{MimeHeaders, Message, MessageParser};


pub struct MimeExtract{
    pub attachments: Option<Vec<AttachmentInfo>>,
    pub content_sections: Option<Vec<ContentSectionInfo>>,
}


impl From<&Option<String>> for MimeExtract {
    fn from(body: &Option<String>) -> Self {
        // worst case return value is {attachments: None, content_sections: None}
        // schema and build_recordbatch() code are setup to handle these gracefully.

        // No error info is available from parse(body) regarding MIME decode.

        body.as_ref()
            .and_then(|body| {
                MessageParser::default().parse(body).map(|eml_msg| {
                    // We now have a parsed mime msg to work with: eml_msg
                    let attachments = AttachmentRecords::try_from(&eml_msg).ok().map(|r| r.attachments);
                    let content_sections = ContentSectionRecords::try_from(&eml_msg).ok().map(|r| r.sections);
                    MimeExtract {
                        attachments,
                        content_sections,
                    }
                })
            })
            .unwrap_or(MimeExtract {
                attachments: None,
                content_sections: None,
            })
    }

}



//FIXME: Apparently overload/shadow of "Error" is normal Rust idiomatic code.
// not sure, went with this for now.
#[derive(Debug, Snafu)]
pub enum MimeExtractError{
    #[snafu(display("No attachments found."))]
    AttachmentsNotFound,

    #[snafu(display("No content sections found."))]
    ContentSectionsNotFound,
}




// Attachment extraction code.
// filename, mime-type, blob

#[derive(Debug, PartialEq)]
pub struct AttachmentInfo {
    pub mime_type: Option<String>,
    pub filename: Option<String>,
    pub blob: Option<Vec<u8>>,
    //FIXME: store charset?
}


#[derive(Debug, PartialEq)]
pub struct AttachmentRecords{
    pub attachments: Vec<AttachmentInfo>,
}



impl TryFrom<&Message<'_>> for AttachmentRecords{
    type Error = MimeExtractError;

    fn try_from(message: &Message) -> Result<Self, Self::Error> {

        let mut attachments = Vec::<AttachmentInfo>::new();

        for attachment in message.attachments() {

            //extract filename
            //let filename = attachment.attachment_name().map(|attachment_name| attachment_name.to_string());
            let filename = attachment.attachment_name().map(ToString::to_string);


            //extract mime type
            let mime_type = if let Some(content_type_struct) = attachment.content_type() {
                //reconstruct the mime type as a string
                //returns either "{major_type_val}" or "{major_type_val}/{subtype_val}"

                let major_type_val = content_type_struct.c_type.to_string();

                let mime_type = match &content_type_struct.c_subtype {
                    Some(subtype_val) => {
                        // concat major type and subtype into a string like "text/plain"
                        format!("{major_type_val}/{subtype_val}")
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


            //attachments are decoded automatically by mail_parse
            //encoding type is available. not sure if encoded bytes(base64/etc) are.
            let attachment_blob =
                if attachment.contents().is_empty() {
                    None
                }else{
                    Some( attachment.contents().to_vec() )
                };


            // Deal with None,None case where there's no filename AND no mime_type
            match (filename, mime_type) {
                (None, None) => {
                    // We have encountered the rare MIME record where there is no filename and no mime_type data.

                    //FIXME: invalid_attachment_action::warn ?

                    attachments.push( AttachmentInfo{
                        filename: None,
                        mime_type: Some("application/octet-stream".to_string()), //mime_type,
                        blob:attachment_blob } );

                },
                (filename, mime_type) => {
                    // Any other combo of filename and mime_type gives us a useful decode.
                    attachments.push( AttachmentInfo{ filename, mime_type, blob:attachment_blob } );
                }
            };

        } //loop attachments for email


        if attachments.is_empty() {
            Err(MimeExtractError::AttachmentsNotFound)
        }else{
            //sane parse
            Ok(
                AttachmentRecords{
                    attachments
                }
            )
        }
        
    }
    
}




// Content Section code.
// Extracts the user-relevant content in text/plain and text/html sections.

#[derive(Debug, PartialEq)]
pub struct ContentSectionInfo {
    pub mime_type: Option<String>,
    pub content: Option<String>,
}


#[derive(Debug, PartialEq)]
pub struct ContentSectionRecords {
    pub sections: Vec<ContentSectionInfo>,
}


impl TryFrom<&Message<'_>> for ContentSectionRecords {
    type Error = MimeExtractError;

    fn try_from( eml_msg: &Message ) -> Result<ContentSectionRecords, Self::Error> {

        let mut content_records = Vec::<ContentSectionInfo>::new();

        for rec in eml_msg.html_bodies() {
            content_records.push(
                ContentSectionInfo {
                    mime_type: Some("text/html".to_string()),
                    content: Some(rec.to_string()),
                }
            );
        }

        for rec in eml_msg.text_bodies() {
            content_records.push(
                ContentSectionInfo {
                    mime_type: Some("text/plain".to_string()),
                    content: Some(rec.to_string()),
                }
            );
        }


        if content_records.is_empty() {
            Err(MimeExtractError::ContentSectionsNotFound)
        }else{
            Ok(
                ContentSectionRecords {
                    sections: content_records
                }
            )
        }

    }
}




#[cfg(test)]
mod tests{
    mod mime_extract{
        use mail_parser::MessageParser;
        use crate::imap::mime::{AttachmentInfo, AttachmentRecords, ContentSectionRecords};

        #[test]
        fn test_eml_utf8() {
            let raw_email = include_str!("test_data/utf8_attachment_names.txt").to_string(); // Load an email file

            // Seems to be a bug in the mail-parser crate.
            // If the Content-Disposition header has a filename= AND a filename*0*= field the fallback field
            // is used as the first component of the reconstructed long filename.

            println!("\nutf8 check");

            let eml_msg = MessageParser::default().parse(&raw_email);

            if let Some(message) = eml_msg {
                let _content_records = ContentSectionRecords::try_from( &message ).expect("Failed to parse message");


                let attachment_records = AttachmentRecords::try_from( &message ).expect("Failed to parse attachments");

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
        fn test_eml_no_attachments(){
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
        fn test_eml_three_attachments(){
            let raw_email = include_str!("test_data/three_attachments.txt").to_string(); // Load an email file

            println!("\nthree attachments");

            let eml_msg = MessageParser::default().parse(&raw_email);

            if let Some(message) = eml_msg {
                let _content_records = ContentSectionRecords::try_from( &message ).expect("Failed to parse message");


                let attachment_records = AttachmentRecords::try_from(&message).expect("Failed to parse attachments");

                let extracted_set = attachment_records.attachments;
                for rec in &extracted_set {
                    let blob = rec.blob.clone().expect("blob not set");

                    let blob_str = if let Ok(blob) = String::from_utf8(blob) {
                        blob
                    } else {
                        format!("[{}] Could not decode into utf8 for display", rec.mime_type.clone().expect("mime type not set"))
                    };
                    println!("-rec: {:?}", &blob_str);
                    println!();
                }


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
        fn test_eml_malformed(){

            // This tests some awkward corner cases for weird headers.

            let raw_email = include_str!("test_data/malformed_headers.txt").to_string(); // Load an email file

            println!("\nmalformed headers");

            let eml_msg = MessageParser::default().parse(&raw_email);

            if let Some(message) = eml_msg {
                let _content_records = ContentSectionRecords::try_from(&message).expect("Failed to parse message");


                let attachment_records = AttachmentRecords::try_from(&message).expect("Failed to parse attachments");

                let extracted_set = attachment_records.attachments;

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
                    AttachmentInfo{
                        filename: None,
                        mime_type: Some("application/octet-stream".to_string()), // No filename or mime-type was detected. Default assigned.
                        blob: None
                    },
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

    }
}


