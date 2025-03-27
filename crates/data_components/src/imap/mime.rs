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


use mail_parser::{MimeHeaders, Message, MessageParser};


pub struct MimeExtract{
    pub attachments: Option<Vec<AttachmentInfo>>,
    pub content_sections: Option<Vec<ContentSectionInfo>>,
}


impl From<Option<String>> for MimeExtract {
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
            .unwrap_or_else(|| MimeExtract {
                attachments: None,
                content_sections: None,
            })
    }

}


// try_from seems to be the wrong idiom for a struct that can always return with None as member var values.

// impl TryFrom<&Option<String>> for MimeExtract {
//     type Error = String;
//
//     fn try_from(body: &Option<String>) -> Result<Self, Self::Error> {
//
//         // Attempt to convert email into parsed mime data and extract attachments and content_sections
//         let (attachments, content_sections) =
//             body.as_ref().and_then(|body_raw| {
//                 MessageParser::default().parse(body_raw).map(|eml_msg| {
//                     // We now have a parsed mime msg to work with: eml_msg
//                     // Some(
//                         (
//                             AttachmentRecords::try_from(&eml_msg).ok().map(|r| r.attachments),
//                             ContentSectionRecords::try_from(&eml_msg).ok().map(|r| r.sections),
//                         )
//                     // )
//                 })
//             }).unwrap(); //FIXME: snafu error for failed mime parse
//
//         Ok(
//             MimeExtract{
//             attachments,
//             content_sections,
//             }
//         )
//
//     }
// }



// Attachment extraction code.
// filename, mime-type, blob

#[derive(Debug)]
#[derive(PartialEq)]
pub struct AttachmentInfo {
    pub mime_type: Option<String>,
    pub filename: Option<String>,
    pub blob: Option<Vec<u8>>,
    //FIXME: store charset?
}


#[derive(Debug)]
#[derive(PartialEq)]
pub struct AttachmentRecords{
    pub attachments: Vec<AttachmentInfo>,
}


impl TryFrom<&Message<'_>> for AttachmentRecords{
    type Error = String; //FIXME: upgrade to snafu

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
            Err("No attachments found.".to_string())
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

#[derive(Debug)]
#[derive(PartialEq)]
pub struct ContentSectionInfo {
    pub mime_type: Option<String>,
    pub content: Option<String>,
}


#[derive(Debug)]
#[derive(PartialEq)]
pub struct ContentSectionRecords {
    pub sections: Vec<ContentSectionInfo>,
}


impl TryFrom<&Message<'_>> for ContentSectionRecords {
    type Error = String; //FIXME: upgrade to snafu

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
            Err("No content sections found.".to_string())
        }else{
            Ok(
                ContentSectionRecords {
                    sections: content_records
                }
            )
        }

    }
}



