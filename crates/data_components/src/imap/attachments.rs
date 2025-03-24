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


use mail_parser::{MimeHeaders, Message};


#[derive(Debug)]
#[derive(PartialEq)]
pub struct AttachmentInfo {
    pub mime_type: Option<String>,
    pub filename: Option<String>,
    pub blob: Option<Vec<u8>>,
}


#[derive(Debug)]
#[derive(PartialEq)]
pub struct AttachmentRecords{
    pub attachments: Vec<AttachmentInfo>,
}



// This is not a useful pattern because we down-convert into a pure Vec before we call
// for build_recordbatch
// impl Into<ListArray> for AttachmentRecords {
//     fn into(self) -> ListArray {
//     }
// }

//
// pub struct AttachmentParseOptions{
//     pub raw_email: String,
//     pub store_blobs: bool, //spend cpu decoding the attachments from base64/other
// }



// impl TryFrom<AttachmentParseOptions> for AttachmentRecords {
//     type Error = String;

impl AttachmentRecords{
    pub fn try_from(eml_msg: &Message, store_blobs: bool) -> Result<Self, String> {

        let mut attachments = Vec::<AttachmentInfo>::new();

        //if let Some(message) = MessageParser::default().parse(raw_email) {
        let message = eml_msg;

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


            //attachments are decoded automatically by mail_parse
            //encoding type is available. not sure if encoded bytes are.
            let attachment_blob = if store_blobs {
                Some(
                    attachment.contents().to_vec()
                )

            } else {
                None
            };


            // Deal with None,None case where there's no filename AND no mime_type
            match (filename, mime_type) {
                (None, None) => {
                    // We have encountered the rare MIME record where there is no filename and no mime_type data.

                    //FIXME: invalid_attachment_action::warn ?

                    //FIXME: Is this attachment probably text?
                    // Further research?
                    // Cite exact RFC passage?
                },
                (filename, mime_type) => {
                    // Any other combo of filename and mime_type gives us a useful decode.
                    attachments.push( AttachmentInfo{ filename, mime_type, blob:attachment_blob } );
                }
            };

        } //loop attachments for email

        //} //can we parse this email?


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








