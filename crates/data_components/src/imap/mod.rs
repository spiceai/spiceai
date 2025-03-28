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

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BinaryBuilder, Date64Array, ListArray, ListBuilder, RecordBatch, StringArray, StringBuilder, StructBuilder,
    },
    datatypes::{DataType, Field, Fields},
    error::ArrowError,
};
use datafusion::{catalog::TableProvider, error::DataFusionError};
use session::ImapSession;
use snafu::prelude::*;

use mime::{AttachmentInfo,ContentSectionInfo};

pub mod provider;
pub mod session;

mod mime;

use Error::FailedToBuildListArrayForAttachments;
use crate::imap::Error::FailedToBuildListArrayForContentSections;


#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error fetching messages: {source}"))]
    FetchMessages { source: imap::Error },
    #[snafu(display("Error examining mailbox: {source}"))]
    ExamineMailbox { source: imap::Error },
    #[snafu(display("Error getting mailbox status: {source}"))]
    GetMailboxStatus { source: imap::Error },
    #[snafu(display("Could not find message at index"))]
    MessageNotFound,
    #[snafu(display("Could not find envelope segment: {segment}"))]
    EnvelopeNotFound { segment: String },
    #[snafu(display("Could not find header"))]
    HeaderNotFound,
    #[snafu(display("Failed to parse header: {source}"))]
    FailedToParseHeader { source: mailparse::MailParseError },
    #[snafu(display(
        "Failed to login.\nVerify the username and password, and try again.\n{source}"
    ))]
    FailedToLogin { source: imap::Error },
    #[snafu(display("Failed to connect: {source}"))]
    FailedToConnect { source: imap::Error },
    #[snafu(display("Failed to logout: {source}"))]
    FailedToLogout { source: imap::Error },
    #[snafu(display("An invalid SSL mode was provided: {ssl_mode}"))]
    InvalidSSLMode { ssl_mode: String },
    #[snafu(display("An invalid authentication mode was provided: {auth_mode}"))]
    InvalidAuthMode { auth_mode: String },


    //FIXME: Not happy with error descriptions.
    #[snafu(display("Should return a field builder: {field_number}/{field_name}"))]
    FailedToBuildListArrayForAttachments { field_number: i32, field_name: String },

    #[snafu(display("Should return a field builder: {field_number}/{field_name}"))]
    FailedToBuildListArrayForContentSections { field_number: i32, field_name: String },

}

#[derive(Debug)]
pub struct ImapTableProvider {
    session: ImapSession,
    fetch_content: bool,
}

fn build_listarray_for_strings(values: Vec<Option<Vec<Option<String>>>>) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for value in values {
        builder.append_option(value);
    }

    builder.finish()
}


//FIXME: I don't really like the overload/shadow of Result here but GPT told me it was an accepted Rust idiom.
pub type Result<T, E = Error> = std::result::Result<T, E>;


fn build_listarray_for_content_sections(values: Vec<Option<Vec<ContentSectionInfo>>>) -> Result<ListArray> {

    let struct_builder = StructBuilder::new(
        Fields::from(vec![
            Field::new("content", DataType::Utf8, true),
            Field::new("mime_type", DataType::Utf8, true),
        ]),
        vec![Box::new(StringBuilder::new()), Box::new(StringBuilder::new())],
    );

    let mut list_builder = ListBuilder::new(struct_builder);

    for attachment_list in values {
        if let Some(attachments) = attachment_list {
            for content_section in attachments {
                let struct_builder = list_builder.values();

                let content_field_builder = struct_builder
                    .field_builder::<StringBuilder>(0)
                    .ok_or( FailedToBuildListArrayForContentSections { field_number: 0, field_name: String::from("content_section") })?;
                content_field_builder.append_option(content_section.content.as_deref());

                let mime_type_field_builder = struct_builder
                    .field_builder::<StringBuilder>(1)
                    .ok_or( FailedToBuildListArrayForContentSections { field_number: 1, field_name: String::from("mime_type") })?;
                mime_type_field_builder.append_option(content_section.mime_type.as_deref());

                struct_builder.append(true);
            }
            list_builder.append(true);
        } else {
            list_builder.append(false);
        }
    }

    Ok(list_builder.finish())
}



fn build_listarray_for_attachments(values: Vec<Option<Vec<AttachmentInfo>>>) -> Result<ListArray> {

    let struct_builder = StructBuilder::new(
        Fields::from(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("mime_type", DataType::Utf8, true),
            Field::new("blob", DataType::Binary, true),
        ]),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(StringBuilder::new()),
            Box::new(BinaryBuilder::new())
        ],
    );

    let mut list_builder = ListBuilder::new(struct_builder);

    for attachment_list in values {
        if let Some(attachments) = attachment_list {
            for attachment in attachments {
                let struct_builder = list_builder.values();

                let filename_field_builder = struct_builder
                    .field_builder::<StringBuilder>(0)
                    .ok_or( FailedToBuildListArrayForAttachments { field_number: 0, field_name:"filename".to_string() } )?;
                filename_field_builder.append_option(attachment.filename.as_deref());

                let mime_type_field_builder = struct_builder
                    .field_builder::<StringBuilder>(1)
                    .ok_or( FailedToBuildListArrayForAttachments { field_number: 1, field_name:"mime_type".to_string() } )?;
                mime_type_field_builder.append_option(attachment.mime_type.as_deref());

                let blob_field_builder = struct_builder
                    .field_builder::<BinaryBuilder>(2)
                    .ok_or( FailedToBuildListArrayForAttachments { field_number: 2, field_name:"blob".to_string() } )?;
                blob_field_builder.append_option(attachment.blob.as_deref());

                struct_builder.append(true);
            }
            list_builder.append(true);
        } else {
            list_builder.append(false);
        }
    }

    Ok(list_builder.finish())
}


impl ImapTableProvider {
    #[must_use]
    pub fn new(session: ImapSession, fetch_content: bool) -> Self {
        Self {
            session,
            fetch_content,
        }
    }

    pub(crate) fn build_recordbatch(
        &self,
        messages: Vec<EmailMessage>,
    ) -> Result<RecordBatch, ArrowError> {
        let mut dates = vec![];
        let mut subjects = vec![];
        let mut froms = vec![];
        let mut tos = vec![];
        let mut ccs = vec![];
        let mut bccs = vec![];
        let mut reply_tos = vec![];
        let mut message_ids = vec![];
        let mut in_reply_tos = vec![];
        let mut headers = vec![];
        let mut bodies = vec![];
        let mut attachments = vec![];
        let mut content_sections = vec![];

        for message in messages {
            dates.push(message.date);
            subjects.push(message.subject);
            froms.push(message.from);
            tos.push(message.to);
            ccs.push(message.cc);
            bccs.push(message.bcc);
            reply_tos.push(message.reply_to);
            message_ids.push(message.message_id);
            in_reply_tos.push(message.in_reply_to);
            headers.push(message.header);

            bodies.push(message.body);
            attachments.push(message.attachments);
            content_sections.push(message.content_sections);
        }

        let mut fields: Vec<ArrayRef> = vec![
            Arc::new(Date64Array::from(dates)),
            Arc::new(StringArray::from(subjects)),
            Arc::new(build_listarray_for_strings(froms)),
            Arc::new(build_listarray_for_strings(tos)),
            Arc::new(build_listarray_for_strings(ccs)),
            Arc::new(build_listarray_for_strings(bccs)),
            Arc::new(build_listarray_for_strings(reply_tos)),
            Arc::new(StringArray::from(message_ids)),
            Arc::new(StringArray::from(in_reply_tos)),
            Arc::new(StringArray::from(headers)),
        ];

        if self.fetch_content {
            fields.push(Arc::new(StringArray::from(bodies))); // field name mismatch. "content" in schema

            let list_array_content_sections = build_listarray_for_content_sections(content_sections)
                .map_err(|e| ArrowError::ComputeError(format!("Failed to build ListArray: {e}")))?;
            fields.push(Arc::new(list_array_content_sections));

            let list_array_attachments = build_listarray_for_attachments(attachments)
                .map_err(|e| ArrowError::ComputeError(format!("Failed to build ListArray: {e}")))?;
            fields.push(Arc::new(list_array_attachments));
        };

        RecordBatch::try_new(self.schema(), fields)
    }
}

impl From<Error> for DataFusionError {
    fn from(error: Error) -> Self {
        DataFusionError::Execution(error.to_string())
    }
}

pub(crate) struct EmailMessage {
    date: i64,
    subject: Option<String>,
    from: Option<Vec<Option<String>>>,
    to: Option<Vec<Option<String>>>,
    cc: Option<Vec<Option<String>>>,
    bcc: Option<Vec<Option<String>>>,
    reply_to: Option<Vec<Option<String>>>,
    message_id: Option<String>,
    in_reply_to: Option<String>,
    header: Option<String>,

    body: Option<String>,
    attachments: Option<Vec<AttachmentInfo>>,
    content_sections: Option<Vec<ContentSectionInfo>>,
}






#[cfg(test)]
mod tests {
    mod build_listarray{
        use super::super::*;
        use arrow::array::{Array, ListArray, StructArray, StringArray, BinaryArray};
        use arrow::datatypes::{DataType, Field};


        #[test]
        fn test_build_listarray_for_content_sections() {
            // This fn was generated by o3-mini-high

            // Prepare test input:
            // - First element: a list with two content sections.
            // - Second element: None.
            // - Third element: an empty list.
            let values = vec![
                Some(vec![
                    ContentSectionInfo {
                        content: Some("Hello".to_string()),
                        mime_type: Some("text/plain".to_string()),
                    },
                    ContentSectionInfo {
                        content: Some("World".to_string()),
                        mime_type: None,
                    },
                ]),
                None,
                Some(vec![]),
            ];

            // Call your function.
            let list_array = build_listarray_for_content_sections(values)
                .expect("Failed to build ListArray");

            // Verify the outer ListArray has three entries.
            assert_eq!(list_array.len(), 3);
            // The second element should be null.
            assert!(!list_array.is_valid(1));
            // The first and third elements should be valid.
            assert!(list_array.is_valid(0));
            assert!(list_array.is_valid(2));

            // Get the offsets into the inner array.
            let offsets = list_array.value_offsets();
            // For the first element, we expect two inner records.
            assert_eq!(offsets[0], 0);
            assert_eq!(offsets[1], 2);
            // The third element is an empty list.
            assert_eq!(offsets[2], offsets[3]);

            // Retrieve the inner StructArray.
            let struct_array = list_array.values()
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected a StructArray");

            // Get the "content" and "mime_type" fields from the struct.
            let content_array = struct_array.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected a StringArray for 'content'");

            let mime_type_array = struct_array.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected a StringArray for 'mime_type'");

            // For the first element (records 0 and 1 in the inner array):
            // Record 0: content = "Hello", mime_type = "text/plain"
            assert_eq!(content_array.value(0), "Hello");
            assert!(mime_type_array.is_valid(0));
            assert_eq!(mime_type_array.value(0), "text/plain");

            // Record 1: content = "World", mime_type should be null.
            assert_eq!(content_array.value(1), "World");
            assert!(!mime_type_array.is_valid(1));
        }




        #[test]
        fn test_build_listarray_for_attachments() {
            // This fn was generated by o3-mini-high

            // Test input:
            // - First element: Some(vec![two attachments])
            // - Second element: None
            // - Third element: Some(empty vector)
            let values = vec![
                Some(vec![
                    AttachmentInfo {
                        filename: Some("file1.txt".to_string()),
                        mime_type: Some("text/plain".to_string()),
                        blob: Some(vec![1, 2, 3]),
                    },
                    AttachmentInfo {
                        filename: Some("file2.jpg".to_string()),
                        mime_type: Some("image/jpeg".to_string()),
                        blob: None,
                    },
                ]),
                None,
                Some(vec![]),
            ];

            let list_array = build_listarray_for_attachments(values)
                .expect("Failed to build ListArray for attachments");

            // Verify outer ListArray has three entries.
            assert_eq!(list_array.len(), 3);
            assert!(list_array.is_valid(0));
            assert!(!list_array.is_valid(1)); // second element is null.
            assert!(list_array.is_valid(2));

            // Verify offsets in the inner array.
            let offsets = list_array.value_offsets();
            // First element should yield two inner records.
            assert_eq!(offsets[0], 0);
            assert_eq!(offsets[1], 2);
            // Third element should be an empty list.
            assert_eq!(offsets[2], offsets[3]);

            // Downcast the inner array to a StructArray.
            let struct_array = list_array.values()
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Expected a StructArray");

            // Retrieve the field arrays:
            // - filename: StringArray
            // - mime_type: StringArray
            // - blob: BinaryArray
            let filename_array = struct_array.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected a StringArray for filename");

            let mime_type_array = struct_array.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected a StringArray for mime_type");

            let blob_array = struct_array.column(2)
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .expect("Expected a BinaryArray for blob");

            // Verify first attachment in the first element.
            assert_eq!(filename_array.value(0), "file1.txt");
            assert!(mime_type_array.is_valid(0));
            assert_eq!(mime_type_array.value(0), "text/plain");
            assert!(blob_array.is_valid(0));
            assert_eq!(blob_array.value(0), &[1, 2, 3]);

            // Verify second attachment in the first element.
            assert_eq!(filename_array.value(1), "file2.jpg");
            assert!(mime_type_array.is_valid(1));
            assert_eq!(mime_type_array.value(1), "image/jpeg");
            // The blob field is null.
            assert!(!blob_array.is_valid(1));
        }



    }

}



