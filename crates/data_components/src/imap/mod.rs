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
        ArrayRef, Date64Array, ListArray, ListBuilder, RecordBatch, StringArray, StringBuilder,
    },
    error::ArrowError,
};
use datafusion::{catalog::TableProvider, error::DataFusionError};
use session::ImapSession;
use snafu::prelude::*;

pub mod provider;
pub mod session;

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
        let mut bodies = vec![];

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
            bodies.push(message.body);
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
        ];

        if self.fetch_content {
            fields.push(Arc::new(StringArray::from(bodies)));
        }

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
    body: Option<String>,
}
