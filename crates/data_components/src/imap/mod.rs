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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{Date64Array, Date64Builder, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use imap::{ImapConnection, Session as ImapSession};
use mailparse::{dateparse, MailHeaderMap, ParsedContentType};
use snafu::prelude::*;
use tokio::sync::Mutex;

use crate::arrow::write::MemTable;

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
}

fn decode(value: &[u8]) -> String {
    match String::from_utf8(value.to_vec()) {
        Ok(s) => s,
        Err(_) => charset::decode_latin1(value).to_string(),
    }
}

#[derive(Debug)]
pub struct ImapTableProvider {
    session: Mutex<ImapSession<Box<dyn ImapConnection>>>,
}

impl ImapTableProvider {
    #[must_use]
    pub fn new(session: ImapSession<Box<dyn ImapConnection>>) -> Self {
        Self {
            session: Mutex::new(session),
        }
    }
}

impl From<Error> for DataFusionError {
    fn from(error: Error) -> Self {
        DataFusionError::Execution(error.to_string())
    }
}

#[async_trait]
impl TableProvider for ImapTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date64, false),
            Field::new("subject", DataType::Utf8, true),
            // Field::new(
            //     "from",
            //     DataType::List(Arc::new(Field::new("email", DataType::Utf8, false))),
            //     false,
            // ),
            // Field::new(
            //     "to",
            //     DataType::List(Arc::new(Field::new("email", DataType::Utf8, false))),
            //     false,
            // ),
            // Field::new(
            //     "cc",
            //     DataType::List(Arc::new(Field::new("email", DataType::Utf8, false))),
            //     false,
            // ),
            // Field::new(
            //     "bcc",
            //     DataType::List(Arc::new(Field::new("email", DataType::Utf8, false))),
            //     false,
            // ),
            // Field::new(
            //     "reply_to",
            //     DataType::List(Arc::new(Field::new("email", DataType::Utf8, false))),
            //     false,
            // ),
            Field::new("message_id", DataType::Utf8, true),
            Field::new("in_reply_to", DataType::Utf8, true),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut session = self.session.lock().await;
        session.examine("INBOX").context(ExamineMailboxSnafu)?;

        let status = session
            .status("INBOX", "(MESSAGES)")
            .context(GetMailboxStatusSnafu)?;
        let message_count = status.exists;

        let messages = session
            .fetch(
                format!("1:{message_count}"),
                "(ENVELOPE RFC822.HEADER RFC822)",
            )
            .context(FetchMessagesSnafu)?;
        let mut subjects = vec![];
        let mut dates = vec![];
        let mut message_ids = vec![];
        let mut in_reply_tos = vec![];

        for i in 0..messages.len() {
            let message = messages.get(i).ok_or(Error::MessageNotFound {})?;
            // let header = message.header().ok_or(Error::HeaderNotFound {})?;
            let envelope = message.envelope().ok_or(Error::EnvelopeNotFound {
                segment: "envelope".to_string(),
            })?;
            let subject = envelope.subject.as_ref().map(|v| decode(v));
            let date = dateparse(&decode(envelope.date.as_ref().ok_or(
                Error::EnvelopeNotFound {
                    segment: "date".to_string(),
                },
            )?))
            .context(FailedToParseHeaderSnafu)?;

            let message_id = envelope.message_id.as_ref().map(|v| decode(v));
            let in_reply_to = envelope.in_reply_to.as_ref().map(|v| decode(v));
            // let headers = mailparse::parse_headers(header).context(FailedToParseHeaderSnafu)?;
            // let subject = headers
            //     .0
            //     .get_first_header("Subject")
            //     .ok_or(Error::EnvelopeNotFound {})?
            //     .get_value();
            // let date = headers
            //     .0
            //     .get_first_header("Date")
            //     .ok_or(Error::EnvelopeNotFound {})?
            //     .get_value();
            // let date = dateparse(&date).context(FailedToParseHeaderSnafu)?;

            // for header in &headers.0 {
            //     println!("{}", header.get_key());
            // }

            println!("{date} - {subject:?} - {i}/{message_count}");

            dates.push(date);
            subjects.push(subject);
            message_ids.push(message_id);
            in_reply_tos.push(in_reply_to);
        }

        let record_batch = RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(Date64Array::from(dates)),
                Arc::new(StringArray::from(subjects)),
                Arc::new(StringArray::from(message_ids)),
                Arc::new(StringArray::from(in_reply_tos)),
            ],
        )?;

        let table = MemTable::try_new(self.schema(), vec![vec![record_batch]])?;
        table.scan(state, projection, filters, limit).await
    }
}
