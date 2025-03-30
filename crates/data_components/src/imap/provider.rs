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

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::Result as DataFusionResult,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use mailparse::dateparse;
use snafu::prelude::*;

use super::{
    mime::MimeExtract, EmailMessage, Error, FailedToLogoutSnafu, FailedToParseHeaderSnafu,
    FetchMessagesSnafu, GetMailboxStatusSnafu, ImapTableProvider,
};
use crate::arrow::write::MemTable;

fn decode_string(value: &[u8]) -> String {
    match String::from_utf8(value.to_vec()) {
        Ok(s) => s,
        Err(_) => charset::decode_latin1(value).to_string(),
    }
}

macro_rules! parse_addreses_from_envelope {
    ($envelope:expr, $segment:ident) => {
        $envelope
            .$segment
            .as_ref()
            .map(|v| {
                let mut froms = vec![];
                for address in v {
                    let mailbox = address.mailbox.as_ref().map(|v| decode_string(v));
                    let host = address.host.as_ref().map(|v| decode_string(v));
                    match (mailbox, host) {
                        (Some(mailbox), Some(host)) => {
                            froms.push(Some(format!("{mailbox}@{host}")));
                        }
                        (Some(_), None) | (None, Some(_)) => {
                            return Err(Error::EnvelopeNotFound {
                                segment: stringify!($segment).to_string(),
                            })
                        }
                        (None, None) => continue,
                    }
                }

                Ok(froms)
            })
            .transpose()?
    };
}

#[async_trait]
impl TableProvider for ImapTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new("date", DataType::Date64, false),
            Field::new("subject", DataType::Utf8, true),
            Field::new(
                "from",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "to",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "cc",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "bcc",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "reply_to",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("message_id", DataType::Utf8, true),
            Field::new("in_reply_to", DataType::Utf8, true),
            Field::new("header", DataType::Utf8, true), //field name pluralised by convention
        ];

        if self.fetch_content {
            fields.push(Field::new("content", DataType::Utf8, true));

            // These contain the extracted text/html or text/plain user-content sections
            fields.push(Field::new(
                "content_sections",
                DataType::List(Arc::new(Field::new_list_field(
                    DataType::Struct(Fields::from(vec![
                        Field::new("content", DataType::Utf8, true),
                        Field::new("mime_type", DataType::Utf8, true),
                    ])),
                    true,
                ))),
                true,
            ));

            fields.push(Field::new(
                "attachments",
                DataType::List(Arc::new(Field::new_list_field(
                    DataType::Struct(Fields::from(vec![
                        Field::new("filename", DataType::Utf8, true),
                        Field::new("mime_type", DataType::Utf8, true),
                        Field::new("blob", DataType::Binary, true),
                    ])),
                    true,
                ))),
                true,
            ));
        };

        Arc::new(Schema::new(fields))
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
        let mut session = self.session.connect()?;

        let status = session
            .status(self.session.mailbox(), "(MESSAGES)")
            .context(GetMailboxStatusSnafu)?;
        let message_count = if let Some(limit) = limit {
            limit.min(status.exists as usize)
        } else {
            status.exists as usize
        };

        let imap_query_msg: String = if self.fetch_content {
            "(ENVELOPE BODY.PEEK[HEADER] BODY.PEEK[])".to_string()
        } else {
            "(ENVELOPE BODY.PEEK[HEADER])".to_string()
        };

        let fetch_messages = session
            .fetch(format!("1:{message_count}"), &imap_query_msg)
            .context(FetchMessagesSnafu)?;
        let mut messages = vec![];

        for i in 0..fetch_messages.len() {
            let message = fetch_messages.get(i).ok_or(Error::MessageNotFound {})?;
            let envelope = message.envelope().ok_or(Error::EnvelopeNotFound {
                segment: "envelope".to_string(),
            })?;
            let subject = envelope.subject.as_ref().map(|v| decode_string(v));
            let date = dateparse(&decode_string(envelope.date.as_ref().ok_or(
                //FIXME: mailparse crate is used here only?
                Error::EnvelopeNotFound {
                    segment: "date".to_string(),
                },
            )?))
            .context(FailedToParseHeaderSnafu)?;
            let message_id = envelope.message_id.as_ref().map(|v| decode_string(v));
            let in_reply_to = envelope.in_reply_to.as_ref().map(|v| decode_string(v));
            let message_froms = parse_addreses_from_envelope!(envelope, from);
            let message_tos = parse_addreses_from_envelope!(envelope, to);
            let message_ccs = parse_addreses_from_envelope!(envelope, cc);
            let message_blind_ccs = parse_addreses_from_envelope!(envelope, bcc);
            let message_reply_tos = parse_addreses_from_envelope!(envelope, reply_to);
            let header = message.header().as_ref().map(|v| decode_string(v));

            let body = if self.fetch_content {
                message.body().as_ref().map(|v| decode_string(v))
            } else {
                None
            };

            let mime_extract = MimeExtract::from(&body);

            messages.push(EmailMessage {
                date,
                subject,
                from: message_froms,
                to: message_tos,
                cc: message_ccs,
                bcc: message_blind_ccs,
                reply_to: message_reply_tos,
                message_id,
                in_reply_to,
                header,
                body,
                attachments: mime_extract.attachments,
                content_sections: mime_extract.content_sections,
            });
        }

        session.logout().context(FailedToLogoutSnafu)?; // good IMAP etiquette to not leave the session open
                                                        // logging out will drop the session, which also drops the client, which drops the stream/connection

        let record_batch = self.build_recordbatch(messages)?;
        let table = MemTable::try_new(self.schema(), vec![vec![record_batch]])?;
        table.scan(state, projection, filters, limit).await
    }
}
