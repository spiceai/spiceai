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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::Result as DataFusionResult,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use mailparse::{ParsedMail, dateparse, parse_mail};
use snafu::prelude::*;

use data_components::arrow::write::MemTable;

use super::{
    EmailMessage, Error, FailedToLogoutSnafu, FailedToParseHeaderSnafu, FetchMessagesSnafu,
    GetMailboxStatusSnafu, ImapTableProvider,
};

fn decode(value: &[u8]) -> String {
    match String::from_utf8(value.to_vec()) {
        Ok(s) => s,
        Err(_) => charset::decode_latin1(value).to_string(),
    }
}

/// Recursively find the first MIME part matching `mimetype` and return its
/// decoded body.
///
/// `ParsedMail::get_body` applies the part's `Content-Transfer-Encoding`
/// (base64 / quoted-printable) and charset, so the returned text is
/// human-readable rather than the raw wire bytes. Attachment parts (any other
/// MIME type) are never descended into for their content — only their nested
/// text parts, if any, are considered.
fn find_part_body(part: &ParsedMail, mimetype: &str) -> Option<String> {
    if part.subparts.is_empty() {
        if part.ctype.mimetype.eq_ignore_ascii_case(mimetype) {
            return part.get_body().ok();
        }
        return None;
    }

    part.subparts
        .iter()
        .find_map(|sub| find_part_body(sub, mimetype))
}

/// Extract the human-readable text body from a raw RFC822 message.
///
/// The IMAP `BODY.PEEK[]` fetch returns the entire raw message — MIME headers,
/// multipart boundaries, and body parts still in their
/// `Content-Transfer-Encoding` (base64 / quoted-printable). Storing that
/// verbatim made `content` both bloated (attachment bytes and headers inline)
/// and, for the common base64 `text/plain` case, unreadable gibberish rather
/// than the message body (see #11549).
///
/// This walks the MIME tree preferring the decoded `text/plain` part, falls
/// back to the `text/html` part, then to the top-level decoded body for
/// non-multipart messages, and finally to the raw decoded bytes if `mailparse`
/// cannot parse the message at all — so content is never silently dropped.
/// Attachment part bytes are never returned.
fn extract_text_body(raw: &[u8]) -> String {
    if let Ok(parsed) = parse_mail(raw) {
        if let Some(text) =
            find_part_body(&parsed, "text/plain").or_else(|| find_part_body(&parsed, "text/html"))
        {
            return text;
        }
        if let Ok(body) = parsed.get_body() {
            return body;
        }
    }

    // Parsing failed entirely — preserve the raw content rather than dropping it.
    decode(raw)
}

/// Parse an RFC822 `Date:` header into milliseconds since the Unix epoch.
///
/// `mailparse::dateparse` returns whole **seconds** since the epoch, but the
/// `date` column is stored in a `Timestamp(Millisecond)` array. The value must
/// therefore be scaled by 1000; without it every timestamp is 1000x too small
/// and — because a mailbox's dates span only a few million seconds — the entire
/// mailbox collapses onto a single near-epoch instant (see #11547).
fn parse_date_millis(raw: &str) -> Result<i64, mailparse::MailParseError> {
    dateparse(raw).map(|seconds| seconds.saturating_mul(1000))
}

macro_rules! parse_addreses_from_envelope {
    ($envelope:expr, $segment:ident) => {
        $envelope
            .$segment
            .as_ref()
            .map(|v| {
                let mut froms = vec![];
                for address in v {
                    let mailbox = address.mailbox.as_ref().map(|v| decode(v));
                    let host = address.host.as_ref().map(|v| decode(v));
                    match (mailbox, host) {
                        (Some(mailbox), Some(host)) => {
                            froms.push(Some(format!("{mailbox}@{host}")));
                        }
                        (Some(_), None) | (None, Some(_)) => {
                            return Err(Error::EnvelopeNotFound {
                                segment: stringify!($segment).to_string(),
                            });
                        }
                        (None, None) => {}
                    }
                }

                Ok(froms)
            })
            .transpose()?
    };
}

#[async_trait]
impl TableProvider for ImapTableProvider {
    fn schema(&self) -> SchemaRef {
        let mut fields = vec![
            Field::new(
                "date",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
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
        ];

        if self.fetch_content {
            fields.push(Field::new("content", DataType::Utf8, true));
        }

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

        let fetch_messages = session
            .fetch(
                format!("1:{message_count}"),
                "(ENVELOPE BODY.PEEK[HEADER] BODY.PEEK[])",
            )
            .context(FetchMessagesSnafu)?;
        let mut messages = vec![];

        for i in 0..fetch_messages.len() {
            let message = fetch_messages.get(i).ok_or(Error::MessageNotFound {})?;
            let envelope = message.envelope().ok_or(Error::EnvelopeNotFound {
                segment: "envelope".to_string(),
            })?;
            let subject = envelope.subject.as_ref().map(|v| decode(v));
            let date = parse_date_millis(&decode(envelope.date.as_ref().ok_or(
                Error::EnvelopeNotFound {
                    segment: "date".to_string(),
                },
            )?))
            .context(FailedToParseHeaderSnafu)?;
            let message_id = envelope.message_id.as_ref().map(|v| decode(v));
            let in_reply_to = envelope.in_reply_to.as_ref().map(|v| decode(v));
            let message_froms = parse_addreses_from_envelope!(envelope, from);
            let message_tos = parse_addreses_from_envelope!(envelope, to);
            let message_ccs = parse_addreses_from_envelope!(envelope, cc);
            let message_blind_ccs = parse_addreses_from_envelope!(envelope, bcc);
            let message_reply_tos = parse_addreses_from_envelope!(envelope, reply_to);
            let body = if self.fetch_content {
                message.body().as_ref().map(|v| extract_text_body(v))
            } else {
                None
            };

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
                body,
            });
        }

        session.logout().context(FailedToLogoutSnafu)?; // good IMAP etiquette to not leave the session open
        // logging out will drop the session, which also drops the client, which drops the stream/connection

        let record_batch = self.build_recordbatch(messages)?;
        let table = MemTable::try_new(self.schema(), vec![vec![record_batch]])?;
        table.scan(state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imap::session::{ImapAuthMode, ImapSession};
    use arrow::array::{Array, TimestampMillisecondArray};
    use secrecy::SecretString;

    fn test_provider(fetch_content: bool) -> ImapTableProvider {
        let session = ImapSession::new(
            ImapAuthMode::Plain {
                username: SecretString::from("user"),
                password: SecretString::from("pass"),
            },
            Arc::from("localhost"),
            993,
            Arc::from("INBOX"),
        );
        ImapTableProvider::new(session, fetch_content)
    }

    fn message(date: i64) -> EmailMessage {
        EmailMessage {
            date,
            subject: Some("subject".to_string()),
            from: None,
            to: None,
            cc: None,
            bcc: None,
            reply_to: None,
            message_id: None,
            in_reply_to: None,
            body: None,
        }
    }

    #[test]
    fn parse_date_millis_scales_seconds_to_milliseconds() {
        let raw = "Tue, 1 Jul 2003 10:52:37 +0000";
        let seconds = dateparse(raw).expect("header parses to seconds");
        let millis = parse_date_millis(raw).expect("header parses to millis");

        // Regression for #11547: the stored value must be milliseconds — exactly
        // 1000x the raw seconds mailparse returns — not the seconds themselves.
        assert_eq!(millis, seconds * 1000);
        assert_eq!(millis, 1_057_056_757_000);
    }

    #[test]
    fn schema_date_column_is_timestamp_millis() {
        let schema = test_provider(false).schema();
        let date = schema.field_with_name("date").expect("date column exists");
        assert_eq!(
            date.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
    }

    #[test]
    fn build_recordbatch_preserves_distinct_send_times() {
        // Two messages a day apart. Before #11547's fix the seconds-in-a-Date64
        // column collapsed every message onto one near-epoch calendar day; the
        // millisecond timestamps must now stay distinct and exact.
        let provider = test_provider(false);
        let day_one = 1_057_056_757_000;
        let day_two = day_one + 86_400_000;
        let batch = provider
            .build_recordbatch(vec![message(day_one), message(day_two)])
            .expect("record batch builds");

        let dates = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("date column is a millisecond timestamp array");

        assert_eq!(dates.len(), 2);
        assert_eq!(dates.value(0), day_one);
        assert_eq!(dates.value(1), day_two);
        assert_ne!(dates.value(0), dates.value(1));
    }

    #[test]
    fn extract_text_body_decodes_base64_text_plain() {
        // Regression for #11549: a `text/plain` part sent as base64 must be
        // decoded to readable text, not stored as the raw base64 payload.
        let raw = b"Content-Type: text/plain; charset=utf-8\r\n\
                    Content-Transfer-Encoding: base64\r\n\r\n\
                    SGVsbG8sIHdvcmxkIQ==\r\n";
        assert_eq!(extract_text_body(raw), "Hello, world!");
    }

    #[test]
    fn extract_text_body_decodes_quoted_printable() {
        // Quoted-printable is the other common `text/plain` encoding; it must be
        // decoded (=C3=A9 -> é), preserving the message's actual characters.
        let raw = b"Content-Type: text/plain; charset=utf-8\r\n\
                    Content-Transfer-Encoding: quoted-printable\r\n\r\n\
                    Caf=C3=A9\r\n";
        assert_eq!(extract_text_body(raw).trim(), "Café");
    }

    #[test]
    fn extract_text_body_prefers_plain_over_html() {
        // multipart/alternative: the readable `text/plain` alternative is
        // preferred over the markup-laden `text/html` one.
        let raw = b"Content-Type: multipart/alternative; boundary=\"b\"\r\n\r\n\
                    --b\r\nContent-Type: text/plain\r\n\r\nplain body\r\n\
                    --b\r\nContent-Type: text/html\r\n\r\n<p>html body</p>\r\n\
                    --b--\r\n";
        let body = extract_text_body(raw);
        assert_eq!(body.trim(), "plain body");
        assert!(!body.contains("html body"), "html part leaked: {body:?}");
    }

    #[test]
    fn extract_text_body_falls_back_to_html_when_no_plain() {
        // When only a `text/html` part exists it is returned (decoded), rather
        // than falling through to the raw MIME.
        let raw = b"Content-Type: multipart/alternative; boundary=\"b\"\r\n\r\n\
                    --b\r\nContent-Type: text/html\r\n\r\n<p>only html</p>\r\n\
                    --b--\r\n";
        assert_eq!(extract_text_body(raw).trim(), "<p>only html</p>");
    }

    #[test]
    fn extract_text_body_excludes_attachment_bytes() {
        // multipart/mixed with a text part and a base64 attachment: only the
        // decoded text body is returned — the attachment payload (the source of
        // the ~320KB/message bloat in #11549) is never included.
        let raw = b"Content-Type: multipart/mixed; boundary=\"b\"\r\n\r\n\
                    --b\r\nContent-Type: text/plain\r\n\r\nreal message\r\n\
                    --b\r\nContent-Type: application/octet-stream\r\n\
                    Content-Transfer-Encoding: base64\r\n\
                    Content-Disposition: attachment; filename=\"a.bin\"\r\n\r\n\
                    AAAAAAAAAAAAAAAAAAA=\r\n\
                    --b--\r\n";
        let body = extract_text_body(raw);
        assert_eq!(body.trim(), "real message");
        assert!(
            !body.contains("AAAAAAAAAAAAAAAAAAA"),
            "attachment bytes leaked into content: {body:?}"
        );
    }

    #[test]
    fn extract_text_body_handles_single_part_plain() {
        // A simple non-multipart message decodes to its body.
        let raw = b"Content-Type: text/plain\r\n\r\njust some text\r\n";
        assert_eq!(extract_text_body(raw).trim(), "just some text");
    }
}
