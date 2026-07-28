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
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};
use mailparse::{ParsedMail, dateparse, parse_mail};
use snafu::prelude::*;

use data_components::arrow::write::MemTable;

use super::{
    EmailMessage, Error, FailedToLogoutSnafu, FailedToParseHeaderSnafu, FetchMessagesSnafu,
    GetMailboxStatusSnafu, ImapTableProvider, SearchMessagesSnafu, search,
};

/// Longest identifier set to send in a single fetch.
///
/// A match set that alternates rather than running contiguously can compact into
/// an identifier set past a server's command-line limit. Fetching the whole
/// mailbox instead is slower but always valid, and the filters are re-applied
/// above the scan either way.
const MAX_ID_SET_LEN: usize = 8 * 1024;

/// The message attributes a scan asks the server for, given whether the table
/// has a `content` column.
///
/// `ENVELOPE` backs the nine header columns and is always needed. `BODY.PEEK[]`
/// is the *entire* raw message — every MIME part, attachments included — and is
/// read only to populate `content`, so a table without that column does not ask
/// for it: transferring a mailbox's attachments only to discard them dominates
/// scan cost (#12045).
///
/// `BODY.PEEK[HEADER]` is asked for in neither case. No column is built from
/// `Fetch::header()`; the header columns come from `ENVELOPE`, and `content`
/// comes from the raw message, which carries its own headers.
///
/// `.PEEK` keeps the server from setting `\Seen`, so a scan never mutates the
/// mailbox.
fn fetch_query(fetch_content: bool) -> &'static str {
    if fetch_content {
        "(ENVELOPE BODY.PEEK[])"
    } else {
        "(ENVELOPE)"
    }
}

/// Which messages a scan asks the server for.
#[derive(Debug, PartialEq, Eq)]
enum FetchSet {
    /// UIDs, used whenever a subset was resolved by `UID SEARCH`.
    Uid(String),
    /// Sequence numbers, used for the unnarrowed `1:N` fetch of the mailbox.
    Sequence(String),
}

/// The identifier set covering the messages `UID SEARCH` matched, or `None` when
/// it matched nothing — there is then nothing to ask the server for, and an empty
/// identifier set is not valid syntax.
///
/// `uids` must be ascending. A set too long to send falls back to the whole
/// mailbox, which is slower but always valid.
fn narrowed_fetch_set(uids: &[u32]) -> Option<FetchSet> {
    search::id_set(uids).map(|set| {
        if set.len() > MAX_ID_SET_LEN {
            FetchSet::Uid(search::ALL_MESSAGES.to_string())
        } else {
            FetchSet::Uid(set)
        }
    })
}

/// The sequence set covering the whole mailbox, capped at `limit` when the query
/// set one, or `None` for an empty mailbox — `1:0` is not a valid identifier set.
fn full_fetch_set(exists: usize, limit: Option<usize>) -> Option<FetchSet> {
    let message_count = limit.map_or(exists, |limit| limit.min(exists));

    (message_count > 0).then(|| FetchSet::Sequence(format!("1:{message_count}")))
}

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

    /// Report the filters the mailbox can narrow with `SEARCH`.
    ///
    /// Always `Inexact`: `SEARCH` matches on the calendar day of the `Date:`
    /// header, so the server returns a superset of the rows the filter keeps and
    /// `DataFusion` has to apply it exactly on top.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if search::search_criteria([*filter]).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut session = self.session.connect()?;

        // Let the server pick the candidate messages where the filters allow it,
        // so refresh cost tracks new mail rather than mailbox size (#11548).
        //
        // The narrowed path addresses messages by UID: a concurrent `EXPUNGE`
        // renumbers sequence numbers, so a set resolved by `SEARCH` could name
        // different messages by the time the fetch runs. UIDs never change.
        let fetch = if let Some(criteria) = search::search_criteria(filters) {
            // `limit` is deliberately ignored here. Reporting the filters as
            // `Inexact` leaves a filter above this scan, so some of the messages
            // fetched will be discarded — capping the fetch at `limit` could then
            // yield fewer rows than the query asked for while matching messages
            // were left in the mailbox. Returning more rows than `limit` is
            // always allowed; returning fewer than are available is not.
            let mut uids: Vec<u32> = session
                .uid_search(&criteria)
                .context(SearchMessagesSnafu { criteria })?
                .into_iter()
                .collect();
            uids.sort_unstable();

            narrowed_fetch_set(&uids)
        } else {
            let status = session
                .status(self.session.mailbox(), "(MESSAGES)")
                .context(GetMailboxStatusSnafu)?;

            full_fetch_set(status.exists as usize, limit)
        };

        let mut messages = vec![];

        // An empty mailbox, or no message matching the filters, means there is
        // nothing to ask for — and `1:0` is not a valid identifier set.
        if let Some(fetch) = fetch {
            let query = fetch_query(self.fetch_content);
            let fetch_messages = match fetch {
                FetchSet::Uid(uids) => session.uid_fetch(uids, query),
                FetchSet::Sequence(sequence_set) => session.fetch(sequence_set, query),
            }
            .context(FetchMessagesSnafu)?;

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

    #[test]
    fn narrowed_fetch_set_addresses_matches_by_uid() {
        // The messages `SEARCH` matched are fetched by UID — a concurrent
        // `EXPUNGE` renumbers sequence numbers but never UIDs — and contiguous
        // runs collapse so the command stays short.
        assert_eq!(
            narrowed_fetch_set(&[4, 5, 6, 9]),
            Some(FetchSet::Uid("4:6,9".to_string()))
        );
    }

    #[test]
    fn narrowed_fetch_set_of_no_matches_is_nothing_to_fetch() {
        // Regression for #11548: a filter matching nothing must fetch nothing
        // rather than falling back to the whole mailbox. An empty identifier set
        // is also not valid syntax, so there is no set to send either.
        assert_eq!(narrowed_fetch_set(&[]), None);
    }

    #[test]
    fn narrowed_fetch_set_falls_back_to_the_mailbox_when_the_set_is_too_long() {
        // An alternating match set collapses into no runs, so its identifier set
        // grows past what a server will accept on one command line. Fetching the
        // whole mailbox is slower but valid, and the filters are re-applied above
        // the scan, so the rows are the same either way.
        let alternating: Vec<u32> = (1..=5_000).map(|id| id * 2).collect();
        assert!(
            search::id_set(&alternating).is_some_and(|set| set.len() > MAX_ID_SET_LEN),
            "the alternating set should exceed the fetch limit"
        );

        assert_eq!(
            narrowed_fetch_set(&alternating),
            Some(FetchSet::Uid(search::ALL_MESSAGES.to_string()))
        );
    }

    #[test]
    fn narrowed_fetch_set_keeps_a_long_run_that_compacts_short() {
        // The fallback keys on the length of the identifier *set*, not the number
        // of messages: a contiguous run of far more UIDs than the limit still
        // compacts to one short range and is fetched as the narrowed set.
        let limit = u32::try_from(MAX_ID_SET_LEN).expect("the fetch limit fits in a u32");
        let ids: Vec<u32> = (1..=limit).collect();

        assert_eq!(
            narrowed_fetch_set(&ids),
            Some(FetchSet::Uid(format!("1:{limit}")))
        );
    }

    #[test]
    fn full_fetch_set_covers_the_whole_mailbox() {
        assert_eq!(
            full_fetch_set(42, None),
            Some(FetchSet::Sequence("1:42".to_string()))
        );
    }

    #[test]
    fn full_fetch_set_of_an_empty_mailbox_is_nothing_to_fetch() {
        // `1:0` is not a valid identifier set, so an empty mailbox must fetch
        // nothing at all.
        assert_eq!(full_fetch_set(0, None), None);
        assert_eq!(full_fetch_set(0, Some(10)), None);
    }

    #[test]
    fn full_fetch_set_caps_at_the_limit() {
        // Without filters no row is discarded above the scan, so a `LIMIT` can
        // cap the fetch — but never above what the mailbox holds.
        assert_eq!(
            full_fetch_set(42, Some(10)),
            Some(FetchSet::Sequence("1:10".to_string()))
        );
        assert_eq!(
            full_fetch_set(5, Some(10)),
            Some(FetchSet::Sequence("1:5".to_string()))
        );
        assert_eq!(full_fetch_set(42, Some(0)), None);
    }

    #[test]
    fn fetch_query_asks_for_the_raw_message_only_to_fill_content() {
        // Regression for #12045: `BODY.PEEK[]` is the whole message — every MIME
        // part and attachment — and is read only to build `content`, so a table
        // without that column must not ask the server for it at all.
        assert_eq!(fetch_query(true), "(ENVELOPE BODY.PEEK[])");
        assert_eq!(fetch_query(false), "(ENVELOPE)");
    }

    #[test]
    fn fetch_query_matches_the_columns_the_schema_exposes() {
        // The attribute list and the schema must stay in step: the raw message is
        // asked for exactly when there is a `content` column to put it in. Adding
        // a column that needs another attribute, or gating `content` differently,
        // has to move both together.
        for fetch_content in [false, true] {
            let has_content_column = test_provider(fetch_content)
                .schema()
                .field_with_name("content")
                .is_ok();
            let asks_for_raw_message = fetch_query(fetch_content).contains("BODY.PEEK[]");

            assert_eq!(
                asks_for_raw_message, has_content_column,
                "fetch_content={fetch_content}: asked for the raw message={asks_for_raw_message}, \
                 but the schema exposes content={has_content_column}"
            );
        }
    }

    #[test]
    fn fetch_query_never_asks_for_an_unread_attribute() {
        // Nothing builds a column from `Fetch::header()`, so `BODY.PEEK[HEADER]`
        // is pure transfer cost in either configuration — the header columns come
        // from `ENVELOPE` and the raw message carries its own headers.
        for fetch_content in [false, true] {
            let query = fetch_query(fetch_content);

            assert!(
                !query.contains("BODY.PEEK[HEADER]"),
                "fetch_content={fetch_content}: {query} asks for a header section no column reads"
            );
            assert!(
                query.contains("ENVELOPE"),
                "fetch_content={fetch_content}: {query} must ask for the envelope"
            );
        }
    }

    #[test]
    fn fetch_query_is_a_valid_peeking_attribute_list() {
        // A scan must not mutate the mailbox: every body section is peeked, so the
        // server never sets `\Seen`. The list also has to stay parenthesized to be
        // valid `FETCH` syntax.
        for fetch_content in [false, true] {
            let query = fetch_query(fetch_content);

            assert!(
                query.starts_with('(') && query.ends_with(')'),
                "fetch_content={fetch_content}: {query} is not a parenthesized attribute list"
            );
            assert!(
                !query.contains("BODY[") && !query.contains("RFC822"),
                "fetch_content={fetch_content}: {query} fetches a body section without `.PEEK`, \
                 which would flag messages as read"
            );
        }
    }
}
