use bytes::Buf;

use crate::error::{PgWireError, Result};

/// Parsed PostgreSQL error/notice response fields
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ErrorFields {
    pub severity: Option<String>,
    pub code: Option<String>, // SQLSTATE
    pub message: Option<String>,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<String>,
    pub where_: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub column: Option<String>,
    pub data_type: Option<String>,
    pub constraint: Option<String>,
    pub file: Option<String>,
    pub line: Option<String>,
    pub routine: Option<String>,
}

impl ErrorFields {
    /// Parse error fields from payload bytes
    pub fn parse(payload: &[u8]) -> Self {
        let mut fields = ErrorFields::default();
        let mut b = payload;

        while !b.is_empty() {
            let code = b[0];
            b = &b[1..];
            if code == 0 {
                break;
            }
            if let Some(pos) = b.iter().position(|&x| x == 0) {
                let s = String::from_utf8_lossy(&b[..pos]).to_string();
                match code {
                    b'S' => fields.severity = Some(s),
                    b'C' => fields.code = Some(s),
                    b'M' => fields.message = Some(s),
                    b'D' => fields.detail = Some(s),
                    b'H' => fields.hint = Some(s),
                    b'P' => fields.position = Some(s),
                    b'W' => fields.where_ = Some(s),
                    b's' => fields.schema = Some(s),
                    b't' => fields.table = Some(s),
                    b'c' => fields.column = Some(s),
                    b'd' => fields.data_type = Some(s),
                    b'n' => fields.constraint = Some(s),
                    b'F' => fields.file = Some(s),
                    b'L' => fields.line = Some(s),
                    b'R' => fields.routine = Some(s),
                    _ => {} // ignore unknown fields
                }
                b = &b[pos + 1..];
            } else {
                break;
            }
        }

        fields
    }

    /// Format as a human-readable error string
    pub fn to_error_string(&self) -> String {
        match (&self.message, &self.code) {
            (Some(m), Some(c)) => format!("{m} (SQLSTATE {c})"),
            (Some(m), None) => m.clone(),
            (None, Some(c)) => format!("error (SQLSTATE {c})"),
            (None, None) => "unknown server error".to_string(),
        }
    }
}

/// Parse an ErrorResponse payload into a human-readable string.
///
/// For more detailed error information, use `ErrorFields::parse()` instead.
pub fn parse_error_response(payload: &[u8]) -> String {
    ErrorFields::parse(payload).to_error_string()
}

/// Parse an AuthenticationRequest payload.
///
/// Returns (auth_type, remaining_data).
/// Auth types:
/// - 0 = AuthenticationOk
/// - 3 = AuthenticationCleartextPassword
/// - 5 = AuthenticationMD5Password (data contains 4-byte salt)
/// - 10 = AuthenticationSASL (data contains mechanism names)
/// - 11 = AuthenticationSASLContinue
/// - 12 = AuthenticationSASLFinal
pub fn parse_auth_request(payload: &[u8]) -> Result<(i32, &[u8])> {
    if payload.len() < 4 {
        return Err(PgWireError::Protocol("auth request too short".into()));
    }
    let mut b = payload;
    let code = b.get_i32();
    Ok((code, b))
}

/// Authentication type constants
pub mod auth {
    pub const OK: i32 = 0;
    pub const CLEARTEXT_PASSWORD: i32 = 3;
    pub const MD5_PASSWORD: i32 = 5;
    pub const SASL: i32 = 10;
    pub const SASL_CONTINUE: i32 = 11;
    pub const SASL_FINAL: i32 = 12;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_response_extracts_message_and_code() {
        // 'M' "hello" \0 'C' "12345" \0 \0
        let payload = [
            b'M', b'h', b'e', b'l', b'l', b'o', 0, b'C', b'1', b'2', b'3', b'4', b'5', 0, 0,
        ];
        let s = parse_error_response(&payload);
        assert!(s.contains("hello"));
        assert!(s.contains("SQLSTATE 12345"));
    }

    #[test]
    fn parse_error_response_handles_message_only() {
        let payload = [b'M', b't', b'e', b's', b't', 0, 0];
        let s = parse_error_response(&payload);
        assert_eq!(s, "test");
    }

    #[test]
    fn parse_error_response_handles_code_only() {
        let payload = [b'C', b'4', b'2', b'0', b'0', b'0', 0, 0];
        let s = parse_error_response(&payload);
        assert_eq!(s, "error (SQLSTATE 42000)");
    }

    #[test]
    fn parse_error_response_handles_empty() {
        let payload = [0];
        let s = parse_error_response(&payload);
        assert_eq!(s, "unknown server error");
    }

    #[test]
    fn parse_error_response_handles_truly_empty() {
        let payload: &[u8] = &[];
        let s = parse_error_response(payload);
        assert_eq!(s, "unknown server error");
    }

    #[test]
    fn error_fields_parses_all_standard_fields() {
        let mut payload = Vec::new();
        // Build a realistic error response
        payload.extend_from_slice(b"SERROR\0");
        payload.extend_from_slice(b"C42P01\0");
        payload.extend_from_slice(b"Mrelation \"foo\" does not exist\0");
        payload.extend_from_slice(b"Dsome detail\0");
        payload.extend_from_slice(b"Htry this\0");
        payload.extend_from_slice(b"sschema_name\0");
        payload.extend_from_slice(b"ttable_name\0");
        payload.extend_from_slice(b"Fparse_relation.c\0");
        payload.extend_from_slice(b"L1234\0");
        payload.extend_from_slice(b"Rsome_routine\0");
        payload.push(0); // terminator

        let fields = ErrorFields::parse(&payload);

        assert_eq!(fields.severity.as_deref(), Some("ERROR"));
        assert_eq!(fields.code.as_deref(), Some("42P01"));
        assert_eq!(
            fields.message.as_deref(),
            Some("relation \"foo\" does not exist")
        );
        assert_eq!(fields.detail.as_deref(), Some("some detail"));
        assert_eq!(fields.hint.as_deref(), Some("try this"));
        assert_eq!(fields.schema.as_deref(), Some("schema_name"));
        assert_eq!(fields.table.as_deref(), Some("table_name"));
        assert_eq!(fields.file.as_deref(), Some("parse_relation.c"));
        assert_eq!(fields.line.as_deref(), Some("1234"));
        assert_eq!(fields.routine.as_deref(), Some("some_routine"));
    }

    #[test]
    fn error_fields_handles_truncated_payload() {
        // Missing null terminator for value
        let payload = [b'M', b'h', b'e', b'l', b'l', b'o'];
        let fields = ErrorFields::parse(&payload);
        // Should not panic, just skip incomplete field
        assert!(fields.message.is_none());
    }

    #[test]
    fn error_fields_ignores_unknown_field_codes() {
        let payload = [b'X', b'u', b'n', b'k', 0, b'M', b'o', b'k', 0, 0];
        let fields = ErrorFields::parse(&payload);
        // Unknown 'X' field ignored, 'M' field parsed
        assert_eq!(fields.message.as_deref(), Some("ok"));
    }

    #[test]
    fn parse_auth_request_ok() {
        let payload = [0, 0, 0, 0]; // auth type 0 = OK
        let (code, rest) = parse_auth_request(&payload).unwrap();
        assert_eq!(code, auth::OK);
        assert!(rest.is_empty());
    }

    #[test]
    fn parse_auth_request_md5_with_salt() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&5i32.to_be_bytes()); // MD5
        payload.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]); // salt

        let (code, salt) = parse_auth_request(&payload).unwrap();
        assert_eq!(code, auth::MD5_PASSWORD);
        assert_eq!(salt, &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn parse_auth_request_sasl_with_mechanisms() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&10i32.to_be_bytes()); // SASL
        payload.extend_from_slice(b"SCRAM-SHA-256\0");
        payload.extend_from_slice(b"SCRAM-SHA-256-PLUS\0");
        payload.push(0); // terminator

        let (code, mechanisms) = parse_auth_request(&payload).unwrap();
        assert_eq!(code, auth::SASL);
        assert!(mechanisms.starts_with(b"SCRAM-SHA-256"));
    }

    #[test]
    fn parse_auth_request_rejects_short_payload() {
        let payload = [0, 0, 0]; // only 3 bytes
        let err = parse_auth_request(&payload).unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    #[test]
    fn auth_constants_have_correct_values() {
        assert_eq!(auth::OK, 0);
        assert_eq!(auth::CLEARTEXT_PASSWORD, 3);
        assert_eq!(auth::MD5_PASSWORD, 5);
        assert_eq!(auth::SASL, 10);
        assert_eq!(auth::SASL_CONTINUE, 11);
        assert_eq!(auth::SASL_FINAL, 12);
    }
}
