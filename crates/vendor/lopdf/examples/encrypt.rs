use lopdf::encryption::crypt_filters::{Aes128CryptFilter, Aes256CryptFilter, CryptFilter};
use lopdf::{Document, EncryptionState, EncryptionVersion, Permissions};
use rand::RngExt as _;
use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(not(feature = "async"))]
fn main() {
    // Collect command line arguments: input_file angle output_file
    let args: Vec<String> = std::env::args().collect();
    assert!(args.len() >= 4, "Not enough arguments: input_file output_file version");
    let input_file = &args[1];
    let output_file = &args[2];
    let version = args[3].parse::<i64>().unwrap_or(0);

    let mut doc = Document::load(input_file).unwrap();
    let permissions = Permissions::PRINTABLE
        | Permissions::COPYABLE
        | Permissions::COPYABLE_FOR_ACCESSIBILITY
        | Permissions::PRINTABLE_IN_HIGH_QUALITY;
    let mut file_encryption_key = [0u8; 32];

    let requested_version = match version {
        1 => {
            assert!(
                args.len() >= 6,
                "Not enough arguments: input_file output_file 1 owner_password user_password"
            );

            let owner_password = &args[4];
            let user_password = &args[5];

            EncryptionVersion::V1 {
                document: &doc,
                owner_password,
                user_password,
                permissions,
            }
        }
        2 => {
            assert!(
                args.len() >= 6,
                "Not enough arguments: input_file output_file 1 owner_password user_password key_length"
            );

            let owner_password = &args[4];
            let user_password = &args[5];
            let key_length = if args.len() > 6 {
                args[6].parse::<usize>().unwrap_or(40)
            } else {
                40
            };

            EncryptionVersion::V2 {
                document: &doc,
                owner_password,
                user_password,
                key_length,
                permissions,
            }
        }
        4 => {
            assert!(
                args.len() >= 6,
                "Not enough arguments: input_file output_file 1 owner_password user_password"
            );

            let owner_password = &args[4];
            let user_password = &args[5];

            let crypt_filter: Arc<dyn CryptFilter> = Arc::new(Aes128CryptFilter);

            EncryptionVersion::V4 {
                document: &doc,
                encrypt_metadata: true,
                crypt_filters: BTreeMap::from([(b"StdCF".to_vec(), crypt_filter)]),
                stream_filter: b"StdCF".to_vec(),
                string_filter: b"StdCF".to_vec(),
                owner_password,
                user_password,
                permissions,
            }
        }
        5 => {
            assert!(
                args.len() >= 6,
                "Not enough arguments: input_file output_file 1 owner_password user_password"
            );

            let owner_password = &args[4];
            let user_password = &args[5];

            let crypt_filter: Arc<dyn CryptFilter> = Arc::new(Aes256CryptFilter);

            let mut rng = rand::rng();
            rng.fill(&mut file_encryption_key);

            EncryptionVersion::V5 {
                encrypt_metadata: true,
                crypt_filters: BTreeMap::from([(b"StdCF".to_vec(), crypt_filter)]),
                file_encryption_key: &file_encryption_key,
                stream_filter: b"StdCF".to_vec(),
                string_filter: b"StdCF".to_vec(),
                owner_password,
                user_password,
                permissions,
            }
        }
        _ => {
            println!("unsupported version {version}");
            return;
        }
    };

    let state = EncryptionState::try_from(requested_version).unwrap();

    // Ensure the document is decrypted.
    if doc.is_encrypted() {
        println!("nothing to be done");
        return;
    }

    // Encrypt the document.
    doc.encrypt(&state).unwrap();

    // Store file in current working directory.
    doc.save(output_file).unwrap();
}

#[cfg(feature = "async")]
#[tokio::main]
async fn main() {
    // Collect command line arguments: input_file angle output_file
    let args: Vec<String> = std::env::args().collect();
    assert!(args.len() >= 4, "Not enough arguments: input_file output_file version");
    let input_file = &args[1];
    let output_file = &args[2];
    let version = args[3].parse::<i64>().unwrap_or(0);

    let mut doc = Document::load(input_file).await.unwrap();
    let permissions = Permissions::PRINTABLE
        | Permissions::COPYABLE
        | Permissions::COPYABLE_FOR_ACCESSIBILITY
        | Permissions::PRINTABLE_IN_HIGH_QUALITY;
    let mut file_encryption_key = [0u8; 32];

    let requested_version = match version {
        1 => {
            assert!(
                args.len() >= 6,
                "Not enough arguments: input_file output_file 1 owner_password user_password"
            );

            let owner_password = &args[4];
            let user_password = &args[5];

            EncryptionVersion::V1 {
                document: &doc,
                owner_password,
                user_password,
                permissions,
            }
        }
        2 => {
            assert!(
                args.len() >= 6,
                "Not enough arguments: input_file output_file 1 owner_password user_password key_length"
            );

            let owner_password = &args[4];
            let user_password = &args[5];
            let key_length = if args.len() > 6 {
                args[6].parse::<usize>().unwrap_or(40)
            } else {
                40
            };

            EncryptionVersion::V2 {
                document: &doc,
                owner_password,
                user_password,
                key_length,
                permissions,
            }
        }
        4 => {
            assert!(
                args.len() >= 6,
                "Not enough arguments: input_file output_file 1 owner_password user_password"
            );

            let owner_password = &args[4];
            let user_password = &args[5];

            let crypt_filter: Arc<dyn CryptFilter> = Arc::new(Aes128CryptFilter);

            EncryptionVersion::V4 {
                document: &doc,
                encrypt_metadata: true,
                crypt_filters: BTreeMap::from([(b"StdCF".to_vec(), crypt_filter)]),
                stream_filter: b"StdCF".to_vec(),
                string_filter: b"StdCF".to_vec(),
                owner_password,
                user_password,
                permissions,
            }
        }
        5 => {
            assert!(
                args.len() >= 6,
                "Not enough arguments: input_file output_file 1 owner_password user_password"
            );

            let owner_password = &args[4];
            let user_password = &args[5];

            let crypt_filter: Arc<dyn CryptFilter> = Arc::new(Aes256CryptFilter);

            let mut rng = rand::rng();
            rng.fill(&mut file_encryption_key);

            EncryptionVersion::V5 {
                encrypt_metadata: true,
                crypt_filters: BTreeMap::from([(b"StdCF".to_vec(), crypt_filter)]),
                file_encryption_key: &file_encryption_key,
                stream_filter: b"StdCF".to_vec(),
                string_filter: b"StdCF".to_vec(),
                owner_password,
                user_password,
                permissions,
            }
        }
        _ => {
            println!("unsupported version {version}");
            return;
        }
    };

    let state = EncryptionState::try_from(requested_version).unwrap();

    // Ensure the document is decrypted.
    if doc.is_encrypted() {
        println!("nothing to be done");
        return;
    }

    // Encrypt the document.
    doc.encrypt(&state).unwrap();

    // Store file in current working directory.
    doc.save(output_file).unwrap();
}
