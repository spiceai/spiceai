/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Platform keychain access for credentials stored by `spice login`.
//!
//! All access goes through this module so tests can exercise credential-store
//! precedence without reading, changing, or deleting the developer's keychain.

#[cfg(not(test))]
const KEYCHAIN_USER: &str = "spice";

/// What the platform keychain can prove about one credential entry.
pub enum CredentialRead {
    Found(String),
    Missing,
    Unavailable(String),
}

#[must_use]
pub fn read(account: &str) -> Option<String> {
    match inspect(account) {
        CredentialRead::Found(value) => Some(value),
        CredentialRead::Missing | CredentialRead::Unavailable(_) => None,
    }
}

#[must_use]
pub fn inspect(account: &str) -> CredentialRead {
    #[cfg(test)]
    return test_store::inspect(account);

    #[cfg(not(test))]
    {
        let entry = match keyring::Entry::new(account, KEYCHAIN_USER) {
            Ok(entry) => entry,
            Err(err) => return CredentialRead::Unavailable(err.to_string()),
        };
        match entry.get_password() {
            Ok(value) if value.is_empty() => CredentialRead::Missing,
            Ok(value) => CredentialRead::Found(value),
            Err(keyring::Error::NoEntry) => CredentialRead::Missing,
            Err(err) => CredentialRead::Unavailable(err.to_string()),
        }
    }
}

pub fn write(account: &str, value: &str) -> Result<(), String> {
    #[cfg(test)]
    return test_store::write(account, value);

    #[cfg(not(test))]
    keyring::Entry::new(account, KEYCHAIN_USER)
        .map_err(|err| err.to_string())?
        .set_password(value)
        .map_err(|err| err.to_string())
}

/// Delete `account`, returning whether it existed.
pub fn delete(account: &str) -> Result<bool, String> {
    #[cfg(test)]
    return test_store::delete(account);

    #[cfg(not(test))]
    {
        let entry = match keyring::Entry::new(account, KEYCHAIN_USER) {
            Ok(entry) => entry,
            Err(err) => return Err(err.to_string()),
        };
        match entry.delete_credential() {
            Ok(()) => Ok(true),
            Err(keyring::Error::NoEntry) => Ok(false),
            Err(err) => Err(err.to_string()),
        }
    }
}

#[cfg(test)]
pub(crate) mod test_store {
    use std::cell::RefCell;
    use std::collections::{BTreeMap, BTreeSet};

    thread_local! {
        static ENTRIES: RefCell<BTreeMap<String, String>> = const { RefCell::new(BTreeMap::new()) };
        static UNREADABLE: RefCell<BTreeSet<String>> = const { RefCell::new(BTreeSet::new()) };
        static DELETE_FAILURES: RefCell<BTreeSet<String>> = const { RefCell::new(BTreeSet::new()) };
        static DELETE_MISSING: RefCell<BTreeSet<String>> = const { RefCell::new(BTreeSet::new()) };
    }

    pub(crate) fn inspect(account: &str) -> super::CredentialRead {
        if UNREADABLE.with_borrow(|entries| entries.contains(account)) {
            return super::CredentialRead::Unavailable("credential store is locked".to_string());
        }
        ENTRIES.with_borrow(|entries| {
            entries
                .get(account)
                .map_or(super::CredentialRead::Missing, |value| {
                    if value.is_empty() {
                        super::CredentialRead::Missing
                    } else {
                        super::CredentialRead::Found(value.clone())
                    }
                })
        })
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "matches the fallible platform keychain operation"
    )]
    pub(crate) fn write(account: &str, value: &str) -> Result<(), String> {
        ENTRIES.with_borrow_mut(|entries| {
            entries.insert(account.to_string(), value.to_string());
        });
        Ok(())
    }

    pub(crate) fn delete(account: &str) -> Result<bool, String> {
        if DELETE_FAILURES.with_borrow(|entries| entries.contains(account)) {
            return Err("credential store is locked".to_string());
        }
        if DELETE_MISSING.with_borrow(|entries| entries.contains(account)) {
            return Ok(false);
        }
        let removed = ENTRIES.with_borrow_mut(|entries| entries.remove(account).is_some());
        UNREADABLE.with_borrow_mut(|entries| {
            entries.remove(account);
        });
        Ok(removed)
    }

    pub(crate) fn make_unreadable(account: &str) {
        UNREADABLE.with_borrow_mut(|entries| {
            entries.insert(account.to_string());
        });
    }

    pub(crate) fn fail_delete(account: &str) {
        DELETE_FAILURES.with_borrow_mut(|entries| {
            entries.insert(account.to_string());
        });
    }

    pub(crate) fn report_missing_on_delete(account: &str) {
        DELETE_MISSING.with_borrow_mut(|entries| {
            entries.insert(account.to_string());
        });
    }

    pub(crate) fn reset() {
        ENTRIES.with_borrow_mut(BTreeMap::clear);
        UNREADABLE.with_borrow_mut(BTreeSet::clear);
        DELETE_FAILURES.with_borrow_mut(BTreeSet::clear);
        DELETE_MISSING.with_borrow_mut(BTreeSet::clear);
    }
}
