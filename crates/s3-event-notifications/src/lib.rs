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

//! Consume Amazon S3 Event Notifications delivered to an SQS queue.
//!
//! - [`event`] parses the notification bodies a queue receives: direct
//!   S3 → SQS, S3 → SNS → SQS, and `EventBridge`.
//! - [`queue`] long-polls and acknowledges messages behind a trait, so a
//!   consumer can be tested without AWS.
//! - [`queue_url`] validates queue URLs and reads their region.
//! - [`client`] builds the SQS client from the credentials a consumer resolved.

pub mod client;
pub mod event;
pub mod queue;
pub mod queue_url;
