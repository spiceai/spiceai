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

use std::fmt::Display;

pub enum Color {
    RedBold,
    Green,
    Blue,
}

impl Display for Color {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let color = match self {
            Color::RedBold => "\x1b[1;31m",
            Color::Green => "\x1b[32m",
            Color::Blue => "\x1b[34m",
        };
        write!(f, "{color}")
    }
}

#[macro_export]
macro_rules! with_color {
    ($color:expr, $msg:expr, $($arg:tt)*) => {
        format!("{}{}\x1b[0m", $color, format!($msg, $($arg)*))
    };
    ($color:expr, $msg:expr) => {
        format!("{}{}\x1b[0m", $color, $msg)
    };
}
