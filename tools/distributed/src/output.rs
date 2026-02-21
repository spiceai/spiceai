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

use ansi_colors::Color;

pub fn success(msg: &str) {
    println!("{}", Color::Green.paint(msg));
}

pub fn info(msg: &str) {
    println!("{}", Color::Blue.paint(msg));
}

pub fn warning(msg: &str) {
    println!("{}", Color::Yellow.paint(msg));
}

pub fn error(msg: &str) {
    eprintln!("{}", Color::Red.paint(msg));
}
