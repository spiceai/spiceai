/*
Copyright 2024-2026 The Spice.ai OSS Authors
Copyright (c) 2014-2026 Kang Seonghoon and contributors

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

mod unlocalized {
    #[derive(Copy, Clone, Debug)]
    pub(crate) struct Locale;

    pub(crate) const fn default_locale() -> Locale {
        Locale
    }

    pub(crate) const fn short_months(_locale: Locale) -> &'static [&'static str] {
        &[
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ]
    }

    pub(crate) const fn long_months(_locale: Locale) -> &'static [&'static str] {
        &[
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ]
    }

    pub(crate) const fn short_weekdays(_locale: Locale) -> &'static [&'static str] {
        &["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    }

    pub(crate) const fn long_weekdays(_locale: Locale) -> &'static [&'static str] {
        &[
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ]
    }

    pub(crate) const fn am_pm(_locale: Locale) -> &'static [&'static str] {
        &["AM", "PM"]
    }

    pub(crate) const fn decimal_point(_locale: Locale) -> &'static str {
        "."
    }
}

pub(crate) use unlocalized::*;
