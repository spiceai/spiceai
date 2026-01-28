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

//! `ClickBench` hits dataset for streaming benchmarks.

use std::sync::Arc;

use arrow::array::{
    Date32Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use test_framework::anyhow::{Context, Result};

use super::DatasetType;
use crate::commands::streaming::traits::StreamingDataset;

/// `ClickBench` hits dataset.
///
/// Generates synthetic web analytics data matching the `ClickBench` schema.
/// The hits table contains 105 columns representing web page visits.
pub struct HitsDataset;

impl HitsDataset {
    /// Get the Arrow schema for the hits table.
    #[must_use]
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("WatchID", DataType::Int64, false),
            Field::new("JavaEnable", DataType::Int16, false),
            Field::new("Title", DataType::Utf8, true),
            Field::new("GoodEvent", DataType::Int16, false),
            Field::new(
                "EventTime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("EventDate", DataType::Date32, false),
            Field::new("CounterID", DataType::Int32, false),
            Field::new("ClientIP", DataType::Int32, false),
            Field::new("RegionID", DataType::Int32, false),
            Field::new("UserID", DataType::Int64, false),
            Field::new("CounterClass", DataType::Int16, false),
            Field::new("OS", DataType::Int16, false),
            Field::new("UserAgent", DataType::Int16, false),
            Field::new("URL", DataType::Utf8, true),
            Field::new("Referer", DataType::Utf8, true),
            Field::new("IsRefresh", DataType::Int16, false),
            Field::new("RefererCategoryID", DataType::Int16, false),
            Field::new("RefererRegionID", DataType::Int32, false),
            Field::new("URLCategoryID", DataType::Int16, false),
            Field::new("URLRegionID", DataType::Int32, false),
            Field::new("ResolutionWidth", DataType::Int16, false),
            Field::new("ResolutionHeight", DataType::Int16, false),
            Field::new("ResolutionDepth", DataType::Int16, false),
            Field::new("FlashMajor", DataType::Int16, false),
            Field::new("FlashMinor", DataType::Int16, false),
            Field::new("FlashMinor2", DataType::Utf8, true),
            Field::new("NetMajor", DataType::Int16, false),
            Field::new("NetMinor", DataType::Int16, false),
            Field::new("UserAgentMajor", DataType::Int16, false),
            Field::new("UserAgentMinor", DataType::Utf8, false),
            Field::new("CookieEnable", DataType::Int16, false),
            Field::new("JavascriptEnable", DataType::Int16, false),
            Field::new("IsMobile", DataType::Int16, false),
            Field::new("MobilePhone", DataType::Int16, false),
            Field::new("MobilePhoneModel", DataType::Utf8, true),
            Field::new("Params", DataType::Utf8, true),
            Field::new("IPNetworkID", DataType::Int32, false),
            Field::new("TraficSourceID", DataType::Int16, false),
            Field::new("SearchEngineID", DataType::Int16, false),
            Field::new("SearchPhrase", DataType::Utf8, true),
            Field::new("AdvEngineID", DataType::Int16, false),
            Field::new("IsArtifical", DataType::Int16, false),
            Field::new("WindowClientWidth", DataType::Int16, false),
            Field::new("WindowClientHeight", DataType::Int16, false),
            Field::new("ClientTimeZone", DataType::Int16, false),
            Field::new(
                "ClientEventTime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("SilverlightVersion1", DataType::Int16, false),
            Field::new("SilverlightVersion2", DataType::Int16, false),
            Field::new("SilverlightVersion3", DataType::Int32, false),
            Field::new("SilverlightVersion4", DataType::Int16, false),
            Field::new("PageCharset", DataType::Utf8, true),
            Field::new("CodeVersion", DataType::Int32, false),
            Field::new("IsLink", DataType::Int16, false),
            Field::new("IsDownload", DataType::Int16, false),
            Field::new("IsNotBounce", DataType::Int16, false),
            Field::new("FUniqID", DataType::Int64, false),
            Field::new("OriginalURL", DataType::Utf8, true),
            Field::new("HID", DataType::Int32, false),
            Field::new("IsOldCounter", DataType::Int16, false),
            Field::new("IsEvent", DataType::Int16, false),
            Field::new("IsParameter", DataType::Int16, false),
            Field::new("DontCountHits", DataType::Int16, false),
            Field::new("WithHash", DataType::Int16, false),
            Field::new("HitColor", DataType::Utf8, false),
            Field::new(
                "LocalEventTime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("Age", DataType::Int16, false),
            Field::new("Sex", DataType::Int16, false),
            Field::new("Income", DataType::Int16, false),
            Field::new("Interests", DataType::Int16, false),
            Field::new("Robotness", DataType::Int16, false),
            Field::new("RemoteIP", DataType::Int32, false),
            Field::new("WindowName", DataType::Int32, false),
            Field::new("OpenerName", DataType::Int32, false),
            Field::new("HistoryLength", DataType::Int16, false),
            Field::new("BrowserLanguage", DataType::Utf8, true),
            Field::new("BrowserCountry", DataType::Utf8, true),
            Field::new("SocialNetwork", DataType::Utf8, true),
            Field::new("SocialAction", DataType::Utf8, true),
            Field::new("HTTPError", DataType::Int16, false),
            Field::new("SendTiming", DataType::Int32, false),
            Field::new("DNSTiming", DataType::Int32, false),
            Field::new("ConnectTiming", DataType::Int32, false),
            Field::new("ResponseStartTiming", DataType::Int32, false),
            Field::new("ResponseEndTiming", DataType::Int32, false),
            Field::new("FetchTiming", DataType::Int32, false),
            Field::new("SocialSourceNetworkID", DataType::Int16, false),
            Field::new("SocialSourcePage", DataType::Utf8, true),
            Field::new("ParamPrice", DataType::Int64, false),
            Field::new("ParamOrderID", DataType::Utf8, true),
            Field::new("ParamCurrency", DataType::Utf8, true),
            Field::new("ParamCurrencyID", DataType::Int16, false),
            Field::new("OpenstatServiceName", DataType::Utf8, true),
            Field::new("OpenstatCampaignID", DataType::Utf8, true),
            Field::new("OpenstatAdID", DataType::Utf8, true),
            Field::new("OpenstatSourceID", DataType::Utf8, true),
            Field::new("UTMSource", DataType::Utf8, true),
            Field::new("UTMMedium", DataType::Utf8, true),
            Field::new("UTMCampaign", DataType::Utf8, true),
            Field::new("UTMContent", DataType::Utf8, true),
            Field::new("UTMTerm", DataType::Utf8, true),
            Field::new("FromTag", DataType::Utf8, true),
            Field::new("HasGCLID", DataType::Int16, false),
            Field::new("RefererHash", DataType::Int64, false),
            Field::new("URLHash", DataType::Int64, false),
            Field::new("CLID", DataType::Int32, false),
        ])
    }

    /// Generate a single row of synthetic data.
    fn generate_row(rng: &mut StdRng, watch_id: i64) -> HitsRow {
        // Base timestamp: 2013-07-15 (typical ClickBench date range)
        let base_timestamp_us = 1_373_846_400_000_000i64; // 2013-07-15 00:00:00 UTC in microseconds
        let event_offset = rng.random_range(0..86_400_000_000i64); // Within a day

        HitsRow {
            watch_id,
            java_enable: rng.random_range(0..2),
            title: format!("Page Title {}", rng.random_range(1..1000)),
            good_event: 1,
            event_time: base_timestamp_us + event_offset,
            event_date: 15901, // Days since epoch for 2013-07-15
            counter_id: rng.random_range(1..100_000),
            client_ip: rng.random_range(0..i32::MAX),
            region_id: rng.random_range(1..1000),
            user_id: rng.random_range(1..10_000_000),
            counter_class: rng.random_range(0..10),
            os: rng.random_range(0..20),
            user_agent: rng.random_range(0..100),
            url: format!("https://example.com/page/{}", rng.random_range(1..10000)),
            referer: format!("https://referer.com/{}", rng.random_range(1..1000)),
            is_refresh: rng.random_range(0..2),
            referer_category_id: rng.random_range(0..100),
            referer_region_id: rng.random_range(0..1000),
            url_category_id: rng.random_range(0..100),
            url_region_id: rng.random_range(0..1000),
            resolution_width: rng.random_range(320..1920),
            resolution_height: rng.random_range(240..1080),
            resolution_depth: rng.random_range(8..32),
            flash_major: rng.random_range(0..20),
            flash_minor: rng.random_range(0..10),
            flash_minor2: String::new(),
            net_major: rng.random_range(0..10),
            net_minor: rng.random_range(0..10),
            user_agent_major: rng.random_range(0..100),
            user_agent_minor: format!("{}", rng.random_range(0..100)),
            cookie_enable: rng.random_range(0..2),
            javascript_enable: rng.random_range(0..2),
            is_mobile: rng.random_range(0..2),
            mobile_phone: rng.random_range(0..10),
            mobile_phone_model: String::new(),
            params: String::new(),
            ip_network_id: rng.random_range(0..10000),
            trafic_source_id: rng.random_range(0..10),
            search_engine_id: rng.random_range(0..50),
            search_phrase: String::new(),
            adv_engine_id: rng.random_range(0..20),
            is_artifical: 0,
            window_client_width: rng.random_range(320..1920),
            window_client_height: rng.random_range(240..1080),
            client_time_zone: rng.random_range(-12..12),
            client_event_time: base_timestamp_us
                + event_offset
                + rng.random_range(0..3_600_000_000),
            silverlight_version1: 0,
            silverlight_version2: 0,
            silverlight_version3: 0,
            silverlight_version4: 0,
            page_charset: String::from("UTF-8"),
            code_version: rng.random_range(1..100),
            is_link: rng.random_range(0..2),
            is_download: rng.random_range(0..2),
            is_not_bounce: rng.random_range(0..2),
            f_uniq_id: rng.random_range(1..i64::MAX),
            original_url: String::new(),
            hid: rng.random_range(0..1000),
            is_old_counter: 0,
            is_event: rng.random_range(0..2),
            is_parameter: 0,
            dont_count_hits: 0,
            with_hash: 0,
            hit_color: String::from("W"),
            local_event_time: base_timestamp_us + event_offset,
            age: rng.random_range(0..100),
            sex: rng.random_range(0..3),
            income: rng.random_range(0..10),
            interests: rng.random_range(0..100),
            robotness: rng.random_range(0..10),
            remote_ip: rng.random_range(0..i32::MAX),
            window_name: 0,
            opener_name: 0,
            history_length: rng.random_range(0..100),
            browser_language: String::from("en"),
            browser_country: String::from("US"),
            social_network: String::new(),
            social_action: String::new(),
            http_error: 0,
            send_timing: rng.random_range(0..10000),
            dns_timing: rng.random_range(0..1000),
            connect_timing: rng.random_range(0..1000),
            response_start_timing: rng.random_range(0..5000),
            response_end_timing: rng.random_range(0..10000),
            fetch_timing: rng.random_range(0..10000),
            social_source_network_id: 0,
            social_source_page: String::new(),
            param_price: 0,
            param_order_id: String::new(),
            param_currency: String::new(),
            param_currency_id: 0,
            openstat_service_name: String::new(),
            openstat_campaign_id: String::new(),
            openstat_ad_id: String::new(),
            openstat_source_id: String::new(),
            utm_source: String::new(),
            utm_medium: String::new(),
            utm_campaign: String::new(),
            utm_content: String::new(),
            utm_term: String::new(),
            from_tag: String::new(),
            has_gclid: 0,
            referer_hash: rng.random_range(0..i64::MAX),
            url_hash: rng.random_range(0..i64::MAX),
            clid: rng.random_range(0..1000),
        }
    }
}

/// Single row of hits data.
struct HitsRow {
    watch_id: i64,
    java_enable: i16,
    title: String,
    good_event: i16,
    event_time: i64,
    event_date: i32,
    counter_id: i32,
    client_ip: i32,
    region_id: i32,
    user_id: i64,
    counter_class: i16,
    os: i16,
    user_agent: i16,
    url: String,
    referer: String,
    is_refresh: i16,
    referer_category_id: i16,
    referer_region_id: i32,
    url_category_id: i16,
    url_region_id: i32,
    resolution_width: i16,
    resolution_height: i16,
    resolution_depth: i16,
    flash_major: i16,
    flash_minor: i16,
    flash_minor2: String,
    net_major: i16,
    net_minor: i16,
    user_agent_major: i16,
    user_agent_minor: String,
    cookie_enable: i16,
    javascript_enable: i16,
    is_mobile: i16,
    mobile_phone: i16,
    mobile_phone_model: String,
    params: String,
    ip_network_id: i32,
    trafic_source_id: i16,
    search_engine_id: i16,
    search_phrase: String,
    adv_engine_id: i16,
    is_artifical: i16,
    window_client_width: i16,
    window_client_height: i16,
    client_time_zone: i16,
    client_event_time: i64,
    silverlight_version1: i16,
    silverlight_version2: i16,
    silverlight_version3: i32,
    silverlight_version4: i16,
    page_charset: String,
    code_version: i32,
    is_link: i16,
    is_download: i16,
    is_not_bounce: i16,
    f_uniq_id: i64,
    original_url: String,
    hid: i32,
    is_old_counter: i16,
    is_event: i16,
    is_parameter: i16,
    dont_count_hits: i16,
    with_hash: i16,
    hit_color: String,
    local_event_time: i64,
    age: i16,
    sex: i16,
    income: i16,
    interests: i16,
    robotness: i16,
    remote_ip: i32,
    window_name: i32,
    opener_name: i32,
    history_length: i16,
    browser_language: String,
    browser_country: String,
    social_network: String,
    social_action: String,
    http_error: i16,
    send_timing: i32,
    dns_timing: i32,
    connect_timing: i32,
    response_start_timing: i32,
    response_end_timing: i32,
    fetch_timing: i32,
    social_source_network_id: i16,
    social_source_page: String,
    param_price: i64,
    param_order_id: String,
    param_currency: String,
    param_currency_id: i16,
    openstat_service_name: String,
    openstat_campaign_id: String,
    openstat_ad_id: String,
    openstat_source_id: String,
    utm_source: String,
    utm_medium: String,
    utm_campaign: String,
    utm_content: String,
    utm_term: String,
    from_tag: String,
    has_gclid: i16,
    referer_hash: i64,
    url_hash: i64,
    clid: i32,
}

impl StreamingDataset for HitsDataset {
    fn table_name(&self) -> &'static str {
        "hits"
    }

    fn dataset_type(&self) -> DatasetType {
        DatasetType::Hits
    }

    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::similar_names,
        clippy::cast_possible_wrap
    )]
    fn generate(&self, scale_factor: f64) -> Result<Vec<RecordBatch>> {
        // ClickBench full dataset has ~100M rows, scale accordingly
        // SF=0.01 -> 1M rows, SF=0.001 -> 100K rows, SF=0.0001 -> 10K rows
        let row_count = (100_000_000.0 * scale_factor) as usize;
        let row_count = row_count.max(100); // At least 100 rows

        println!("Generating ClickBench hits data: {row_count} rows (scale factor {scale_factor})");

        let mut rng = StdRng::seed_from_u64(42);

        // Pre-allocate vectors
        let mut watch_ids = Vec::with_capacity(row_count);
        let mut java_enables = Vec::with_capacity(row_count);
        let mut titles = Vec::with_capacity(row_count);
        let mut good_events = Vec::with_capacity(row_count);
        let mut event_times = Vec::with_capacity(row_count);
        let mut event_dates = Vec::with_capacity(row_count);
        let mut counter_ids = Vec::with_capacity(row_count);
        let mut client_ips = Vec::with_capacity(row_count);
        let mut region_ids = Vec::with_capacity(row_count);
        let mut user_ids = Vec::with_capacity(row_count);
        let mut counter_classes = Vec::with_capacity(row_count);
        let mut os_values = Vec::with_capacity(row_count);
        let mut user_agents = Vec::with_capacity(row_count);
        let mut urls = Vec::with_capacity(row_count);
        let mut referers = Vec::with_capacity(row_count);
        let mut is_refreshes = Vec::with_capacity(row_count);
        let mut referer_category_ids = Vec::with_capacity(row_count);
        let mut referer_region_ids = Vec::with_capacity(row_count);
        let mut url_category_ids = Vec::with_capacity(row_count);
        let mut url_region_ids = Vec::with_capacity(row_count);
        let mut resolution_widths = Vec::with_capacity(row_count);
        let mut resolution_heights = Vec::with_capacity(row_count);
        let mut resolution_depths = Vec::with_capacity(row_count);
        let mut flash_majors = Vec::with_capacity(row_count);
        let mut flash_minors = Vec::with_capacity(row_count);
        let mut flash_minor2s = Vec::with_capacity(row_count);
        let mut net_majors = Vec::with_capacity(row_count);
        let mut net_minors = Vec::with_capacity(row_count);
        let mut user_agent_majors = Vec::with_capacity(row_count);
        let mut user_agent_minors = Vec::with_capacity(row_count);
        let mut cookie_enables = Vec::with_capacity(row_count);
        let mut javascript_enables = Vec::with_capacity(row_count);
        let mut is_mobiles = Vec::with_capacity(row_count);
        let mut mobile_phones = Vec::with_capacity(row_count);
        let mut mobile_phone_models = Vec::with_capacity(row_count);
        let mut params_vec = Vec::with_capacity(row_count);
        let mut ip_network_ids = Vec::with_capacity(row_count);
        let mut trafic_source_ids = Vec::with_capacity(row_count);
        let mut search_engine_ids = Vec::with_capacity(row_count);
        let mut search_phrases = Vec::with_capacity(row_count);
        let mut adv_engine_ids = Vec::with_capacity(row_count);
        let mut is_artificals = Vec::with_capacity(row_count);
        let mut window_client_widths = Vec::with_capacity(row_count);
        let mut window_client_heights = Vec::with_capacity(row_count);
        let mut client_time_zones = Vec::with_capacity(row_count);
        let mut client_event_times = Vec::with_capacity(row_count);
        let mut silverlight_version1s = Vec::with_capacity(row_count);
        let mut silverlight_version2s = Vec::with_capacity(row_count);
        let mut silverlight_version3s = Vec::with_capacity(row_count);
        let mut silverlight_version4s = Vec::with_capacity(row_count);
        let mut page_charsets = Vec::with_capacity(row_count);
        let mut code_versions = Vec::with_capacity(row_count);
        let mut is_links = Vec::with_capacity(row_count);
        let mut is_downloads = Vec::with_capacity(row_count);
        let mut is_not_bounces = Vec::with_capacity(row_count);
        let mut f_uniq_ids = Vec::with_capacity(row_count);
        let mut original_urls = Vec::with_capacity(row_count);
        let mut hids = Vec::with_capacity(row_count);
        let mut is_old_counters = Vec::with_capacity(row_count);
        let mut is_events = Vec::with_capacity(row_count);
        let mut is_parameters = Vec::with_capacity(row_count);
        let mut dont_count_hits_vec = Vec::with_capacity(row_count);
        let mut with_hashes = Vec::with_capacity(row_count);
        let mut hit_colors = Vec::with_capacity(row_count);
        let mut local_event_times = Vec::with_capacity(row_count);
        let mut ages = Vec::with_capacity(row_count);
        let mut sexes = Vec::with_capacity(row_count);
        let mut incomes = Vec::with_capacity(row_count);
        let mut interests_vec = Vec::with_capacity(row_count);
        let mut robotnesses = Vec::with_capacity(row_count);
        let mut remote_ips = Vec::with_capacity(row_count);
        let mut window_names = Vec::with_capacity(row_count);
        let mut opener_names = Vec::with_capacity(row_count);
        let mut history_lengths = Vec::with_capacity(row_count);
        let mut browser_languages = Vec::with_capacity(row_count);
        let mut browser_countries = Vec::with_capacity(row_count);
        let mut social_networks = Vec::with_capacity(row_count);
        let mut social_actions = Vec::with_capacity(row_count);
        let mut http_errors = Vec::with_capacity(row_count);
        let mut send_timings = Vec::with_capacity(row_count);
        let mut dns_timings = Vec::with_capacity(row_count);
        let mut connect_timings = Vec::with_capacity(row_count);
        let mut response_start_timings = Vec::with_capacity(row_count);
        let mut response_end_timings = Vec::with_capacity(row_count);
        let mut fetch_timings = Vec::with_capacity(row_count);
        let mut social_source_network_ids = Vec::with_capacity(row_count);
        let mut social_source_pages = Vec::with_capacity(row_count);
        let mut param_prices = Vec::with_capacity(row_count);
        let mut param_order_ids = Vec::with_capacity(row_count);
        let mut param_currencies = Vec::with_capacity(row_count);
        let mut param_currency_ids = Vec::with_capacity(row_count);
        let mut openstat_service_names = Vec::with_capacity(row_count);
        let mut openstat_campaign_ids = Vec::with_capacity(row_count);
        let mut openstat_ad_ids = Vec::with_capacity(row_count);
        let mut openstat_source_ids = Vec::with_capacity(row_count);
        let mut utm_sources = Vec::with_capacity(row_count);
        let mut utm_mediums = Vec::with_capacity(row_count);
        let mut utm_campaigns = Vec::with_capacity(row_count);
        let mut utm_contents = Vec::with_capacity(row_count);
        let mut utm_terms = Vec::with_capacity(row_count);
        let mut from_tags = Vec::with_capacity(row_count);
        let mut has_gclids = Vec::with_capacity(row_count);
        let mut referer_hashes = Vec::with_capacity(row_count);
        let mut url_hashes = Vec::with_capacity(row_count);
        let mut clids = Vec::with_capacity(row_count);

        for i in 0..row_count {
            let row = Self::generate_row(&mut rng, (i + 1) as i64);

            watch_ids.push(row.watch_id);
            java_enables.push(row.java_enable);
            titles.push(row.title);
            good_events.push(row.good_event);
            event_times.push(row.event_time);
            event_dates.push(row.event_date);
            counter_ids.push(row.counter_id);
            client_ips.push(row.client_ip);
            region_ids.push(row.region_id);
            user_ids.push(row.user_id);
            counter_classes.push(row.counter_class);
            os_values.push(row.os);
            user_agents.push(row.user_agent);
            urls.push(row.url);
            referers.push(row.referer);
            is_refreshes.push(row.is_refresh);
            referer_category_ids.push(row.referer_category_id);
            referer_region_ids.push(row.referer_region_id);
            url_category_ids.push(row.url_category_id);
            url_region_ids.push(row.url_region_id);
            resolution_widths.push(row.resolution_width);
            resolution_heights.push(row.resolution_height);
            resolution_depths.push(row.resolution_depth);
            flash_majors.push(row.flash_major);
            flash_minors.push(row.flash_minor);
            flash_minor2s.push(row.flash_minor2);
            net_majors.push(row.net_major);
            net_minors.push(row.net_minor);
            user_agent_majors.push(row.user_agent_major);
            user_agent_minors.push(row.user_agent_minor);
            cookie_enables.push(row.cookie_enable);
            javascript_enables.push(row.javascript_enable);
            is_mobiles.push(row.is_mobile);
            mobile_phones.push(row.mobile_phone);
            mobile_phone_models.push(row.mobile_phone_model);
            params_vec.push(row.params);
            ip_network_ids.push(row.ip_network_id);
            trafic_source_ids.push(row.trafic_source_id);
            search_engine_ids.push(row.search_engine_id);
            search_phrases.push(row.search_phrase);
            adv_engine_ids.push(row.adv_engine_id);
            is_artificals.push(row.is_artifical);
            window_client_widths.push(row.window_client_width);
            window_client_heights.push(row.window_client_height);
            client_time_zones.push(row.client_time_zone);
            client_event_times.push(row.client_event_time);
            silverlight_version1s.push(row.silverlight_version1);
            silverlight_version2s.push(row.silverlight_version2);
            silverlight_version3s.push(row.silverlight_version3);
            silverlight_version4s.push(row.silverlight_version4);
            page_charsets.push(row.page_charset);
            code_versions.push(row.code_version);
            is_links.push(row.is_link);
            is_downloads.push(row.is_download);
            is_not_bounces.push(row.is_not_bounce);
            f_uniq_ids.push(row.f_uniq_id);
            original_urls.push(row.original_url);
            hids.push(row.hid);
            is_old_counters.push(row.is_old_counter);
            is_events.push(row.is_event);
            is_parameters.push(row.is_parameter);
            dont_count_hits_vec.push(row.dont_count_hits);
            with_hashes.push(row.with_hash);
            hit_colors.push(row.hit_color);
            local_event_times.push(row.local_event_time);
            ages.push(row.age);
            sexes.push(row.sex);
            incomes.push(row.income);
            interests_vec.push(row.interests);
            robotnesses.push(row.robotness);
            remote_ips.push(row.remote_ip);
            window_names.push(row.window_name);
            opener_names.push(row.opener_name);
            history_lengths.push(row.history_length);
            browser_languages.push(row.browser_language);
            browser_countries.push(row.browser_country);
            social_networks.push(row.social_network);
            social_actions.push(row.social_action);
            http_errors.push(row.http_error);
            send_timings.push(row.send_timing);
            dns_timings.push(row.dns_timing);
            connect_timings.push(row.connect_timing);
            response_start_timings.push(row.response_start_timing);
            response_end_timings.push(row.response_end_timing);
            fetch_timings.push(row.fetch_timing);
            social_source_network_ids.push(row.social_source_network_id);
            social_source_pages.push(row.social_source_page);
            param_prices.push(row.param_price);
            param_order_ids.push(row.param_order_id);
            param_currencies.push(row.param_currency);
            param_currency_ids.push(row.param_currency_id);
            openstat_service_names.push(row.openstat_service_name);
            openstat_campaign_ids.push(row.openstat_campaign_id);
            openstat_ad_ids.push(row.openstat_ad_id);
            openstat_source_ids.push(row.openstat_source_id);
            utm_sources.push(row.utm_source);
            utm_mediums.push(row.utm_medium);
            utm_campaigns.push(row.utm_campaign);
            utm_contents.push(row.utm_content);
            utm_terms.push(row.utm_term);
            from_tags.push(row.from_tag);
            has_gclids.push(row.has_gclid);
            referer_hashes.push(row.referer_hash);
            url_hashes.push(row.url_hash);
            clids.push(row.clid);

            if (i + 1) % 100_000 == 0 {
                println!("Generated {} rows...", i + 1);
            }
        }

        println!("Generated {row_count} hits records");

        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(watch_ids)),
                Arc::new(Int16Array::from(java_enables)),
                Arc::new(StringArray::from(titles)),
                Arc::new(Int16Array::from(good_events)),
                Arc::new(TimestampMicrosecondArray::from(event_times)),
                Arc::new(Date32Array::from(event_dates)),
                Arc::new(Int32Array::from(counter_ids)),
                Arc::new(Int32Array::from(client_ips)),
                Arc::new(Int32Array::from(region_ids)),
                Arc::new(Int64Array::from(user_ids)),
                Arc::new(Int16Array::from(counter_classes)),
                Arc::new(Int16Array::from(os_values)),
                Arc::new(Int16Array::from(user_agents)),
                Arc::new(StringArray::from(urls)),
                Arc::new(StringArray::from(referers)),
                Arc::new(Int16Array::from(is_refreshes)),
                Arc::new(Int16Array::from(referer_category_ids)),
                Arc::new(Int32Array::from(referer_region_ids)),
                Arc::new(Int16Array::from(url_category_ids)),
                Arc::new(Int32Array::from(url_region_ids)),
                Arc::new(Int16Array::from(resolution_widths)),
                Arc::new(Int16Array::from(resolution_heights)),
                Arc::new(Int16Array::from(resolution_depths)),
                Arc::new(Int16Array::from(flash_majors)),
                Arc::new(Int16Array::from(flash_minors)),
                Arc::new(StringArray::from(flash_minor2s)),
                Arc::new(Int16Array::from(net_majors)),
                Arc::new(Int16Array::from(net_minors)),
                Arc::new(Int16Array::from(user_agent_majors)),
                Arc::new(StringArray::from(user_agent_minors)),
                Arc::new(Int16Array::from(cookie_enables)),
                Arc::new(Int16Array::from(javascript_enables)),
                Arc::new(Int16Array::from(is_mobiles)),
                Arc::new(Int16Array::from(mobile_phones)),
                Arc::new(StringArray::from(mobile_phone_models)),
                Arc::new(StringArray::from(params_vec)),
                Arc::new(Int32Array::from(ip_network_ids)),
                Arc::new(Int16Array::from(trafic_source_ids)),
                Arc::new(Int16Array::from(search_engine_ids)),
                Arc::new(StringArray::from(search_phrases)),
                Arc::new(Int16Array::from(adv_engine_ids)),
                Arc::new(Int16Array::from(is_artificals)),
                Arc::new(Int16Array::from(window_client_widths)),
                Arc::new(Int16Array::from(window_client_heights)),
                Arc::new(Int16Array::from(client_time_zones)),
                Arc::new(TimestampMicrosecondArray::from(client_event_times)),
                Arc::new(Int16Array::from(silverlight_version1s)),
                Arc::new(Int16Array::from(silverlight_version2s)),
                Arc::new(Int32Array::from(silverlight_version3s)),
                Arc::new(Int16Array::from(silverlight_version4s)),
                Arc::new(StringArray::from(page_charsets)),
                Arc::new(Int32Array::from(code_versions)),
                Arc::new(Int16Array::from(is_links)),
                Arc::new(Int16Array::from(is_downloads)),
                Arc::new(Int16Array::from(is_not_bounces)),
                Arc::new(Int64Array::from(f_uniq_ids)),
                Arc::new(StringArray::from(original_urls)),
                Arc::new(Int32Array::from(hids)),
                Arc::new(Int16Array::from(is_old_counters)),
                Arc::new(Int16Array::from(is_events)),
                Arc::new(Int16Array::from(is_parameters)),
                Arc::new(Int16Array::from(dont_count_hits_vec)),
                Arc::new(Int16Array::from(with_hashes)),
                Arc::new(StringArray::from(hit_colors)),
                Arc::new(TimestampMicrosecondArray::from(local_event_times)),
                Arc::new(Int16Array::from(ages)),
                Arc::new(Int16Array::from(sexes)),
                Arc::new(Int16Array::from(incomes)),
                Arc::new(Int16Array::from(interests_vec)),
                Arc::new(Int16Array::from(robotnesses)),
                Arc::new(Int32Array::from(remote_ips)),
                Arc::new(Int32Array::from(window_names)),
                Arc::new(Int32Array::from(opener_names)),
                Arc::new(Int16Array::from(history_lengths)),
                Arc::new(StringArray::from(browser_languages)),
                Arc::new(StringArray::from(browser_countries)),
                Arc::new(StringArray::from(social_networks)),
                Arc::new(StringArray::from(social_actions)),
                Arc::new(Int16Array::from(http_errors)),
                Arc::new(Int32Array::from(send_timings)),
                Arc::new(Int32Array::from(dns_timings)),
                Arc::new(Int32Array::from(connect_timings)),
                Arc::new(Int32Array::from(response_start_timings)),
                Arc::new(Int32Array::from(response_end_timings)),
                Arc::new(Int32Array::from(fetch_timings)),
                Arc::new(Int16Array::from(social_source_network_ids)),
                Arc::new(StringArray::from(social_source_pages)),
                Arc::new(Int64Array::from(param_prices)),
                Arc::new(StringArray::from(param_order_ids)),
                Arc::new(StringArray::from(param_currencies)),
                Arc::new(Int16Array::from(param_currency_ids)),
                Arc::new(StringArray::from(openstat_service_names)),
                Arc::new(StringArray::from(openstat_campaign_ids)),
                Arc::new(StringArray::from(openstat_ad_ids)),
                Arc::new(StringArray::from(openstat_source_ids)),
                Arc::new(StringArray::from(utm_sources)),
                Arc::new(StringArray::from(utm_mediums)),
                Arc::new(StringArray::from(utm_campaigns)),
                Arc::new(StringArray::from(utm_contents)),
                Arc::new(StringArray::from(utm_terms)),
                Arc::new(StringArray::from(from_tags)),
                Arc::new(Int16Array::from(has_gclids)),
                Arc::new(Int64Array::from(referer_hashes)),
                Arc::new(Int64Array::from(url_hashes)),
                Arc::new(Int32Array::from(clids)),
            ],
        )
        .context("Failed to create Arrow RecordBatch")?;

        Ok(vec![batch])
    }

    fn marker_record(&self) -> Result<RecordBatch> {
        // Create a marker record with WatchID = -1
        // Base timestamp for marker
        let marker_timestamp = 0i64;

        let batch = RecordBatch::try_new(
            Arc::new(Self::schema()),
            vec![
                Arc::new(Int64Array::from(vec![-1i64])),     // WatchID
                Arc::new(Int16Array::from(vec![0i16])),      // JavaEnable
                Arc::new(StringArray::from(vec!["MARKER"])), // Title
                Arc::new(Int16Array::from(vec![0i16])),      // GoodEvent
                Arc::new(TimestampMicrosecondArray::from(vec![marker_timestamp])), // EventTime
                Arc::new(Date32Array::from(vec![0i32])),     // EventDate
                Arc::new(Int32Array::from(vec![0i32])),      // CounterID
                Arc::new(Int32Array::from(vec![0i32])),      // ClientIP
                Arc::new(Int32Array::from(vec![0i32])),      // RegionID
                Arc::new(Int64Array::from(vec![0i64])),      // UserID
                Arc::new(Int16Array::from(vec![0i16])),      // CounterClass
                Arc::new(Int16Array::from(vec![0i16])),      // OS
                Arc::new(Int16Array::from(vec![0i16])),      // UserAgent
                Arc::new(StringArray::from(vec![""])),       // URL
                Arc::new(StringArray::from(vec![""])),       // Referer
                Arc::new(Int16Array::from(vec![0i16])),      // IsRefresh
                Arc::new(Int16Array::from(vec![0i16])),      // RefererCategoryID
                Arc::new(Int32Array::from(vec![0i32])),      // RefererRegionID
                Arc::new(Int16Array::from(vec![0i16])),      // URLCategoryID
                Arc::new(Int32Array::from(vec![0i32])),      // URLRegionID
                Arc::new(Int16Array::from(vec![0i16])),      // ResolutionWidth
                Arc::new(Int16Array::from(vec![0i16])),      // ResolutionHeight
                Arc::new(Int16Array::from(vec![0i16])),      // ResolutionDepth
                Arc::new(Int16Array::from(vec![0i16])),      // FlashMajor
                Arc::new(Int16Array::from(vec![0i16])),      // FlashMinor
                Arc::new(StringArray::from(vec![""])),       // FlashMinor2
                Arc::new(Int16Array::from(vec![0i16])),      // NetMajor
                Arc::new(Int16Array::from(vec![0i16])),      // NetMinor
                Arc::new(Int16Array::from(vec![0i16])),      // UserAgentMajor
                Arc::new(StringArray::from(vec![""])),       // UserAgentMinor
                Arc::new(Int16Array::from(vec![0i16])),      // CookieEnable
                Arc::new(Int16Array::from(vec![0i16])),      // JavascriptEnable
                Arc::new(Int16Array::from(vec![0i16])),      // IsMobile
                Arc::new(Int16Array::from(vec![0i16])),      // MobilePhone
                Arc::new(StringArray::from(vec![""])),       // MobilePhoneModel
                Arc::new(StringArray::from(vec![""])),       // Params
                Arc::new(Int32Array::from(vec![0i32])),      // IPNetworkID
                Arc::new(Int16Array::from(vec![0i16])),      // TraficSourceID
                Arc::new(Int16Array::from(vec![0i16])),      // SearchEngineID
                Arc::new(StringArray::from(vec![""])),       // SearchPhrase
                Arc::new(Int16Array::from(vec![0i16])),      // AdvEngineID
                Arc::new(Int16Array::from(vec![0i16])),      // IsArtifical
                Arc::new(Int16Array::from(vec![0i16])),      // WindowClientWidth
                Arc::new(Int16Array::from(vec![0i16])),      // WindowClientHeight
                Arc::new(Int16Array::from(vec![0i16])),      // ClientTimeZone
                Arc::new(TimestampMicrosecondArray::from(vec![marker_timestamp])), // ClientEventTime
                Arc::new(Int16Array::from(vec![0i16])), // SilverlightVersion1
                Arc::new(Int16Array::from(vec![0i16])), // SilverlightVersion2
                Arc::new(Int32Array::from(vec![0i32])), // SilverlightVersion3
                Arc::new(Int16Array::from(vec![0i16])), // SilverlightVersion4
                Arc::new(StringArray::from(vec![""])),  // PageCharset
                Arc::new(Int32Array::from(vec![0i32])), // CodeVersion
                Arc::new(Int16Array::from(vec![0i16])), // IsLink
                Arc::new(Int16Array::from(vec![0i16])), // IsDownload
                Arc::new(Int16Array::from(vec![0i16])), // IsNotBounce
                Arc::new(Int64Array::from(vec![0i64])), // FUniqID
                Arc::new(StringArray::from(vec![""])),  // OriginalURL
                Arc::new(Int32Array::from(vec![0i32])), // HID
                Arc::new(Int16Array::from(vec![0i16])), // IsOldCounter
                Arc::new(Int16Array::from(vec![0i16])), // IsEvent
                Arc::new(Int16Array::from(vec![0i16])), // IsParameter
                Arc::new(Int16Array::from(vec![0i16])), // DontCountHits
                Arc::new(Int16Array::from(vec![0i16])), // WithHash
                Arc::new(StringArray::from(vec!["M"])), // HitColor
                Arc::new(TimestampMicrosecondArray::from(vec![marker_timestamp])), // LocalEventTime
                Arc::new(Int16Array::from(vec![0i16])), // Age
                Arc::new(Int16Array::from(vec![0i16])), // Sex
                Arc::new(Int16Array::from(vec![0i16])), // Income
                Arc::new(Int16Array::from(vec![0i16])), // Interests
                Arc::new(Int16Array::from(vec![0i16])), // Robotness
                Arc::new(Int32Array::from(vec![0i32])), // RemoteIP
                Arc::new(Int32Array::from(vec![0i32])), // WindowName
                Arc::new(Int32Array::from(vec![0i32])), // OpenerName
                Arc::new(Int16Array::from(vec![0i16])), // HistoryLength
                Arc::new(StringArray::from(vec![""])),  // BrowserLanguage
                Arc::new(StringArray::from(vec![""])),  // BrowserCountry
                Arc::new(StringArray::from(vec![""])),  // SocialNetwork
                Arc::new(StringArray::from(vec![""])),  // SocialAction
                Arc::new(Int16Array::from(vec![0i16])), // HTTPError
                Arc::new(Int32Array::from(vec![0i32])), // SendTiming
                Arc::new(Int32Array::from(vec![0i32])), // DNSTiming
                Arc::new(Int32Array::from(vec![0i32])), // ConnectTiming
                Arc::new(Int32Array::from(vec![0i32])), // ResponseStartTiming
                Arc::new(Int32Array::from(vec![0i32])), // ResponseEndTiming
                Arc::new(Int32Array::from(vec![0i32])), // FetchTiming
                Arc::new(Int16Array::from(vec![0i16])), // SocialSourceNetworkID
                Arc::new(StringArray::from(vec![""])),  // SocialSourcePage
                Arc::new(Int64Array::from(vec![0i64])), // ParamPrice
                Arc::new(StringArray::from(vec![""])),  // ParamOrderID
                Arc::new(StringArray::from(vec![""])),  // ParamCurrency
                Arc::new(Int16Array::from(vec![0i16])), // ParamCurrencyID
                Arc::new(StringArray::from(vec![""])),  // OpenstatServiceName
                Arc::new(StringArray::from(vec![""])),  // OpenstatCampaignID
                Arc::new(StringArray::from(vec![""])),  // OpenstatAdID
                Arc::new(StringArray::from(vec![""])),  // OpenstatSourceID
                Arc::new(StringArray::from(vec![""])),  // UTMSource
                Arc::new(StringArray::from(vec![""])),  // UTMMedium
                Arc::new(StringArray::from(vec![""])),  // UTMCampaign
                Arc::new(StringArray::from(vec![""])),  // UTMContent
                Arc::new(StringArray::from(vec![""])),  // UTMTerm
                Arc::new(StringArray::from(vec![""])),  // FromTag
                Arc::new(Int16Array::from(vec![0i16])), // HasGCLID
                Arc::new(Int64Array::from(vec![0i64])), // RefererHash
                Arc::new(Int64Array::from(vec![0i64])), // URLHash
                Arc::new(Int32Array::from(vec![0i32])), // CLID
            ],
        )
        .context("Failed to create marker RecordBatch")?;

        Ok(batch)
    }

    fn marker_detection_query(&self) -> String {
        format!(
            "SELECT COUNT(*) as cnt FROM {} WHERE \"WatchID\" = -1",
            self.table_name()
        )
    }

    fn schema(&self) -> Schema {
        Self::schema()
    }

    fn primary_key_columns(&self) -> Vec<&'static str> {
        vec!["WatchID"]
    }
}
