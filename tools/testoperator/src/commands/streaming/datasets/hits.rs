/*
Copyright 2026 The Spice.ai OSS Authors

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
//!
//! Loads the ClickBench hits dataset from S3/MinIO or downloads from ClickHouse.
//!
//! ## Environment Variables
//!
//! - `CLICKBENCH_S3_URI`: S3 URI to hits.parquet (e.g., `s3://bucket/path/hits.parquet`)
//! - `CLICKBENCH_S3_ENDPOINT`: S3/MinIO endpoint (optional, for MinIO)
//! - `CLICKBENCH_S3_ACCESS_KEY_ID`: S3 access key ID (required when using S3)
//! - `CLICKBENCH_S3_SECRET_ACCESS_KEY`: S3 secret access key (required when using S3)
//!
//! If no S3 configuration is provided, downloads from ClickHouse's public dataset.

use std::sync::Arc;

use arrow::array::{
    Date32Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::Connection;
use test_framework::anyhow::{Context, Result};

use super::DatasetType;
use crate::commands::streaming::traits::StreamingDataset;

/// Default URL for ClickBench hits dataset.
const CLICKBENCH_DEFAULT_URL: &str =
    "https://datasets.clickhouse.com/hits_compatible/hits.parquet";

/// `ClickBench` hits dataset.
///
/// Loads the ClickBench hits dataset from S3/MinIO or downloads from ClickHouse.
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

    /// Get the parquet source URL, configuring S3 if needed.
    fn get_parquet_source(conn: &Connection) -> Result<String> {
        // Check if S3 configuration is provided
        if let Ok(s3_uri) = std::env::var("CLICKBENCH_S3_URI") {
            let access_key = std::env::var("CLICKBENCH_S3_ACCESS_KEY_ID")
                .context("CLICKBENCH_S3_ACCESS_KEY_ID required when CLICKBENCH_S3_URI is set")?;
            let secret_key = std::env::var("CLICKBENCH_S3_SECRET_ACCESS_KEY")
                .context("CLICKBENCH_S3_SECRET_ACCESS_KEY required when CLICKBENCH_S3_URI is set")?;

            // Configure DuckDB S3 settings
            conn.execute_batch("INSTALL httpfs; LOAD httpfs;")
                .context("Failed to load httpfs extension")?;

            // Set S3 credentials
            conn.execute_batch(&format!(
                "SET s3_access_key_id='{access_key}';
                 SET s3_secret_access_key='{secret_key}';"
            ))
            .context("Failed to set S3 credentials")?;

            // Set endpoint if provided (for MinIO)
            if let Ok(endpoint) = std::env::var("CLICKBENCH_S3_ENDPOINT") {
                // Remove http:// or https:// prefix for DuckDB
                let endpoint_host = endpoint
                    .trim_start_matches("http://")
                    .trim_start_matches("https://");
                let use_ssl = endpoint.starts_with("https://");

                conn.execute_batch(&format!(
                    "SET s3_endpoint='{endpoint_host}';
                     SET s3_url_style='path';
                     SET s3_use_ssl={use_ssl};"
                ))
                .context("Failed to set S3 endpoint")?;
            }

            println!("Using S3 source: {s3_uri}");
            Ok(s3_uri)
        } else {
            // Use default ClickHouse URL
            conn.execute_batch("INSTALL httpfs; LOAD httpfs;")
                .context("Failed to load httpfs extension")?;

            println!("Using default ClickBench URL: {CLICKBENCH_DEFAULT_URL}");
            Ok(CLICKBENCH_DEFAULT_URL.to_string())
        }
    }
}

impl StreamingDataset for HitsDataset {
    fn table_name(&self) -> &'static str {
        "hits"
    }

    fn dataset_type(&self) -> DatasetType {
        DatasetType::Hits
    }

    fn generate(&self, scale_factor: f64) -> Result<Vec<RecordBatch>> {
        println!("Loading ClickBench hits data with scale factor {scale_factor}");

        let conn =
            Connection::open_in_memory().context("Failed to open in-memory DuckDB connection")?;

        let parquet_source = Self::get_parquet_source(&conn)?;

        // Calculate sample percentage (ClickBench has ~100M rows)
        // SF=1.0 -> 100%, SF=0.01 -> 1%, SF=0.001 -> 0.1%
        let sample_pct = (scale_factor * 100.0).min(100.0);

        // Build query with sampling
        let query = if sample_pct >= 100.0 {
            format!(
                "SELECT
                    WatchID, JavaEnable, Title, GoodEvent,
                    epoch_us(EventTime) as EventTime, EventDate, CounterID, ClientIP,
                    RegionID, UserID, CounterClass, OS, UserAgent, URL, Referer,
                    IsRefresh, RefererCategoryID, RefererRegionID, URLCategoryID, URLRegionID,
                    ResolutionWidth, ResolutionHeight, ResolutionDepth, FlashMajor, FlashMinor,
                    FlashMinor2, NetMajor, NetMinor, UserAgentMajor, UserAgentMinor,
                    CookieEnable, JavascriptEnable, IsMobile, MobilePhone, MobilePhoneModel,
                    Params, IPNetworkID, TraficSourceID, SearchEngineID, SearchPhrase,
                    AdvEngineID, IsArtifical, WindowClientWidth, WindowClientHeight, ClientTimeZone,
                    epoch_us(ClientEventTime) as ClientEventTime,
                    SilverlightVersion1, SilverlightVersion2, SilverlightVersion3, SilverlightVersion4,
                    PageCharset, CodeVersion, IsLink, IsDownload, IsNotBounce,
                    FUniqID, OriginalURL, HID, IsOldCounter, IsEvent,
                    IsParameter, DontCountHits, WithHash, HitColor,
                    epoch_us(LocalEventTime) as LocalEventTime,
                    Age, Sex, Income, Interests, Robotness,
                    RemoteIP, WindowName, OpenerName, HistoryLength, BrowserLanguage,
                    BrowserCountry, SocialNetwork, SocialAction, HTTPError, SendTiming,
                    DNSTiming, ConnectTiming, ResponseStartTiming, ResponseEndTiming, FetchTiming,
                    SocialSourceNetworkID, SocialSourcePage, ParamPrice, ParamOrderID, ParamCurrency,
                    ParamCurrencyID, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID,
                    UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm,
                    FromTag, HasGCLID, RefererHash, URLHash, CLID
                FROM read_parquet('{parquet_source}')"
            )
        } else {
            format!(
                "SELECT
                    WatchID, JavaEnable, Title, GoodEvent,
                    epoch_us(EventTime) as EventTime, EventDate, CounterID, ClientIP,
                    RegionID, UserID, CounterClass, OS, UserAgent, URL, Referer,
                    IsRefresh, RefererCategoryID, RefererRegionID, URLCategoryID, URLRegionID,
                    ResolutionWidth, ResolutionHeight, ResolutionDepth, FlashMajor, FlashMinor,
                    FlashMinor2, NetMajor, NetMinor, UserAgentMajor, UserAgentMinor,
                    CookieEnable, JavascriptEnable, IsMobile, MobilePhone, MobilePhoneModel,
                    Params, IPNetworkID, TraficSourceID, SearchEngineID, SearchPhrase,
                    AdvEngineID, IsArtifical, WindowClientWidth, WindowClientHeight, ClientTimeZone,
                    epoch_us(ClientEventTime) as ClientEventTime,
                    SilverlightVersion1, SilverlightVersion2, SilverlightVersion3, SilverlightVersion4,
                    PageCharset, CodeVersion, IsLink, IsDownload, IsNotBounce,
                    FUniqID, OriginalURL, HID, IsOldCounter, IsEvent,
                    IsParameter, DontCountHits, WithHash, HitColor,
                    epoch_us(LocalEventTime) as LocalEventTime,
                    Age, Sex, Income, Interests, Robotness,
                    RemoteIP, WindowName, OpenerName, HistoryLength, BrowserLanguage,
                    BrowserCountry, SocialNetwork, SocialAction, HTTPError, SendTiming,
                    DNSTiming, ConnectTiming, ResponseStartTiming, ResponseEndTiming, FetchTiming,
                    SocialSourceNetworkID, SocialSourcePage, ParamPrice, ParamOrderID, ParamCurrency,
                    ParamCurrencyID, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID,
                    UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm,
                    FromTag, HasGCLID, RefererHash, URLHash, CLID
                FROM read_parquet('{parquet_source}')
                USING SAMPLE {sample_pct} PERCENT (bernoulli)"
            )
        };

        println!("Executing query with {sample_pct}% sample...");

        let mut stmt = conn.prepare(&query).context("Failed to prepare query")?;

        // Collect data into vectors
        let mut watch_ids = Vec::new();
        let mut java_enables = Vec::new();
        let mut titles: Vec<Option<String>> = Vec::new();
        let mut good_events = Vec::new();
        let mut event_times = Vec::new();
        let mut event_dates = Vec::new();
        let mut counter_ids = Vec::new();
        let mut client_ips = Vec::new();
        let mut region_ids = Vec::new();
        let mut user_ids = Vec::new();
        let mut counter_classes = Vec::new();
        let mut os_values = Vec::new();
        let mut user_agents = Vec::new();
        let mut urls: Vec<Option<String>> = Vec::new();
        let mut referers: Vec<Option<String>> = Vec::new();
        let mut is_refreshes = Vec::new();
        let mut referer_category_ids = Vec::new();
        let mut referer_region_ids = Vec::new();
        let mut url_category_ids = Vec::new();
        let mut url_region_ids = Vec::new();
        let mut resolution_widths = Vec::new();
        let mut resolution_heights = Vec::new();
        let mut resolution_depths = Vec::new();
        let mut flash_majors = Vec::new();
        let mut flash_minors = Vec::new();
        let mut flash_minor2s: Vec<Option<String>> = Vec::new();
        let mut net_majors = Vec::new();
        let mut net_minors = Vec::new();
        let mut user_agent_majors = Vec::new();
        let mut user_agent_minors = Vec::new();
        let mut cookie_enables = Vec::new();
        let mut javascript_enables = Vec::new();
        let mut is_mobiles = Vec::new();
        let mut mobile_phones = Vec::new();
        let mut mobile_phone_models: Vec<Option<String>> = Vec::new();
        let mut params_vec: Vec<Option<String>> = Vec::new();
        let mut ip_network_ids = Vec::new();
        let mut trafic_source_ids = Vec::new();
        let mut search_engine_ids = Vec::new();
        let mut search_phrases: Vec<Option<String>> = Vec::new();
        let mut adv_engine_ids = Vec::new();
        let mut is_artificals = Vec::new();
        let mut window_client_widths = Vec::new();
        let mut window_client_heights = Vec::new();
        let mut client_time_zones = Vec::new();
        let mut client_event_times = Vec::new();
        let mut silverlight_version1s = Vec::new();
        let mut silverlight_version2s = Vec::new();
        let mut silverlight_version3s = Vec::new();
        let mut silverlight_version4s = Vec::new();
        let mut page_charsets: Vec<Option<String>> = Vec::new();
        let mut code_versions = Vec::new();
        let mut is_links = Vec::new();
        let mut is_downloads = Vec::new();
        let mut is_not_bounces = Vec::new();
        let mut f_uniq_ids = Vec::new();
        let mut original_urls: Vec<Option<String>> = Vec::new();
        let mut hids = Vec::new();
        let mut is_old_counters = Vec::new();
        let mut is_events = Vec::new();
        let mut is_parameters = Vec::new();
        let mut dont_count_hits_vec = Vec::new();
        let mut with_hashes = Vec::new();
        let mut hit_colors = Vec::new();
        let mut local_event_times = Vec::new();
        let mut ages = Vec::new();
        let mut sexes = Vec::new();
        let mut incomes = Vec::new();
        let mut interests_vec = Vec::new();
        let mut robotnesses = Vec::new();
        let mut remote_ips = Vec::new();
        let mut window_names = Vec::new();
        let mut opener_names = Vec::new();
        let mut history_lengths = Vec::new();
        let mut browser_languages: Vec<Option<String>> = Vec::new();
        let mut browser_countries: Vec<Option<String>> = Vec::new();
        let mut social_networks: Vec<Option<String>> = Vec::new();
        let mut social_actions: Vec<Option<String>> = Vec::new();
        let mut http_errors = Vec::new();
        let mut send_timings = Vec::new();
        let mut dns_timings = Vec::new();
        let mut connect_timings = Vec::new();
        let mut response_start_timings = Vec::new();
        let mut response_end_timings = Vec::new();
        let mut fetch_timings = Vec::new();
        let mut social_source_network_ids = Vec::new();
        let mut social_source_pages: Vec<Option<String>> = Vec::new();
        let mut param_prices = Vec::new();
        let mut param_order_ids: Vec<Option<String>> = Vec::new();
        let mut param_currencies: Vec<Option<String>> = Vec::new();
        let mut param_currency_ids = Vec::new();
        let mut openstat_service_names: Vec<Option<String>> = Vec::new();
        let mut openstat_campaign_ids: Vec<Option<String>> = Vec::new();
        let mut openstat_ad_ids: Vec<Option<String>> = Vec::new();
        let mut openstat_source_ids: Vec<Option<String>> = Vec::new();
        let mut utm_sources: Vec<Option<String>> = Vec::new();
        let mut utm_mediums: Vec<Option<String>> = Vec::new();
        let mut utm_campaigns: Vec<Option<String>> = Vec::new();
        let mut utm_contents: Vec<Option<String>> = Vec::new();
        let mut utm_terms: Vec<Option<String>> = Vec::new();
        let mut from_tags: Vec<Option<String>> = Vec::new();
        let mut has_gclids = Vec::new();
        let mut referer_hashes = Vec::new();
        let mut url_hashes = Vec::new();
        let mut clids = Vec::new();

        let mut rows = stmt.query([]).context("Failed to execute query")?;
        let mut row_count = 0usize;

        while let Some(row) = rows.next().context("Failed to read row")? {
            watch_ids.push(row.get::<_, i64>(0)?);
            java_enables.push(row.get::<_, i16>(1)?);
            titles.push(row.get::<_, Option<String>>(2)?);
            good_events.push(row.get::<_, i16>(3)?);
            event_times.push(row.get::<_, i64>(4)?);
            event_dates.push(row.get::<_, i32>(5)?);
            counter_ids.push(row.get::<_, i32>(6)?);
            client_ips.push(row.get::<_, i32>(7)?);
            region_ids.push(row.get::<_, i32>(8)?);
            user_ids.push(row.get::<_, i64>(9)?);
            counter_classes.push(row.get::<_, i16>(10)?);
            os_values.push(row.get::<_, i16>(11)?);
            user_agents.push(row.get::<_, i16>(12)?);
            urls.push(row.get::<_, Option<String>>(13)?);
            referers.push(row.get::<_, Option<String>>(14)?);
            is_refreshes.push(row.get::<_, i16>(15)?);
            referer_category_ids.push(row.get::<_, i16>(16)?);
            referer_region_ids.push(row.get::<_, i32>(17)?);
            url_category_ids.push(row.get::<_, i16>(18)?);
            url_region_ids.push(row.get::<_, i32>(19)?);
            resolution_widths.push(row.get::<_, i16>(20)?);
            resolution_heights.push(row.get::<_, i16>(21)?);
            resolution_depths.push(row.get::<_, i16>(22)?);
            flash_majors.push(row.get::<_, i16>(23)?);
            flash_minors.push(row.get::<_, i16>(24)?);
            flash_minor2s.push(row.get::<_, Option<String>>(25)?);
            net_majors.push(row.get::<_, i16>(26)?);
            net_minors.push(row.get::<_, i16>(27)?);
            user_agent_majors.push(row.get::<_, i16>(28)?);
            user_agent_minors.push(row.get::<_, String>(29)?);
            cookie_enables.push(row.get::<_, i16>(30)?);
            javascript_enables.push(row.get::<_, i16>(31)?);
            is_mobiles.push(row.get::<_, i16>(32)?);
            mobile_phones.push(row.get::<_, i16>(33)?);
            mobile_phone_models.push(row.get::<_, Option<String>>(34)?);
            params_vec.push(row.get::<_, Option<String>>(35)?);
            ip_network_ids.push(row.get::<_, i32>(36)?);
            trafic_source_ids.push(row.get::<_, i16>(37)?);
            search_engine_ids.push(row.get::<_, i16>(38)?);
            search_phrases.push(row.get::<_, Option<String>>(39)?);
            adv_engine_ids.push(row.get::<_, i16>(40)?);
            is_artificals.push(row.get::<_, i16>(41)?);
            window_client_widths.push(row.get::<_, i16>(42)?);
            window_client_heights.push(row.get::<_, i16>(43)?);
            client_time_zones.push(row.get::<_, i16>(44)?);
            client_event_times.push(row.get::<_, i64>(45)?);
            silverlight_version1s.push(row.get::<_, i16>(46)?);
            silverlight_version2s.push(row.get::<_, i16>(47)?);
            silverlight_version3s.push(row.get::<_, i32>(48)?);
            silverlight_version4s.push(row.get::<_, i16>(49)?);
            page_charsets.push(row.get::<_, Option<String>>(50)?);
            code_versions.push(row.get::<_, i32>(51)?);
            is_links.push(row.get::<_, i16>(52)?);
            is_downloads.push(row.get::<_, i16>(53)?);
            is_not_bounces.push(row.get::<_, i16>(54)?);
            f_uniq_ids.push(row.get::<_, i64>(55)?);
            original_urls.push(row.get::<_, Option<String>>(56)?);
            hids.push(row.get::<_, i32>(57)?);
            is_old_counters.push(row.get::<_, i16>(58)?);
            is_events.push(row.get::<_, i16>(59)?);
            is_parameters.push(row.get::<_, i16>(60)?);
            dont_count_hits_vec.push(row.get::<_, i16>(61)?);
            with_hashes.push(row.get::<_, i16>(62)?);
            hit_colors.push(row.get::<_, String>(63)?);
            local_event_times.push(row.get::<_, i64>(64)?);
            ages.push(row.get::<_, i16>(65)?);
            sexes.push(row.get::<_, i16>(66)?);
            incomes.push(row.get::<_, i16>(67)?);
            interests_vec.push(row.get::<_, i16>(68)?);
            robotnesses.push(row.get::<_, i16>(69)?);
            remote_ips.push(row.get::<_, i32>(70)?);
            window_names.push(row.get::<_, i32>(71)?);
            opener_names.push(row.get::<_, i32>(72)?);
            history_lengths.push(row.get::<_, i16>(73)?);
            browser_languages.push(row.get::<_, Option<String>>(74)?);
            browser_countries.push(row.get::<_, Option<String>>(75)?);
            social_networks.push(row.get::<_, Option<String>>(76)?);
            social_actions.push(row.get::<_, Option<String>>(77)?);
            http_errors.push(row.get::<_, i16>(78)?);
            send_timings.push(row.get::<_, i32>(79)?);
            dns_timings.push(row.get::<_, i32>(80)?);
            connect_timings.push(row.get::<_, i32>(81)?);
            response_start_timings.push(row.get::<_, i32>(82)?);
            response_end_timings.push(row.get::<_, i32>(83)?);
            fetch_timings.push(row.get::<_, i32>(84)?);
            social_source_network_ids.push(row.get::<_, i16>(85)?);
            social_source_pages.push(row.get::<_, Option<String>>(86)?);
            param_prices.push(row.get::<_, i64>(87)?);
            param_order_ids.push(row.get::<_, Option<String>>(88)?);
            param_currencies.push(row.get::<_, Option<String>>(89)?);
            param_currency_ids.push(row.get::<_, i16>(90)?);
            openstat_service_names.push(row.get::<_, Option<String>>(91)?);
            openstat_campaign_ids.push(row.get::<_, Option<String>>(92)?);
            openstat_ad_ids.push(row.get::<_, Option<String>>(93)?);
            openstat_source_ids.push(row.get::<_, Option<String>>(94)?);
            utm_sources.push(row.get::<_, Option<String>>(95)?);
            utm_mediums.push(row.get::<_, Option<String>>(96)?);
            utm_campaigns.push(row.get::<_, Option<String>>(97)?);
            utm_contents.push(row.get::<_, Option<String>>(98)?);
            utm_terms.push(row.get::<_, Option<String>>(99)?);
            from_tags.push(row.get::<_, Option<String>>(100)?);
            has_gclids.push(row.get::<_, i16>(101)?);
            referer_hashes.push(row.get::<_, i64>(102)?);
            url_hashes.push(row.get::<_, i64>(103)?);
            clids.push(row.get::<_, i32>(104)?);

            row_count += 1;
            if row_count.is_multiple_of(100_000) {
                println!("Loaded {row_count} rows...");
            }
        }

        println!("Loaded {row_count} hits records");

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
