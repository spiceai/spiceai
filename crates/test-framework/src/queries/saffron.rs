use super::{ParameterValue, Query};
use crate::{flight::query_to_batches, spiced::SpicedInstance};
use anyhow::{Context, Result};
use arrow::{
    array::{Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray},
    record_batch::RecordBatch,
};
use std::sync::Arc;

/// Creates a fixed `NumberInfoRecord` for consistent snapshotting
fn create_fixed_number_info_record() -> NumberInfoRecord {
    NumberInfoRecord {
        account_sid: "AC8ee2a82cf11c7c8860b192678c9e3066".to_string(),
        number_pool_sid: "NPe85ca269e9eb6b9268ffeab9d6ffaba9".to_string(),
        number_sid: "PNbd07a0546edbbd27fbf578fdf1581454".to_string(), // Not used in parameters
        max_rate: 20,
        number_did: "+16176906114".to_string(), // Not used in parameters
        number_type: "lc".to_string(),
        supported_dest_region: "US".to_string(), // Not used in parameters
        number_region: "US".to_string(),
        current_rate: 100, // Not used in parameters
        is_available: 1,   // Not used in parameters
        provider_sid: "OT22140ff77c26978211f29cf6663d75b5".to_string(), // Not used in parameters
        area_code_region: "617".to_string(),
        available_for_number_selection: 0, // Maps to selectA2pNumber flag = 0
        capability: "sms".to_string(),
    }
}

/// Defines fixed parameters for Saffron queries that can be used for snapshotting
#[must_use]
pub fn add_saffron_fixed_parameters(queries: Vec<Query>) -> Vec<Query> {
    let fixed_record = create_fixed_number_info_record();

    queries
        .into_iter()
        .map(|q| create_query_with_parameter_set(q, &fixed_record))
        .collect()
}

/// Generate randomized Saffron queries by fetching random parameter sets
/// from the `number_info_with_cap` table.
pub async fn generate_randomized_saffron_queries(
    queries: Vec<Query>,
    instance: &SpicedInstance,
    random_param_set_count: usize,
    query_overrides: Option<super::QueryOverrides>,
) -> Result<Vec<Query>> {
    let records = fetch_test_records(instance, random_param_set_count, query_overrides).await?;

    if records.is_empty() {
        anyhow::bail!("No test records found in number_info_with_cap table");
    }
    Ok(generate_queries_with_dynamic_parameters(queries, &records))
}

/// Fetch random test records from the Spice instance.
async fn fetch_test_records(
    instance: &SpicedInstance,
    limit: usize,
    query_overrides: Option<super::QueryOverrides>,
) -> Result<Vec<NumberInfoRecord>> {
    println!("Fetching {limit} random NumberInfoRecord records...");

    let spice_client = Arc::new(instance.spice_client(None, false).await?);

    let query = match query_overrides {
        Some(super::QueryOverrides::SaffronViews) => {
            // Custom query for SaffronViews scenario
            format!(
                "SELECT DateCreated, DateUpdated, AccountSid, NumberPoolSid, NumberSid, \
                 MaxRate, NumberDid, NumberType, SupportedDestRegion, NumberRegion, \
                 CurrentRate, IsAvailable, ProviderSid, AreaCodeRegion, \
                 AvailableForNumberSelection, Capability \
                 FROM number_info_with_cap \
                 ORDER BY RAND() \
                 LIMIT {limit}"
            )
        }
        _ => {
            // Default query for standard scenario
            format!(
                "SELECT A.DateCreated, A.DateUpdated, A.AccountSid, A.NumberPoolSid, A.NumberSid, \
                 A.MaxRate, A.NumberDid, A.NumberType, A.SupportedDestRegion, A.NumberRegion, \
                 A.CurrentRate, A.IsAvailable, A.ProviderSid, A.AreaCodeRegion, \
                 A.AvailableForNumberSelection, B.Capability \
                 FROM number_info AS A \
                 INNER JOIN number_caps AS B ON A.NumberSid = B.NumberSid \
                 ORDER BY RAND() \
                 LIMIT {limit}"
            )
        }
    };

    let batches = query_to_batches(Arc::clone(&spice_client), &query, None)
        .await
        .context("Failed to execute test record query")?;

    let mut records = Vec::new();
    for (batch_idx, batch) in batches.iter().enumerate() {
        for row in 0..batch.num_rows() {
            let record = NumberInfoRecord::from_record_batch(batch, row).map_err(|err| {
                anyhow::anyhow!(
                    "Failed to extract sample record for batch {batch_idx}, row {row}: {err}"
                )
            })?;
            records.push(record);
        }
    }
    println!("Extracted {} sample records", records.len());
    Ok(records)
}

/// Generate multiple queries for each record (parameter set).
fn generate_queries_with_dynamic_parameters(
    queries: Vec<Query>,
    records: &[NumberInfoRecord],
) -> Vec<Query> {
    queries
        .into_iter()
        .flat_map(|base_query| {
            records
                .iter()
                .map(move |record| create_query_with_parameter_set(base_query.clone(), record))
        })
        .collect()
}

/// Build a parameter set dynamically from a record and return the complete query.
#[allow(clippy::too_many_lines)]
fn create_query_with_parameter_set(mut query: Query, record: &NumberInfoRecord) -> Query {
    let qtype = query.name.strip_prefix("saffron_").unwrap_or(&query.name);

    query.parameters = match qtype {
        "q1" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String("PN00000000000000000000000000000099".into()), // Throttled NumberSid
            ParameterValue::String(record.capability.clone().into()),
            ParameterValue::Number(i64::from(record.available_for_number_selection > 0)), // selectA2pNumber flag
        ]),
        "q2" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String(record.number_type.clone().into()),
            ParameterValue::Number(i64::from(record.max_rate)),
            ParameterValue::String("PN00000000000000000000000000000099".into()), // Throttled NumberSid (NOT IN)
            ParameterValue::Number(i64::from(record.available_for_number_selection > 0)), // selectA2pNumber flag
            ParameterValue::String(record.capability.clone().into()),
            ParameterValue::Number(0), // LIMIT offset
        ]),
        "q3" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String(record.number_region.clone().into()),
            ParameterValue::String(record.number_type.clone().into()),
            ParameterValue::String(record.area_code_region.clone().into()),
            ParameterValue::String("PN00000000000000000000000000000099".into()), // Throttled NumberSid (NOT IN)
            ParameterValue::String(record.capability.clone().into()),
            ParameterValue::Number(i64::from(record.available_for_number_selection > 0)), // selectA2pNumber flag
        ]),
        "q4" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String(record.number_region.clone().into()),
            ParameterValue::String(record.number_type.clone().into()),
            ParameterValue::String(record.area_code_region.clone().into()),
            ParameterValue::Number(i64::from(record.max_rate)),
            ParameterValue::String("PN00000000000000000000000000000099".into()), // Throttled NumberSid (NOT IN)
            ParameterValue::String(record.capability.clone().into()),
            ParameterValue::Number(i64::from(record.available_for_number_selection > 0)), // selectA2pNumber flag
            ParameterValue::Number(0), // LIMIT offset
        ]),
        "q5" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String(record.number_region.clone().into()),
            ParameterValue::String(record.number_type.clone().into()),
            ParameterValue::String("PN00000000000000000000000000000099".into()), // Throttled NumberSid (NOT IN)
            ParameterValue::String(record.capability.clone().into()),
            ParameterValue::Number(i64::from(record.available_for_number_selection > 0)), // selectA2pNumber flag
        ]),
        "q6" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String(record.number_region.clone().into()),
            ParameterValue::String(record.number_type.clone().into()),
            ParameterValue::Number(i64::from(record.max_rate)),
            ParameterValue::String("PN00000000000000000000000000000099".into()), // Throttled NumberSid (NOT IN)
            ParameterValue::Number(i64::from(record.available_for_number_selection > 0)), // selectA2pNumber flag
            ParameterValue::String(record.capability.clone().into()),
            ParameterValue::Number(0), // LIMIT offset
        ]),
        "q7" => Some(vec![
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_region.clone().into()), // SupportedDestRegion filter
            ParameterValue::String(record.number_region.clone().into()), // NumberRegion filter
            ParameterValue::String("PN00000000000000000000000000000099".into()), // Throttled NumberSid (NOT IN)
            ParameterValue::String(record.number_type.clone().into()), // NumberType filter
            ParameterValue::String(record.capability.clone().into()),
            ParameterValue::Number(20), // LIMIT value
        ]),
        "q8" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String("PN00000000000000000000000000000099".into()), // Throttled NumberSid (NOT IN)
            ParameterValue::String(record.capability.clone().into()),
            ParameterValue::Number(20), // LIMIT value
        ]),
        "q9" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String(record.number_type.clone().into()), // SenderType
            ParameterValue::Number(0), // OFFSET value (default to first record)
        ]),
        "q10" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String(record.number_type.clone().into()), // SenderType
            ParameterValue::String(record.number_region.clone().into()), // Region
            ParameterValue::Number(0), // OFFSET value (default to first record)
        ]),
        "q11" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_pool_sid.clone().into()),
            ParameterValue::String(record.number_did.clone().into()), // NumberDid for identity lookup
        ]),
        "q12" => Some(vec![
            ParameterValue::String(record.account_sid.clone().into()),
            ParameterValue::String(record.number_did.clone().into()), // NumberDid used as SenderIdentity
        ]),
        _ => None,
    };

    query
}

#[derive(Debug, Clone)]
pub struct NumberInfoRecord {
    pub account_sid: String,
    pub number_pool_sid: String,
    pub number_sid: String,
    pub max_rate: i32,
    pub number_did: String,
    pub number_type: String,
    pub supported_dest_region: String,
    pub number_region: String,
    pub current_rate: i32,
    pub is_available: i32,
    pub provider_sid: String,
    pub area_code_region: String,
    pub available_for_number_selection: i32,
    pub capability: String,
}

impl NumberInfoRecord {
    pub fn from_record_batch(batch: &RecordBatch, row: usize) -> Result<Self> {
        Ok(Self {
            account_sid: get_string(batch, "AccountSid", row)?,
            number_pool_sid: get_string(batch, "NumberPoolSid", row)?,
            number_sid: get_string(batch, "NumberSid", row)?,
            max_rate: get_i32(batch, "MaxRate", row)?,
            number_did: get_string(batch, "NumberDid", row)?,
            number_type: get_string(batch, "NumberType", row)?,
            supported_dest_region: get_string(batch, "SupportedDestRegion", row)?,
            number_region: get_string(batch, "NumberRegion", row)?,
            current_rate: get_i32(batch, "CurrentRate", row)?,
            is_available: get_i32(batch, "IsAvailable", row)?,
            provider_sid: get_string(batch, "ProviderSid", row)?,
            area_code_region: get_string(batch, "AreaCodeRegion", row)?,
            available_for_number_selection: get_i32(batch, "AvailableForNumberSelection", row)?,
            capability: get_string(batch, "Capability", row)?,
        })
    }
}

fn get_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a dyn Array> {
    let (idx, _) = batch
        .schema()
        .column_with_name(name)
        .with_context(|| format!("Column '{name}' not found"))?;
    Ok(batch.column(idx))
}

fn get_string(batch: &RecordBatch, col: &str, row: usize) -> Result<String> {
    let arr = get_column(batch, col)?;

    // Try StringArray first (Utf8)
    if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
        if str_arr.is_null(row) {
            Ok(String::new()) // Return empty string for null values instead of error
        } else {
            Ok(str_arr.value(row).to_string())
        }
    }
    // Try LargeStringArray (LargeUtf8)
    else if let Some(large_str_arr) = arr.as_any().downcast_ref::<LargeStringArray>() {
        if large_str_arr.is_null(row) {
            Ok(String::new()) // Return empty string for null values instead of error
        } else {
            Ok(large_str_arr.value(row).to_string())
        }
    } else {
        Err(anyhow::anyhow!(
            "Column '{}' is neither a StringArray nor LargeStringArray, actual type: {:?}",
            col,
            arr.data_type()
        ))
    }
}

fn get_i32(batch: &RecordBatch, col: &str, row: usize) -> Result<i32> {
    let arr = get_column(batch, col)?;

    if let Some(i8_arr) = arr.as_any().downcast_ref::<Int8Array>() {
        if i8_arr.is_null(row) {
            Ok(0)
        } else {
            Ok(i32::from(i8_arr.value(row)))
        }
    } else if let Some(i16_arr) = arr.as_any().downcast_ref::<Int16Array>() {
        if i16_arr.is_null(row) {
            Ok(0)
        } else {
            Ok(i32::from(i16_arr.value(row)))
        }
    } else if let Some(i32_arr) = arr.as_any().downcast_ref::<Int32Array>() {
        if i32_arr.is_null(row) {
            Ok(0)
        } else {
            Ok(i32_arr.value(row))
        }
    } else if let Some(i64_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        if i64_arr.is_null(row) {
            Ok(0)
        } else {
            Ok(i32::try_from(i64_arr.value(row)).unwrap_or(0))
        }
    } else {
        Err(anyhow::anyhow!(
            "Column '{}' is not an integer array, actual type: {:?}",
            col,
            arr.data_type()
        ))
    }
}
