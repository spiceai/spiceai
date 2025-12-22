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

use std::sync::Arc;

use arrow::datatypes::DataType;

use super::Query;

// DataFusion has ParamValues which can define a list of `Vec<ScalarValue>`
// This is a scaled down equivalent to the `ScalarValue` enum, but we don't want to import DataFusion just for this.
#[derive(Debug, Clone)]
pub enum ParameterValue {
    String(Arc<str>),
    Number(i64),
    Float(f64),
}

impl ParameterValue {
    #[must_use]
    pub fn dtype(&self) -> DataType {
        match self {
            ParameterValue::String(_) => DataType::Utf8,
            ParameterValue::Number(_) => DataType::Int64,
            ParameterValue::Float(_) => DataType::Float64,
        }
    }

    #[must_use]
    pub fn array(&self) -> Arc<dyn arrow::array::Array> {
        match self {
            ParameterValue::String(value) => {
                Arc::new(arrow::array::StringArray::from(vec![value.as_ref()]))
            }
            ParameterValue::Number(value) => Arc::new(arrow::array::Int64Array::from(vec![*value])),
            ParameterValue::Float(value) => {
                #[expect(clippy::cast_possible_truncation)]
                let decimal_value = (*value * 1_000_000.0) as i128;
                Arc::new(arrow::array::Decimal128Array::from(vec![decimal_value]))
            }
        }
    }
}

/// Defines parameters for TPC-H queries. Values are extracted from the original TPC-H queries,
/// with their values replaced with $1 parameters in the `/parameterized/` TPC-H files.
#[must_use]
pub fn add_tpch_parameters(queries: Vec<Query>) -> Vec<Query> {
    queries
        .into_iter()
        .map(|q| {
            let mut q = q;
            match q.name.replace("tpch_", "").as_str() {
                "q1" => q.parameters = Some(vec![ParameterValue::String("1998-09-02".into())]),
                "q2" => {
                    q.parameters = Some(vec![
                        ParameterValue::Number(15),
                        ParameterValue::String("%BRASS".into()),
                        ParameterValue::String("EUROPE".into()),
                        ParameterValue::String("EUROPE".into()),
                    ]);
                }
                "q3" => {
                    q.parameters = Some(vec![ParameterValue::String("BUILDING".into())]);
                }
                "q5" => {
                    q.parameters = Some(vec![ParameterValue::String("ASIA".into())]);
                }
                "q6" => {
                    q.parameters = Some(vec![
                        ParameterValue::Float(0.06),
                        ParameterValue::Float(0.01),
                        ParameterValue::Float(0.06),
                        ParameterValue::Float(0.01),
                        ParameterValue::Number(24),
                    ]);
                }
                "q7" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("FRANCE".into()),
                        ParameterValue::String("GERMANY".into()),
                        ParameterValue::String("GERMANY".into()),
                        ParameterValue::String("FRANCE".into()),
                    ]);
                }
                "q8" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("BRAZIL".into()),
                        ParameterValue::String("AMERICA".into()),
                        ParameterValue::String("ECONOMY ANODIZED STEEL".into()),
                    ]);
                }
                "q9" => {
                    q.parameters = Some(vec![ParameterValue::String("%green%".into())]);
                }
                "q10" => {
                    q.parameters = Some(vec![ParameterValue::String("R".into())]);
                }
                "q11" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("GERMANY".into()),
                        ParameterValue::String("GERMANY".into()),
                    ]);
                }
                "q12" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("1-URGENT".into()),
                        ParameterValue::String("2-HIGH".into()),
                        ParameterValue::String("1-URGENT".into()),
                        ParameterValue::String("2-HIGH".into()),
                        ParameterValue::String("MAIL".into()),
                        ParameterValue::String("SHIP".into()),
                    ]);
                }
                "q13" => {
                    q.parameters = Some(vec![ParameterValue::String("%special%requests%".into())]);
                }
                "q14" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("PROMO%".into()),
                        ParameterValue::Number(1),
                        ParameterValue::Number(0),
                        ParameterValue::Number(1),
                    ]);
                }
                "q16" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("Brand#45".into()),
                        ParameterValue::String("MEDIUM POLISHED%".into()),
                        ParameterValue::Number(49),
                        ParameterValue::Number(14),
                        ParameterValue::Number(23),
                        ParameterValue::Number(45),
                        ParameterValue::Number(19),
                        ParameterValue::Number(3),
                        ParameterValue::Number(36),
                        ParameterValue::Number(9),
                        ParameterValue::String("%Customer%Complaints%".into()),
                    ]);
                }
                "q17" => {
                    q.parameters = Some(vec![
                        ParameterValue::Float(7.0),
                        ParameterValue::String("Brand#23".into()),
                        ParameterValue::String("MED BOX".into()),
                        ParameterValue::Float(0.2),
                    ]);
                }
                "q18" => {
                    q.parameters = Some(vec![ParameterValue::Number(300)]);
                }
                "q19" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("Brand#12".into()),
                        ParameterValue::String("SM CASE".into()),
                        ParameterValue::String("SM BOX".into()),
                        ParameterValue::String("SM PACK".into()),
                        ParameterValue::String("SM PKG".into()),
                        ParameterValue::Number(1),
                        ParameterValue::Number(1),
                        ParameterValue::Number(10),
                        ParameterValue::Number(1),
                        ParameterValue::Number(5),
                        ParameterValue::String("AIR".into()),
                        ParameterValue::String("AIR REG".into()),
                        ParameterValue::String("DELIVER IN PERSON".into()),
                        ParameterValue::String("Brand#23".into()),
                        ParameterValue::String("MED BAG".into()),
                        ParameterValue::String("MED BOX".into()),
                        ParameterValue::String("MED PKG".into()),
                        ParameterValue::String("MED PACK".into()),
                        ParameterValue::Number(10),
                        ParameterValue::Number(10),
                        ParameterValue::Number(10),
                        ParameterValue::Number(1),
                        ParameterValue::Number(10),
                        ParameterValue::String("AIR".into()),
                        ParameterValue::String("AIR REG".into()),
                        ParameterValue::String("DELIVER IN PERSON".into()),
                        ParameterValue::String("Brand#34".into()),
                        ParameterValue::String("LG CASE".into()),
                        ParameterValue::String("LG BOX".into()),
                        ParameterValue::String("LG PACK".into()),
                        ParameterValue::String("LG PKG".into()),
                        ParameterValue::Number(20),
                        ParameterValue::Number(20),
                        ParameterValue::Number(10),
                        ParameterValue::Number(1),
                        ParameterValue::Number(15),
                        ParameterValue::String("AIR".into()),
                        ParameterValue::String("AIR REG".into()),
                        ParameterValue::String("DELIVER IN PERSON".into()),
                    ]);
                }
                "q20" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("forest%".into()),
                        ParameterValue::Float(0.5),
                        ParameterValue::String("CANADA".into()),
                    ]);
                }
                "q21" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("F".into()),
                        ParameterValue::String("SAUDI ARABIA".into()),
                    ]);
                }
                "q22" => {
                    q.parameters = Some(vec![
                        ParameterValue::String("13".into()),
                        ParameterValue::String("31".into()),
                        ParameterValue::String("23".into()),
                        ParameterValue::String("29".into()),
                        ParameterValue::String("30".into()),
                        ParameterValue::String("18".into()),
                        ParameterValue::String("17".into()),
                        ParameterValue::Float(0.00),
                        ParameterValue::String("13".into()),
                        ParameterValue::String("31".into()),
                        ParameterValue::String("23".into()),
                        ParameterValue::String("29".into()),
                        ParameterValue::String("30".into()),
                        ParameterValue::String("18".into()),
                        ParameterValue::String("17".into()),
                    ]);
                }
                _ => {}
            }

            q.name = q.name.replace("tpch_", "tpch[parameterized]_").into();
            q
        })
        .collect()
}
