/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::dbconnection::odbcconn::ODBCParameter;
use crate::dbconnection::AsyncDbConnection;
use crate::dbconnection::{odbcconn::ODBCConnection, DbConnection};
use async_trait::async_trait;
use odbc_api::{
    buffers::Item, sys::AttrConnectionPooling, Connection, ConnectionOptions, Environment,
    IntoParameter,
};
use secrets::Secret;
use snafu::{ensure, ResultExt, Snafu};
use std::borrow::Borrow;
use std::sync::Mutex;
use std::{any::Any, collections::HashMap, path::PathBuf, sync::Arc};

use super::{DbConnectionPool, Result};
use lazy_static::lazy_static;

lazy_static! {
  static ref ENV: Environment = unsafe {
      // Enable connection pooling. Let driver decide wether the attributes of two connection
      // are similar enough to change the attributes of a pooled one, to fit the requested
      // connection, or if it is cheaper to create a new Connection from scratch.
      // See <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/driver-aware-connection-pooling>
      Environment::set_connection_pooling(AttrConnectionPooling::DriverAware).unwrap();
      Environment::new().unwrap()
  };
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid parameter: {parameter_name}"))]
    InvalidParameterError { parameter_name: String },
}

pub struct ODBCPool {
    pool: &'static Environment,
    params: Arc<Option<HashMap<String, String>>>,
}

impl ODBCPool {
    pub async fn new(
        params: Arc<Option<HashMap<String, String>>>,
        secret: Option<Secret>,
    ) -> Result<Self> {
        Ok(Self { params, pool: &ENV })
    }
}

#[async_trait]
impl<'a> DbConnectionPool<Connection<'a>, &'a ODBCParameter> for ODBCPool
where
    'a: 'static,
{
    async fn connect(&self) -> Result<Box<dyn DbConnection<Connection<'a>, &'a ODBCParameter>>> {
        if let Some(params) = self.params.as_ref() {
            let url = params.get("url").expect("Must provide URL");
            let cxn = self
                .pool
                .connect_with_connection_string(url.as_str(), ConnectionOptions::default())?;
            Ok(Box::new(ODBCConnection::new(cxn)))
        } else {
            InvalidParameterSnafu {
                parameter_name: "url".to_string(),
            }
            .fail()?
        }
    }
}
