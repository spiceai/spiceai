use crate::dataconnector::abfs::AzureBlobFSFactory;
use crate::register_data_connector;

register_data_connector!("abfss", AzureBlobFSFactory);
