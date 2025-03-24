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


use mail_parser::{Message};


#[derive(Debug)]
#[derive(PartialEq)]
pub struct ContentInfo {
    pub mime_type: Option<String>,
    pub content: Option<String>,
}


#[derive(Debug)]
#[derive(PartialEq)]
pub struct ContentRecords{
    pub sections: Vec<ContentInfo>,
}



impl ContentRecords {
    pub fn try_from( eml_msg: &Message ) -> Result< ContentRecords, String > {

        let mut content_records = Vec::<ContentInfo>::new();

        let mut html_iter = eml_msg.html_bodies();
        while let Some(rec) = html_iter.next() {
            content_records.push(
                ContentInfo{
                    mime_type: Some("text/html".to_string()),
                    content: Some(rec.to_string()),
                }
            );
        }

        let mut text_iter = eml_msg.text_bodies();
        while let Some(rec) = text_iter.next() {
            content_records.push(
                ContentInfo{
                    mime_type: Some("text/plain".to_string()),
                    content: Some(rec.to_string()),
                }
            );
        }


        if content_records.is_empty() {
            Err("No content sections found.".to_string())
        }else{
            Ok(
                ContentRecords{
                    sections: content_records
                }
            )
        }

    }
}



