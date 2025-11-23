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

// AWS Partner Revenue Measurement (PRM) user agent format: APN/1.1 (product-code)
// Reference: https://prm.partner.aws.dev/prm-user-agent-samples.html
//
// The product code should be retrieved from the AWS Marketplace Management Portal.
const AWS_USER_AGENT: &str = "APN/1.1 (crl16swivin80rts2oqloids6)"; // https://aws.amazon.com/marketplace/pp/prodview-jmf6jskjvnq7i

#[must_use]
pub fn user_agent() -> &'static str {
    AWS_USER_AGENT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_agent_format() {
        let ua = user_agent();

        // Verify it follows the AWS PRM format: APN/1.1 (product-code)
        assert!(ua.starts_with("APN/1.1 ("));
        assert!(ua.ends_with(')'));

        // Verify the format has the correct structure
        assert_eq!(ua.matches('(').count(), 1);
        assert_eq!(ua.matches(')').count(), 1);

        // Extract and verify product code is not empty
        let start = ua.find('(').expect("user-agent should contain '('") + 1;
        let end = ua.find(')').expect("user-agent should contain ')'");
        let product_code = &ua[start..end];
        assert!(!product_code.is_empty());
    }
}
