use sanitize_filename::{Options, sanitize_with_options};
use sha2::{Digest, Sha256};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct OutputTemplate {
    pattern: String,
}

impl OutputTemplate {
    pub fn new(pattern: String) -> Self {
        Self { pattern }
    }

    pub fn resolve(&self, column_name: &str, value: &str) -> PathBuf {
        let mut result = self.pattern.clone();

        result = result.replace("{value}", value);
        result = result.replace("{column}", column_name);
        result = result.replace(
            "{safe_value}",
            &sanitize_with_options(
                value,
                Options {
                    truncate: true,
                    replacement: "_",
                    ..Options::default()
                },
            ),
        );
        result = result.replace("{hash}", &hash_value(value));

        PathBuf::from(result)
    }
}

fn hash_value(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let result = hasher.finalize();
    format!("{result:x}").chars().take(8).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_template() {
        let template = OutputTemplate::new("data_{value}.arrow".to_string());
        let path = template.resolve("user_id", "123");
        assert_eq!(path.to_str().unwrap(), "data_123.arrow");
    }

    #[test]
    fn test_column_placeholder() {
        let template = OutputTemplate::new("{column}/{value}.parquet".to_string());
        let path = template.resolve("user_id", "456");
        assert_eq!(path.to_str().unwrap(), "user_id/456.parquet");
    }

    #[test]
    fn test_safe_value_placeholder() {
        let template = OutputTemplate::new("file_{safe_value}.arrow".to_string());
        let path = template.resolve("email", "user@example.com");
        assert_eq!(path.to_str().unwrap(), "file_user@example.com.arrow");

        let path2 = template.resolve("path", "folder/file:test");
        assert_eq!(path2.to_str().unwrap(), "file_folder_file_test.arrow");
    }

    #[test]
    fn test_hash_placeholder() {
        let template = OutputTemplate::new("data_{hash}.parquet".to_string());
        let path = template.resolve("desc", "test value");
        assert!(path.to_str().unwrap().starts_with("data_"));
        assert!(path.to_str().unwrap().ends_with(".parquet"));
        assert_eq!(path.to_str().unwrap().len(), "data_12345678.parquet".len());
    }

    #[test]
    fn test_multiple_placeholders() {
        let template = OutputTemplate::new("{column}/{safe_value}_{hash}.arrow".to_string());
        let path = template.resolve("category", "Books & Media");
        assert!(
            path.to_str()
                .unwrap()
                .starts_with("category/Books & Media_")
        );
        assert!(
            path.extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("arrow"))
        );
    }

    #[test]
    fn test_hash_consistency() {
        let value = "test value";
        let hash1 = hash_value(value);
        let hash2 = hash_value(value);
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 8);
    }
}
