/// Returns a syntactically valid explicit scheme without parsing the remaining reference.
pub(super) fn explicit_scheme(reference: &str) -> Option<&str> {
    if cfg!(windows) {
        let bytes = reference.as_bytes();
        let has_drive_root = bytes.first().is_some_and(u8::is_ascii_alphabetic)
            && bytes.get(1) == Some(&b':')
            && (bytes.get(2) == Some(&b'\\')
                || (bytes.get(2) == Some(&b'/') && bytes.get(3) != Some(&b'/')));
        if has_drive_root {
            return None;
        }
    }

    let (scheme, _) = reference.split_once(':')?;
    let mut characters = scheme.chars();
    matches!(characters.next(), Some('a'..='z'))
        .then_some(())
        .filter(|()| {
            characters.all(|character| {
                character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || matches!(character, '+' | '-' | '.')
            })
        })
        .map(|()| scheme)
}

#[cfg(test)]
mod tests {
    use super::explicit_scheme;

    #[test]
    fn classifies_only_the_prefix_and_preserves_bare_references() {
        assert_eq!(
            explicit_scheme("bqs://project/table?query=a?b"),
            Some("bqs")
        );
        assert_eq!(explicit_scheme("input.parquet"), None);
        assert_eq!(explicit_scheme("BQS://project/table"), None);
    }

    #[cfg(windows)]
    #[test]
    fn preserves_lowercase_windows_drive_roots_as_bare_references() {
        assert_eq!(explicit_scheme(r"c:\input.parquet"), None);
        assert_eq!(explicit_scheme("c:/input.parquet"), None);
    }
}
