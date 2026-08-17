/// Returns a syntactically valid explicit scheme without parsing the remaining reference.
pub(super) fn explicit_scheme(reference: &str) -> Option<&str> {
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
}
