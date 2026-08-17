use std::collections::HashSet;

pub fn uniq_by<'a, T: Clone, U: Eq + std::hash::Hash>(
    items: &'a [T],
    f: impl Fn(&'a T) -> U,
) -> Vec<T> {
    let mut seen = HashSet::new();

    items
        .iter()
        .filter(|item| seen.insert(f(item)))
        .cloned()
        .collect()
}

pub fn uniq<T: Eq + std::hash::Hash + Clone>(items: &[T]) -> Vec<T> {
    uniq_by(items, |item| item)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uniq_empty() {
        let items: Vec<i32> = vec![];
        let result = uniq(&items);
        assert_eq!(result, Vec::<i32>::new());
    }

    #[test]
    fn test_uniq_single() {
        let items = vec![1];
        let result = uniq(&items);
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_uniq_all_unique() {
        let items = vec![1, 2, 3, 4, 5];
        let result = uniq(&items);
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_uniq_all_duplicates() {
        let items = vec![1, 1, 1, 1];
        let result = uniq(&items);
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_uniq_mixed_duplicates() {
        let items = vec![1, 2, 1, 3, 2, 4, 1, 5];
        let result = uniq(&items);
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_uniq_preserves_order() {
        let items = vec![3, 1, 2, 1, 3, 2];
        let result = uniq(&items);
        assert_eq!(result, vec![3, 1, 2]);
    }

    #[test]
    fn test_uniq_strings() {
        let items = vec!["a", "b", "a", "c", "b"];
        let result = uniq(&items);
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_uniq_by_empty() {
        let items: Vec<i32> = vec![];
        let result = uniq_by(&items, |x| x * 2);
        assert_eq!(result, Vec::<i32>::new());
    }

    #[test]
    fn test_uniq_by_single() {
        let items = vec![1];
        let result = uniq_by(&items, |x| x * 2);
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_uniq_by_all_unique() {
        let items = vec![1, 2, 3, 4, 5];
        let result = uniq_by(&items, |x| x * 2);
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_uniq_by_all_duplicates() {
        let items = vec![1, 2, 3, 4, 5];
        let result = uniq_by(&items, |_| 0); // All map to same key
        assert_eq!(result, vec![1]);
    }

    #[test]
    fn test_uniq_by_mixed_duplicates() {
        let items = vec![1, 2, 3, 4, 5, 6];
        let result = uniq_by(&items, |x| x % 3); // Group by modulo 3
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_uniq_by_preserves_order() {
        let items = vec![5, 2, 8, 1, 6, 3];
        let result = uniq_by(&items, |x| x % 3);
        // Keys: 5%3=2, 2%3=2(dup), 8%3=2(dup), 1%3=1, 6%3=0, 3%3=0(dup)
        // Result: first occurrence of each unique key
        assert_eq!(result, vec![5, 1, 6]);
    }

    #[test]
    fn test_uniq_by_struct_field() {
        #[derive(Clone, Debug, PartialEq)]
        struct Person {
            id: u32,
            name: String,
        }

        let items = vec![
            Person {
                id: 1,
                name: "Alice".to_string(),
            },
            Person {
                id: 2,
                name: "Bob".to_string(),
            },
            Person {
                id: 1,
                name: "Charlie".to_string(),
            },
            Person {
                id: 3,
                name: "David".to_string(),
            },
        ];

        let result = uniq_by(&items, |p| p.id);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].id, 1);
        assert_eq!(result[0].name, "Alice"); // First occurrence preserved
        assert_eq!(result[1].id, 2);
        assert_eq!(result[2].id, 3);
    }

    #[test]
    fn test_uniq_by_string_length() {
        let items = vec!["a", "bb", "c", "dd", "eee"];
        let result = uniq_by(&items, |s| s.len());
        assert_eq!(result, vec!["a", "bb", "eee"]);
    }
}
