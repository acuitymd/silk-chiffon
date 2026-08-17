use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

fn markdown_files_below(root: &Path) -> Vec<PathBuf> {
    fn visit(path: &Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if !matches!(
                    path.file_name().and_then(|name| name.to_str()),
                    Some(".git" | "target")
                ) {
                    visit(&path, files);
                }
            } else if path.extension().is_some_and(|extension| extension == "md") {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    visit(root, &mut files);
    files.sort();
    files
}

fn links(markdown: &str) -> Vec<&str> {
    let mut links = Vec::new();
    let mut remaining = markdown;
    while let Some(start) = remaining.find("](") {
        remaining = &remaining[start + 2..];
        let Some(end) = remaining.find(')') else {
            break;
        };
        let target = remaining[..end].trim();
        if !target.is_empty() && !target.starts_with('<') {
            links.push(target);
        }
        remaining = &remaining[end + 1..];
    }
    links
}

fn github_anchor(heading: &str) -> String {
    let mut anchor = String::new();
    for character in heading.trim().trim_matches('#').trim().chars() {
        if character.is_alphanumeric() || character == '-' || character == '_' {
            anchor.extend(character.to_lowercase());
        } else if character.is_whitespace() {
            anchor.push('-');
        }
    }
    anchor
}

fn anchors(markdown: &str) -> HashSet<String> {
    let mut counts = HashMap::<String, usize>::new();
    markdown
        .lines()
        .filter_map(|line| {
            let heading = line.strip_prefix('#')?;
            let heading = heading.trim_start_matches('#').strip_prefix(' ')?;
            let base = github_anchor(heading);
            let count = counts.entry(base.clone()).or_default();
            let anchor = if *count == 0 {
                base
            } else {
                format!("{base}-{count}")
            };
            *count += 1;
            Some(anchor)
        })
        .collect()
}

#[test]
fn local_markdown_links_and_anchors_resolve() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let files = markdown_files_below(root);
    assert!(!files.is_empty());

    for source in files {
        let markdown = fs::read_to_string(&source).unwrap();
        for target in links(&markdown) {
            if target.starts_with("http://")
                || target.starts_with("https://")
                || target.starts_with("mailto:")
            {
                continue;
            }
            let (path, fragment) = target
                .split_once('#')
                .map_or((target, None), |(path, fragment)| (path, Some(fragment)));
            let destination = if path.is_empty() {
                source.clone()
            } else {
                source.parent().unwrap().join(path)
            };
            assert!(
                destination.exists(),
                "{} links to missing {target}",
                source.strip_prefix(root).unwrap().display()
            );
            if let Some(fragment) = fragment {
                let destination_markdown = fs::read_to_string(&destination).unwrap();
                assert!(
                    anchors(&destination_markdown).contains(fragment),
                    "{} links to missing anchor {target}",
                    source.strip_prefix(root).unwrap().display()
                );
            }
        }
    }
}

#[test]
fn external_links_contain_no_tracking_or_agent_citation_artifacts() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for source in markdown_files_below(root) {
        let markdown = fs::read_to_string(&source).unwrap();
        for target in links(&markdown) {
            for forbidden in ["utm_", "oaicite", "turn0", "contentReference"] {
                assert!(
                    !target.contains(forbidden),
                    "{} contains {forbidden:?} in {target}",
                    source.strip_prefix(root).unwrap().display()
                );
            }
        }
    }
}
