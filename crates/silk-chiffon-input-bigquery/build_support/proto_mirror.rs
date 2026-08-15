use std::{
    error::Error,
    ffi::OsStr,
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
};

fn transaction_paths(generated_dir: &Path) -> Result<(PathBuf, PathBuf, PathBuf), Box<dyn Error>> {
    let parent = generated_dir
        .parent()
        .ok_or("generated proto mirror has no parent directory")?;
    let name = generated_dir
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or("generated proto mirror has a non-UTF-8 name")?;
    Ok((
        parent.join(format!(".{name}.bq-storage-candidate")),
        parent.join(format!(".{name}.bq-storage-backup")),
        parent.join(format!(".{name}.bq-storage-committed")),
    ))
}

fn remove_owned_tree(path: &Path) -> Result<(), Box<dyn Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

fn recover_generated_mirror_with(
    generated_dir: &Path,
    rename: &mut dyn FnMut(&Path, &Path) -> io::Result<()>,
) -> Result<(), Box<dyn Error>> {
    let (candidate, backup, committed) = transaction_paths(generated_dir)?;
    if committed.exists() {
        if !generated_dir.exists() {
            return Err("committed generated proto mirror is missing".into());
        }
        remove_owned_tree(&backup)?;
        fs::remove_file(&committed)?;
    } else if backup.exists() {
        remove_owned_tree(generated_dir)?;
        rename(&backup, generated_dir)?;
    }
    remove_owned_tree(&candidate)?;
    Ok(())
}

fn validate_live_inventory(generated_dir: &Path) -> Result<(), Box<dyn Error>> {
    if !generated_dir.exists() {
        return Ok(());
    }
    if generated_dir.is_symlink() {
        return Err(format!(
            "generated proto mirror may not be a symbolic link: {}",
            generated_dir.display()
        )
        .into());
    }
    for entry in fs::read_dir(generated_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            return Err(format!(
                "unexpected directory in generated proto mirror: {}",
                entry.path().display()
            )
            .into());
        }
    }
    Ok(())
}

fn prepare_candidate(
    out_dir: &Path,
    candidate: &Path,
    generated_files: &[&str],
) -> Result<(), Box<dyn Error>> {
    fs::create_dir(candidate)?;
    for relative in generated_files {
        let source = out_dir.join(relative);
        if !source.is_file() {
            return Err(format!("generated proto source is missing: {}", source.display()).into());
        }
        fs::copy(source, candidate.join(relative))?;
    }
    check_generated_mirror(out_dir, candidate, generated_files)
}

fn write_commit_marker(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut marker = File::create(path)?;
    marker.write_all(b"committed\n")?;
    marker.sync_all()?;
    File::open(path.parent().ok_or("commit marker has no parent")?)?.sync_all()?;
    Ok(())
}

pub fn update_generated_mirror(
    out_dir: &Path,
    generated_dir: &Path,
    generated_files: &[&str],
) -> Result<(), Box<dyn Error>> {
    update_generated_mirror_with(
        out_dir,
        generated_dir,
        generated_files,
        &mut |source, target| fs::rename(source, target),
    )
}

pub fn update_generated_mirror_with(
    out_dir: &Path,
    generated_dir: &Path,
    generated_files: &[&str],
    rename: &mut dyn FnMut(&Path, &Path) -> io::Result<()>,
) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(
        generated_dir
            .parent()
            .ok_or("generated proto mirror has no parent directory")?,
    )?;
    recover_generated_mirror_with(generated_dir, rename)?;
    validate_live_inventory(generated_dir)?;
    let (candidate, backup, committed) = transaction_paths(generated_dir)?;

    if backup.exists() || committed.exists() {
        return Err(format!(
            "generated proto transaction state already exists: {}",
            generated_dir.display()
        )
        .into());
    }
    if let Err(error) = prepare_candidate(out_dir, &candidate, generated_files) {
        let _ = remove_owned_tree(&candidate);
        return Err(error);
    }

    let had_live_tree = generated_dir.exists();
    let result = (|| -> Result<(), Box<dyn Error>> {
        if had_live_tree {
            rename(generated_dir, &backup)?;
        }
        if let Err(error) = rename(&candidate, generated_dir) {
            if had_live_tree {
                rename(&backup, generated_dir)?;
            }
            return Err(error.into());
        }
        if had_live_tree {
            write_commit_marker(&committed)?;
            fs::remove_dir_all(&backup)?;
            fs::remove_file(&committed)?;
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = remove_owned_tree(&candidate);
    }
    result
}

pub fn check_generated_mirror(
    out_dir: &Path,
    generated_dir: &Path,
    generated_files: &[&str],
) -> Result<(), Box<dyn Error>> {
    let mut expected = generated_files
        .iter()
        .map(|relative| generated_dir.join(relative))
        .collect::<Vec<PathBuf>>();
    expected.sort();
    let mut actual = fs::read_dir(generated_dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    actual.sort();

    if actual != expected {
        return Err(
            "generated proto file inventory differs; run `just bigquery-proto-update`".into(),
        );
    }
    for relative in generated_files {
        let generated = fs::read(out_dir.join(relative))?;
        let committed = fs::read(generated_dir.join(relative))?;
        if generated != committed {
            return Err(format!(
                "generated proto {relative} is stale; run `just bigquery-proto-update`"
            )
            .into());
        }
    }
    Ok(())
}
