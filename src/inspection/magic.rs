use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use anyhow::Result;

pub fn magic_bytes_match_start(file: &mut File, expected: &[u8]) -> Result<bool> {
    if file.metadata()?.len() < expected.len() as u64 {
        return Ok(false);
    }

    let mut buf = vec![0u8; expected.len()];

    // save original position so we can return to it later
    let start_pos = file.stream_position()?;

    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;

    // return to original position
    file.seek(SeekFrom::Start(start_pos))?;

    Ok(buf == expected)
}

pub fn magic_bytes_match_end(file: &mut File, expected: &[u8]) -> Result<bool> {
    if file.metadata()?.len() < expected.len() as u64 {
        return Ok(false);
    }

    let mut buf = vec![0u8; expected.len()];

    // save original position so we can return to it later
    let start_pos = file.stream_position()?;

    // converting from usize to i64 can overflow, so we allow
    // for errors rather than panicking or overflowing
    let expected_len: i64 = expected.len().try_into()?;

    file.seek(SeekFrom::End(-expected_len))?;
    file.read_exact(&mut buf)?;

    // return to original position
    file.seek(SeekFrom::Start(start_pos))?;

    Ok(buf == expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const PARQUET_MAGIC: &[u8] = b"PAR1";

    #[test]
    fn test_magic_bytes_match_start_with_real_parquet() {
        let mut file = File::open("tests/files/people.parquet").unwrap();
        assert!(magic_bytes_match_start(&mut file, PARQUET_MAGIC).unwrap());
    }

    #[test]
    fn test_magic_bytes_match_end_with_real_parquet() {
        let mut file = File::open("tests/files/people.parquet").unwrap();
        assert!(magic_bytes_match_end(&mut file, PARQUET_MAGIC).unwrap());
    }

    #[test]
    fn test_magic_bytes_match_start_no_match() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"NOTPARQUET").unwrap();
        temp.flush().unwrap();

        let mut file = temp.reopen().unwrap();
        assert!(!magic_bytes_match_start(&mut file, PARQUET_MAGIC).unwrap());
    }

    #[test]
    fn test_magic_bytes_match_end_no_match() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"somedata").unwrap();
        temp.flush().unwrap();

        let mut file = temp.reopen().unwrap();
        assert!(!magic_bytes_match_end(&mut file, PARQUET_MAGIC).unwrap());
    }

    #[test]
    fn test_magic_bytes_file_too_small() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"PAR").unwrap(); // 3 bytes, PARQUET_MAGIC is 4
        temp.flush().unwrap();

        let mut file = temp.reopen().unwrap();
        assert!(!magic_bytes_match_start(&mut file, PARQUET_MAGIC).unwrap());
        assert!(!magic_bytes_match_end(&mut file, PARQUET_MAGIC).unwrap());
    }

    #[test]
    fn test_magic_bytes_empty_file() {
        let temp = NamedTempFile::new().unwrap();
        let mut file = temp.reopen().unwrap();
        assert!(!magic_bytes_match_start(&mut file, PARQUET_MAGIC).unwrap());
        assert!(!magic_bytes_match_end(&mut file, PARQUET_MAGIC).unwrap());
    }

    #[test]
    fn test_magic_bytes_preserves_position() {
        let mut file = File::open("tests/files/people.parquet").unwrap();

        // seek to middle of file
        file.seek(SeekFrom::Start(50)).unwrap();
        let pos_before = file.stream_position().unwrap();

        magic_bytes_match_start(&mut file, PARQUET_MAGIC).unwrap();

        let pos_after = file.stream_position().unwrap();
        assert_eq!(pos_before, pos_after, "position should be preserved");
    }

    #[test]
    fn test_magic_bytes_end_preserves_position() {
        let mut file = File::open("tests/files/people.parquet").unwrap();

        file.seek(SeekFrom::Start(50)).unwrap();
        let pos_before = file.stream_position().unwrap();

        magic_bytes_match_end(&mut file, PARQUET_MAGIC).unwrap();

        let pos_after = file.stream_position().unwrap();
        assert_eq!(pos_before, pos_after, "position should be preserved");
    }

    #[test]
    fn test_magic_bytes_exact_size_file() {
        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(b"PAR1").unwrap(); // exactly 4 bytes
        temp.flush().unwrap();

        let mut file = temp.reopen().unwrap();
        assert!(magic_bytes_match_start(&mut file, PARQUET_MAGIC).unwrap());
        assert!(magic_bytes_match_end(&mut file, PARQUET_MAGIC).unwrap());
    }
}
