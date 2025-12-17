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
