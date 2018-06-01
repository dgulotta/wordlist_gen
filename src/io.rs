use curl::easy::Easy;
use std::io::Cursor;

/// Downloads data from a url to memory.
pub fn load_url(url: &str) -> Result<Cursor<Vec<u8>>,::curl::Error>
{
    let mut handle = Easy::new();
    handle.url(url)?;
    let mut v = Vec::new();
    {
        let mut transfer = handle.transfer();
        transfer.write_function(|data| {
            v.extend_from_slice(data); Ok(data.len())
        })?;
        transfer.perform()?;
    }
    Ok(Cursor::new(v))
}
