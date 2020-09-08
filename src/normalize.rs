use unidecode::unidecode;

pub type Key = Vec<u8>;

/// Converts a Unicode string into a form suitable for wordlists.
///
/// The resulting string will contain only ASCII uppercase letters,
/// digits, and spaces.
pub fn normalize(s: &str) -> String
{
    let dec = unidecode(s);
    let mut ss = String::with_capacity(dec.len());
    let mut brk = true;
    for c in dec.chars() {
        if c.is_alphanumeric() {
            ss.push(c.to_ascii_uppercase());
            brk = false;
        }
        else if c=='&' {
            if !brk { ss.push(' '); }
            ss.push_str("AND ");
            brk = true;
        }
        else if c.is_whitespace() || "-/".contains(c) {
            if !brk {
                ss.push(' ');
                brk = true;
            }
        }
    }
    if ss.ends_with(' ') {
        ss.pop();
    }
    ss
}

fn key_char(c: char) -> Option<u8>
{
    if c.is_ascii_alphabetic() {
        Some(c.to_ascii_lowercase() as u8)
    }
    else { None }
}

/// Strips a string of all characters except ASCII letters. 
pub fn key(s: &str) -> Key
{
    s.chars().filter_map(key_char).collect()
}
