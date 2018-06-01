type Wordlist = Vec<(String,u64)>;

use csv;
use rayon::slice::ParallelSliceMut;
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied,Vacant};
use std::io::Write;

use ::normalize::Key;

pub struct WordlistGenerator
{
    data: HashMap<Key,(String,u64)>,
    cutoff: u64
}

impl WordlistGenerator
{
    pub fn new(cutoff: u64) -> Self { Self { data: HashMap::new(), cutoff: cutoff } }

    pub fn add(&mut self, name: String, freq: u64) {
        if freq >= self.cutoff {
            let norm = ::normalize::normalize(&name);
            let key = ::normalize::key(&norm);
            if key.is_empty() { return; }
            match self.data.entry(key) {
                Occupied(mut ent) => {
                    let v = ent.get_mut();
                    if freq > v.1 { *v = (norm,freq) };
                }
                Vacant(ent) => {
                    ent.insert((norm,freq));
                }
            }
        }
    }

    pub fn generate(mut self) -> Wordlist {
        let mut wl = self.data.drain().map(|(_,(k,v))| (k,v)).collect();
        sort_wordlist(&mut wl);
        wl
    }
}

pub fn sort_wordlist(wl: &mut Wordlist)
{
    wl.par_sort_unstable_by_key(|&(_,v)| !v);
}

pub fn write_wordlist<W: Write>(writer: &mut W, wl: &Wordlist) -> csv::Result<()> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(writer);
    for w in wl { writer.serialize(w)?; }
    Ok(())
}