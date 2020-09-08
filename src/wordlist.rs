use rayon::slice::ParallelSliceMut;
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied,Vacant};
use std::io::Write;

use crate::normalize::{Key, normalize};

type ListItem = (String,u64);
type Wordlist = Vec<ListItem>;

pub struct WordlistGenerator
{
    data: HashMap<Key,ListItem>,
    cutoff: u64
}

impl WordlistGenerator
{
    pub fn new(cutoff: u64) -> Self { Self { data: HashMap::new(), cutoff } }

    pub fn add(&mut self, name: String, freq: u64) {
        if freq >= self.cutoff {
            let norm = normalize(&name);
            let key = crate::normalize::key(&norm);
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
        let mut wl: Wordlist = self.data.drain().map(|(_,(k,v))| (k,v)).collect();
        sort_wordlist(&mut wl);
        wl
    }
}

pub fn sort_wordlist(wl: &mut [ListItem])
{
    wl.par_sort_unstable_by_key(|&(_,v)| !v);
}

pub fn write_wordlist<W: Write>(writer: &mut W, wl: &[ListItem]) -> csv::Result<()> {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(writer);
    for w in wl { writer.serialize(w)?; }
    Ok(())
}
