extern crate clap;
extern crate wordlist_gen;
use clap::{App,Arg};

use std::ops::Deref;
use std::path::{Path, PathBuf};

use wordlist_gen::lists::imdb::{Loader, LocalLoader, WebLoader, make_all};

fn main()
{
    let matches = App::new("IMDb Wordlist Generator")
        .about("Generates lists of well-known movies, TV shows, and actors.")
        .arg(Arg::with_name("indir")
            .long("indir")
            .takes_value(true)
            .help("load files from the given local directory instead of the web"))
        .arg(Arg::with_name("outdir")
            .takes_value(true)
            .required(true)
            .help("the directory to which the wordlists will be saved"))
        .get_matches();
    let loader: Box<dyn Loader> = if let Some(dir) = matches.value_of("indir") {
        Box::new(LocalLoader { path: PathBuf::from(dir) })
    } else {
        Box::new(WebLoader {})
    };
    let out_path = Path::new(matches.value_of("outdir").unwrap());
    make_all(loader.deref(), out_path);
}
