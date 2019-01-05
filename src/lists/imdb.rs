use std::collections::HashMap;
use std::io::Read;
use std::fs::File;
use std::path::{Path, PathBuf};
use csv;
use flate2::read::GzDecoder;
use rayon::join;
use wordlist::{WordlistGenerator, write_wordlist};
use io::load_url;

const ACTOR_LIMIT: usize = 37;
const ACTOR_WEIGHTS: [u64; ACTOR_LIMIT] = [100, 88, 77, 68, 60, 53, 47, 41, 36,
    32, 28, 25, 22, 19, 17, 15, 13, 11, 10, 9, 8, 7, 6, 5, 4, 4, 3, 3, 3, 2, 2,
    2, 1, 1, 1, 1, 1];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TitleRatingData
{
    pub tconst: String,
    pub num_votes: u64
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TitlePrincipalData
{
    pub tconst: String,
    pub ordering: usize,
    pub nconst: String,
    pub category: String
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TitleBasicData
{
    pub tconst: String,
    pub title_type: String,
    pub primary_title: String
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NameBasicData
{
    pub nconst: String,
    pub primary_name: String
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TitleAkaData
{
    pub title_id: String,
    pub title: String,
    pub region: String,
    pub types: String,
    pub attributes: String
}

pub trait Loader: Sync
{
    fn load(&self, name: &str) -> Box<Read>;
}

pub struct LocalLoader
{
    pub path: PathBuf
}

impl Loader for LocalLoader
{
    fn load(&self, name: &str) -> Box<Read> {
        let fname = format!("{}.tsv.gz", name);
        let path = self.path.join(fname);
        let f = File::open(path).unwrap();
        Box::new(GzDecoder::new(f))
    }
}

pub struct WebLoader {}

impl Loader for WebLoader
{
    fn load(&self, name: &str) -> Box<Read> {
        let url = format!("https://datasets.imdbws.com/{}.tsv.gz",name);
        let f = load_url(&url).unwrap();
        Box::new(GzDecoder::new(f))
    }
}

fn to_num(s: &str) -> u32
{
    s[2..].parse().unwrap()
}

fn add_item(map: &mut HashMap<u32,u64>, key: u32, amt: u64)
{
    let counter = map.entry(key).or_insert(0);
    *counter += amt;
}

fn make_reader(rdr: Box<Read>) -> csv::Reader<Box<Read>>
{
    csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .quote(0)
        .from_reader(rdr)
}

fn make_rating(d: csv::Result<TitleRatingData>) -> (u32,u64)
{
    let data = d.unwrap();
    (to_num(&data.tconst), data.num_votes)
}

fn process_title_ratings(ldr: &Loader) -> HashMap<u32,u64>
{
    let f = ldr.load("title.ratings");
    let mut rdr = make_reader(f);
    rdr.deserialize()
        .map(make_rating)
        .collect()
}

fn process_title_akas(ldr: &Loader) -> HashMap<u32,Vec<String>>
{
    let f = ldr.load("title.akas");
    let mut rdr = make_reader(f);
    let mut akas = HashMap::new();
    for res in rdr.deserialize::<TitleAkaData>() {
        let rec = res.unwrap();
        if &rec.types == "original" ||
            (&rec.attributes == "short title" && &rec.region == "US") {
                akas.entry(to_num(&rec.title_id)).or_insert_with(Vec::new)
                    .push(rec.title);
            }
    }
    akas
}

fn process_title_basics(ldr: &Loader, mut ratings: HashMap<u32,u64>,
    mut akas: HashMap<u32,Vec<String>>, movie_cutoff: u64, tv_cutoff: u64)
    -> (HashMap<u32,u64>, Vec<(String,u64)>, Vec<(String,u64)>)
{
    let f = ldr.load("title.basics");
    let mut rdr = make_reader(f);
    let mut movies = WordlistGenerator::new(movie_cutoff);
    let mut tv = WordlistGenerator::new(tv_cutoff);
    for res in rdr.deserialize::<TitleBasicData>() {
        let rec = res.unwrap();
        let key = to_num(&rec.tconst);
        let wl =
            match rec.title_type.as_str() {
                "movie" => { &mut movies }
                "tvSeries" => { &mut tv }
                _ => { ratings.remove(&key); continue }
            };
        if let Some(&votes) = ratings.get(&key) {
            wl.add(rec.primary_title,votes);
            if let Some(ent) = akas.get_mut(&key) {
                for title in ent.drain(..) {
                    wl.add(title,votes);
                }
            }
        }
    }
    (ratings, movies.generate(), tv.generate())
}

fn process_title_principals(ldr: &Loader, ratings: &HashMap<u32,u64>) -> HashMap<u32,u64>
{
    let f = ldr.load("title.principals");
    let mut rdr = make_reader(f);
    let mut counts = HashMap::new();
    for res in rdr.deserialize::<TitlePrincipalData>() {
        let rec = res.unwrap();
        if let Some(&votes) = ratings.get(&to_num(&rec.tconst)) {
            if rec.ordering <= ACTOR_LIMIT && rec.category.starts_with("act") {
                add_item(&mut counts, to_num(&rec.nconst), votes * ACTOR_WEIGHTS[rec.ordering-1]);
            }
        }
    }
    counts
}

fn process_name_basics(ldr: &Loader, actor_votes: &HashMap<u32,u64>, cutoff: u64) -> Vec<(String,u64)>
{
    let f = ldr.load("name.basics");
    let mut rdr = make_reader(f);
    let mut actors = WordlistGenerator::new(cutoff);
    for res in rdr.deserialize::<NameBasicData>() {
        let rec = res.unwrap();
        if let Some(&votes) = actor_votes.get(&to_num(&rec.nconst)) {
            actors.add(rec.primary_name,votes);
        }
    }
    actors.generate()
}

fn process_movies(movies: &Vec<(String,u64)>, path: &Path)
{
    let fname = path.join("imdb_movies.txt");
    let mut movie_file = File::create(fname).unwrap();
    write_wordlist(&mut movie_file, &movies).unwrap();
}

fn process_tv(tv: &Vec<(String,u64)>, path: &Path)
{
    let fname = path.join("imdb_tv.txt");
    let mut tv_file = File::create(fname).unwrap();
    write_wordlist(&mut tv_file, &tv).unwrap();
}

fn process_actors(ldr: &Loader, ratings: &HashMap<u32,u64>, path: &Path)
{
    let actor_votes = process_title_principals(ldr, &ratings);
    let actors = process_name_basics(ldr, &actor_votes, 800000);
    let fname = path.join("imdb_actors.txt");
    let mut actor_file = File::create(fname).unwrap();
    write_wordlist(&mut actor_file, &actors).unwrap();
}

pub fn make_all(ldr: &Loader, out_path: &Path)
{
    let (pre_ratings, akas) = join(
        || process_title_ratings(ldr),
        || process_title_akas(ldr));
    let (ratings, movies, tv) = process_title_basics(ldr, pre_ratings, akas, 4000, 1200);
    join(
        || join(|| process_movies(&movies, out_path), || process_tv(&tv, out_path)),
        || process_actors(ldr, &ratings, out_path));
}
