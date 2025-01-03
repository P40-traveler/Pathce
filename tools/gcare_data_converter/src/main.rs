use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use clap::Parser;
use csv::ReaderBuilder;
use rayon::iter::{
    IntoParallelIterator, IntoParallelRefIterator, IntoParallelRefMutIterator, ParallelIterator,
};

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    path: PathBuf,
    #[arg(short, long)]
    threads: usize,
}

fn read_csv(path: PathBuf) -> (u32, Vec<(u32, u32, u32)>) {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b' ')
        .flexible(true)
        .from_path(path)
        .unwrap();
    let mut max_vertex_id = 0;
    let mut edges = Vec::new();
    for row in reader.records() {
        let row = row.unwrap();
        let ty = row.get(0).unwrap();
        match ty {
            "v" => {
                let id = row.get(1).unwrap().parse().unwrap();
                max_vertex_id = max_vertex_id.max(id);
            }
            "e" => {
                let src = row.get(1).unwrap().parse().unwrap();
                let dst = row.get(2).unwrap().parse().unwrap();
                let label = row.get(3).unwrap().parse().unwrap();
                edges.push((src, dst, label));
            }
            _ => panic!("invalid type: {}", ty),
        }
    }
    (max_vertex_id, edges)
}

#[derive(Debug, Clone)]
struct Adjacency {
    neighbor: u32,
    label: u32,
}

fn build_adj_list(max_vertex_id: u32, edges: Vec<(u32, u32, u32)>) -> Vec<Vec<Adjacency>> {
    let mut adj_list = vec![Vec::new(); max_vertex_id as usize + 1];
    let adj_list_locked: Vec<_> = adj_list.par_iter_mut().map(Mutex::new).collect();
    edges.into_par_iter().for_each(|(s, t, label)| {
        adj_list_locked
            .get(s as usize)
            .unwrap()
            .lock()
            .unwrap()
            .push(Adjacency { neighbor: t, label });
    });
    adj_list
}

fn relabel(adj_list: Vec<Vec<Adjacency>>) -> Vec<u32> {
    let labels: Vec<_> = adj_list
        .par_iter()
        .map(|adj| {
            let mut label_count_map: HashMap<u32, usize> = HashMap::new();
            for a in adj {
                *label_count_map.entry(a.label).or_default() += 1usize;
            }
            let (max_label, _) = label_count_map.iter().max_by_key(|(_, c)| *c).unwrap();
            let (min_label, _) = label_count_map.iter().min_by_key(|(_, c)| *c).unwrap();
            (*max_label, *min_label)
        })
        .collect();

    vec![]
}

fn main() {
    let args = Args::parse();
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build_global()
        .unwrap();
    let start = Instant::now();
    let (max_vertex_id, edges) = read_csv(args.path);
    println!("read file: {} s", start.elapsed().as_secs_f64());
    let start = Instant::now();
    let adj_list = build_adj_list(max_vertex_id, edges);
    println!("build adj list: {} s", start.elapsed().as_secs_f64());
    let start = Instant::now();
    let labels = relabel(adj_list);
    println!("relabel: {} s", start.elapsed().as_secs_f64());
}
