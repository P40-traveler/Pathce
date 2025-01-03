use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;
use oxrdf::{NamedNode, Triple};
use oxrdfio::{RdfFormat, RdfSerializer, WriterQuadSerializer};
use serde_json::Value;

#[derive(Debug, Parser)]
struct Args {
    /// Specify the dataset directory
    #[arg(short, long)]
    dataset: PathBuf,
    /// Specify the gCard schema file
    #[arg(short, long)]
    schema: PathBuf,
    /// Specify the output (.nt) file
    #[arg(short, long)]
    output: PathBuf,
}

fn read_csv(path: PathBuf) -> Vec<(u32, u32)> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_path(path)
        .unwrap();
    let mut edges = Vec::new();
    for row in reader.records() {
        let row = row.unwrap();
        let src = row.get(0).unwrap().parse().unwrap();
        let dst = row.get(1).unwrap().parse().unwrap();
        edges.push((src, dst));
    }
    edges
}

fn write_triples(
    edges: Vec<(u32, u32)>,
    edge_label_id: u64,
    writer: &mut WriterQuadSerializer<impl Write>,
) {
    for (src, dst) in edges {
        let triple = Triple::new(
            NamedNode::new(format!("http://ex.org/{src}")).unwrap(),
            NamedNode::new(format!("http://ex.org/0{edge_label_id}")).unwrap(),
            NamedNode::new(format!("http://ex.org/{dst}")).unwrap(),
        );
        writer.serialize_triple(&triple).unwrap();
    }
}

fn main() {
    let args = Args::parse();
    println!("{:?}", args);
    let output = BufWriter::new(File::create(args.output).unwrap());
    let mut writer = RdfSerializer::from_format(RdfFormat::NTriples).for_writer(output);
    let schema: Value = serde_json::from_reader(File::open(args.schema).unwrap()).unwrap();
    let obj = schema.as_object().unwrap();
    let edge_label_map = obj.get("edge_labels").unwrap().as_object().unwrap();
    for (edge_label, edge_label_id) in edge_label_map {
        let edge_label_id = edge_label_id.as_u64().unwrap();
        let edge_file = args.dataset.join(format!("{edge_label}.csv"));
        println!("processing {}", edge_file.to_str().unwrap());
        let edges = read_csv(edge_file);
        write_triples(edges, edge_label_id, &mut writer);
    }
    writer.finish().unwrap();
}
