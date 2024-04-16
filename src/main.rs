use std::{any::type_name, env, fs::File, io::{self, BufRead}, path::Path};

#[derive(Debug)]
struct Vertex {
    id: u64,
    osm_id: u64,
    lon: f64,
    lat: f64,
    height: i64,
}

#[derive(Debug)]
struct Edge {
    start: u64,
    end: u64,
    weight: u64,
    typ: u64,
    max_speed: u64,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let file_path: &String = &args[1];

    println!("In file {}", file_path);

    let mut lines =  read_lines(file_path).unwrap();
    for _ in 0..5 {
        lines.next();
    }

    let num_vertices: u64 = lines.next().unwrap().unwrap().parse().unwrap();
    let num_edges: u64 = lines.next().unwrap().unwrap().parse().unwrap();

    println!("Number of vertices: {}", num_vertices);
    println!("Number of edges: {}", num_edges);

    let mut vertices: Vec<Vertex> = Vec::new();
    let mut edges: Vec<Edge> = Vec::new();

    for _ in 0..num_vertices {
        if let Some(line) = lines.next() {
            let l = line.unwrap();
            let mut iter = l.split_whitespace();
            let vertex = Vertex {
                id: iter.next().unwrap().parse().unwrap(),
                osm_id: iter.next().unwrap().parse().unwrap(),
                lon: iter.next().unwrap().parse().unwrap(),
                lat: iter.next().unwrap().parse().unwrap(),
                height: iter.next().unwrap().parse().unwrap(),                
            };
            vertices.push(vertex);
        }
    }

    for _ in 0..num_edges {
        if let Some(line) = lines.next() {
            let l = line.unwrap();
            let mut iter = l.split_whitespace();
            let start: u64 = iter.next().unwrap().parse().unwrap();
            let end: u64 = iter.next().unwrap().parse().unwrap();
            let weight: u64 = iter.next().unwrap().parse().unwrap();
            let typ: u64 = iter.next().unwrap().parse().unwrap();
            let max_speed: u64 = iter.next().unwrap().parse().unwrap();

            let edge = Edge {
                start: start,
                end: end,
                weight: weight,
                typ: typ,
                max_speed: max_speed                              
            };
            edges.push(edge);
            
            if start != end {
                let back_edge = Edge {
                    start: end,
                    end: start,
                    weight: weight,
                    typ: typ,
                    max_speed: max_speed                              
                };
                edges.push(back_edge);
            }
        }
    }

    // Sort by starting node in order to get offset array
    edges.sort_by_key(|edge| edge.start);
    
    println!("First five edges of offset array {:?}", &edges[0..5]);

    println!("DONE");
}

// The output is wrapped in a Result to allow matching on errors.
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
