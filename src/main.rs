use std::{env, fs::File, io::{self, BufRead}, path::Path};


// Consider using usizes in all fields
#[derive(Debug)]
struct Vertex {
    id: u64,
    osm_id: u64,
    lon: f64,
    lat: f64,
    height: i64,
    marker: usize,
}

#[derive(Debug)]
struct Edge {
    start_vertex: u64,
    end_node: u64,
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
                marker: 0,
            };
            vertices.push(vertex);
        }
    }

    for _ in 0..num_edges {
        if let Some(line) = lines.next() {
            let l = line.unwrap();
            let mut iter = l.split_whitespace();
            let start_vertex: u64 = iter.next().unwrap().parse().unwrap();
            let end_node: u64 = iter.next().unwrap().parse().unwrap();
            let weight: u64 = iter.next().unwrap().parse().unwrap();
            let typ: u64 = iter.next().unwrap().parse().unwrap();
            let max_speed: u64 = iter.next().unwrap().parse().unwrap();

            let edge = Edge {
                start_vertex: start_vertex,
                end_node: end_node,
                weight: weight,
                typ: typ,
                max_speed: max_speed                              
            };
            edges.push(edge);
            
            if start_vertex != end_node {
                let back_edge = Edge {
                    start_vertex: end_node,
                    end_node: start_vertex,
                    weight: weight,
                    typ: typ,
                    max_speed: max_speed                              
                };
                edges.push(back_edge);
            }
        }
    }

    // Sort by starting node in order to get offset array
    edges.sort_by_key(|edge| edge.start_vertex);
    
    // num_edges != edges.len() since we are converting the edges to an undirected graph
    // by inserting the edges another time in reverse direction
    let mut offset_array: Vec<u64> = vec![edges.len() as u64, num_vertices];
    
    // Initialize variables
    let mut previous_vertex_id = 0;
    offset_array.insert(0, 0);
    
    // If the the start_vertex changes in the edges vector, store the offset in the offset vector
    // and set this offset for all start_vertex id's that have been skipped in this last step.
    // However, I am not sure if this case even occurs in our road network.
    for (i, edge) in edges.iter().enumerate() {
        if edge.start_vertex != previous_vertex_id {
            for j in previous_vertex_id+1..=edge.start_vertex {
                offset_array.insert(j as usize, i as u64);
            }
            previous_vertex_id = edge.start_vertex;
        }
    }

    // Finding connected components of a graph using DFS
    // All vertices are initially initialized with a marker value of 0 which can be interpreted as
    // the component not being marked
    let mut components: usize = 0;
    for vertex in vertices {
        if vertex.marker == 0 {
            components += 1;
            depth_first_search(vertex)
        }
    }
    
    println!("First five offsets of offset array {:?}", &offset_array[0..300]);

    println!("Last offsets of offset array {:?}", &offset_array.last());

    println!("First five edges of edge array {:?}", &edges[0..100]);

    println!("DONE");
}

fn depth_first_search(vertex: Vertex) {
    unimplemented!()
}

// The output is wrapped in a Result to allow matching on errors.
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
