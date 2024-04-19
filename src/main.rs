use std::{
    collections::{HashMap, HashSet}, env, fs::File, io::{self, BufRead}, path::Path
};

// Consider using usizes in all fields
#[derive(Debug)]
struct Vertex {
    id: usize,
    osm_id: usize,
    lon: f64,
    lat: f64,
    height: usize,
    marked: usize,
}

#[derive(Debug)]
struct Edge {
    start_vertex: usize,
    end_vertex: usize,
    weight: usize,
    typ: usize,
    max_speed: usize,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let file_path: &String = &args[1];

    println!("In file {}", file_path);

    let mut lines = read_lines(file_path).unwrap();
    for _ in 0..5 {
        lines.next();
    }

    let num_vertices: usize = lines.next().unwrap().unwrap().parse().unwrap();
    let num_edges: usize = lines.next().unwrap().unwrap().parse().unwrap();

    println!("Number of vertices: {}", num_vertices);
    println!("Number of edges: {}", num_edges);
    
    let mut vertices = HashMap::new();
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
                marked: 0,
            };
            // We don't need to store the whole vertex, lon and lat should be sufficient
            vertices.insert(vertex.id, vertex);
        }
    }

    for _ in 0..num_edges {
        if let Some(line) = lines.next() {
            let l = line.unwrap();
            let mut iter = l.split_whitespace();

            let start_vertex = iter.next().unwrap().parse().unwrap();
            let end_vertex = iter.next().unwrap().parse().unwrap();
            let weight = iter.next().unwrap().parse().unwrap();
            let typ = iter.next().unwrap().parse().unwrap();
            let max_speed = iter.next().unwrap().parse().unwrap();
            
            let edge = Edge {
                start_vertex: start_vertex,
                end_vertex: end_vertex,
                weight: weight,
                typ: typ,
                max_speed: max_speed,
            };
            edges.push(edge);

            if start_vertex != end_vertex {
                let back_edge = Edge {
                    start_vertex: end_vertex,
                    end_vertex: start_vertex,
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
    let mut offset_array: Vec<usize> = vec![edges.len(); num_vertices];

    // Initialize variables
    let mut previous_vertex_id = 0;
    // offset_array.insert(0, 0);
    offset_array[0] = 0;

    // If the the start_vertex changes in the edges vector, store the offset in the offset vector
    // and set this offset for all start_vertex id's that have been skipped in this last step.
    // However, I am not sure if this case even occurs in our road network.
    for (edge_index, edge) in edges.iter().enumerate() {
        if edge.start_vertex != previous_vertex_id {
            for j in previous_vertex_id + 1..=edge.start_vertex {
                offset_array[j] = edge_index;
            }
            previous_vertex_id = edge.start_vertex;
        }
    }

    // println!("Offset array {:?}", offset_array);

    // Finding connected components of a graph using DFS
    // All vertices are initially initialized with a marker value of 0 which can be interpreted as
    // the component not being marked
    let mut c: usize = 0;

    let mut visited: HashSet<usize> = HashSet::new();

    for v in vertices.values().into_iter() {
        if !visited.contains(&v.id) {
            // println!("So far, visited does not contain {}, hence executing dfs again. visited: {:?}", v.id, visited);
            c += 1;
            dfs(v, &mut visited, &offset_array, &edges);
        }
    }

    println!("Number of connected components {}", c);
}

fn dfs(
    v: &Vertex,
    visited: &mut HashSet<usize>,
    offset_array: &Vec<usize>,
    edges: &Vec<Edge>,
) {
    let mut stack = Vec::new();

    stack.push(v.id);
    visited.insert(v.id);

    while !stack.is_empty() {
        if let Some(current_vertex) = stack.pop() {
            let neighbour_ids = get_neighbour_ids(current_vertex, offset_array, edges);
            // println!("Neighbour_ids {:?}", neighbour_ids);
            for n in neighbour_ids {
                if !visited.contains(&n) {
                    stack.push(n);
                    visited.insert(n);
                }
            }
        }
    }
}

fn get_neighbour_ids(vertex_id: usize, offset_array: &Vec<usize>, edges: &Vec<Edge>) -> Vec<usize> {
    // println!("Get neighbours for vertex {}", vertex_id);
    let mut offset_index = *offset_array.get(vertex_id).unwrap();
    let mut neighbour_ids: Vec<usize> = Vec::new();

    let mut start_vertex;

    if let Some(edge) = edges.get(offset_index) {
        start_vertex = edge.start_vertex;
    } else {
        return Vec::new();
    }

    while start_vertex == vertex_id {
        let neighbour_id = edges.get(offset_index).unwrap().end_vertex;
        neighbour_ids.push(neighbour_id);
        offset_index += 1;
        if let Some(new_edge) = edges.get(offset_index) {
            start_vertex = new_edge.start_vertex;
        } else {
            break
        }
    }

    neighbour_ids
}

// The output is wrapped in a Result to allow matching on errors.
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
