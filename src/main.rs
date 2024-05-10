use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    env,
    fs::File,
    io::{self, BufRead},
    path::Path,
    time::Instant,
};

mod dijkstra;
mod pq;

#[derive(Debug, Copy, Clone)]
struct Vertex {
    id: usize,
    osm_id: usize,
    lon: f64,
    lat: f64,
    height: usize,
    level: usize,
}

#[derive(Debug, Copy, Clone)]
struct Edge {
    id: usize,
    start_vertex: usize,
    end_vertex: usize,
    weight: usize,
    typ: usize,
    max_speed: i64,
    edge_id_a: i64,
    edge_id_b: i64,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let file_path: &String = &args[1];

    println!("Reading graph from file: {}", file_path);

    let mut lines = read_lines(file_path).unwrap();
    for _ in 0..10 {
        lines.next();
    }

    let num_vertices: usize = lines.next().unwrap().unwrap().parse().unwrap();
    let num_edges: usize = lines.next().unwrap().unwrap().parse().unwrap();

    println!("Number of vertices: {}", num_vertices);
    println!("Number of edges: {}", num_edges);

    let mut vertices = Vec::new();
    let mut edges: Vec<Edge> = Vec::new();

    for _ in 0..num_vertices {
        if let Some(line) = lines.next() {
            let l = line.unwrap();
            let mut iter = l.split_whitespace();

            let id = iter.next().unwrap().parse().unwrap();
            let osm_id = iter.next().unwrap().parse().unwrap();
            let lon = iter.next().unwrap().parse().unwrap();
            let lat = iter.next().unwrap().parse().unwrap();
            let height = iter.next().unwrap().parse().unwrap();
            let level = iter.next().unwrap().parse().unwrap();

            let vertex = Vertex {
                id,
                osm_id,
                lon,
                lat,
                height,
                level,
            };
            vertices.push(vertex);
        }
    }

    for id in 0..num_edges {
        if let Some(line) = lines.next() {
            let l = line.unwrap();
            let mut iter = l.split_whitespace();

            let start_vertex = iter.next().unwrap().parse().unwrap();
            let end_vertex = iter.next().unwrap().parse().unwrap();
            let weight = iter.next().unwrap().parse().unwrap();
            let typ = iter.next().unwrap().parse().unwrap();
            let max_speed = iter.next().unwrap().parse().unwrap();
            let edge_id_a = iter.next().unwrap().parse().unwrap();
            let edge_id_b = iter.next().unwrap().parse().unwrap();

            let edge = Edge {
                id,
                start_vertex,
                end_vertex,
                weight,
                typ,
                max_speed,
                edge_id_a,
                edge_id_b,
            };
            edges.push(edge);
        }
    }

    let (mut edges_up, mut edges_down): (Vec<Edge>, Vec<Edge>) = edges
        .drain(..)
        .partition(|edge| vertices[edge.start_vertex].level < vertices[edge.end_vertex].level);

    edges_down.iter_mut().for_each(|edge| {
        std::mem::swap(&mut edge.start_vertex, &mut edge.end_vertex);
    });

    // Create offset_array of predecessors as well
    // let mut predecessor_upward_edges = edges_up.clone();
    // let mut predecessor_downward_edges = edges_down.clone();

    // predecessor_upward_edges.sort_by_key(|edge| edge.end_vertex);
    // predecessor_downward_edges.sort_by_key(|edge| edge.end_vertex);

    edges_up.sort_by_key(|edge| edge.start_vertex);
    edges_down.sort_by_key(|edge| edge.start_vertex);

    let offset_array_up: Vec<usize> = create_offset_array(&edges_up, num_vertices);
    let offset_array_down: Vec<usize> = create_offset_array(&edges_down, num_vertices);
    // let predecessor_upward_offset_array: Vec<usize> = create_predecessor_offset_array(predecessor_upward_edges, num_vertices);
    // let predecessor_downward_offset_array: Vec<usize> = create_predecessor_offset_array(predecessor_downward_edges, num_vertices);

    // Run bi-directional dijkstra
    let mut path_finding = dijkstra::Dijkstra::new(
        &vertices,
        &offset_array_up,
        &offset_array_down,
        &edges_up,
        &edges_down,
    );

    let now = Instant::now();

    // 377371 - 754742
    let s = 377371;
    let t = 754742;
    let distance = path_finding.ch_query(s, t);

    let elapsed = now.elapsed();

    // let mut upwards_path_node_id = current_min;
    // let mut upwards_path: Vec<usize> = Vec::new();
    // while upwards_path_node_id != s {
    //     upwards_path.insert(0, upwards_path_node_id);
    //     upwards_path_node_id = predecessors_up[upwards_path_node_id];
    // }
    // upwards_path.insert(0, s);
    // println!("Upwards path: {:?}", upwards_path);

    // let mut downwards_path_node_id = current_min;
    // let mut downwards_path: Vec<usize> = Vec::new();
    // while downwards_path_node_id != t {
    //     downwards_path.push(downwards_path_node_id);
    //     downwards_path_node_id = predecessors_down[downwards_path_node_id];
    // }
    // downwards_path.push(t);
    // println!("Downwards path: {:?}", downwards_path);
    // downwards_path.remove(0);

    // // Concatenate paths
    // upwards_path.append(&mut downwards_path);

    // println!("Final path: {:?}", upwards_path);

    println!(
        "distance: {}/436627 - in time: {}",
        distance,
        elapsed.as_millis()
    );
}

fn create_predecessor_offset_array(edges: Vec<Edge>, num_vertices: usize) -> Vec<usize> {
    let mut offset_array: Vec<usize> = vec![edges.len(); num_vertices + 1];

    // Initialize variables
    let mut previous_vertex_id = 0;
    offset_array[0] = 0;

    // If the the start_vertex changes in the edges vector, store the offset in the offset vector
    // and set this offset for all start_vertex id's that have been skipped in this last step.
    // However, I am not sure if this case even occurs in our road network.
    for (edge_index, edge) in edges.iter().enumerate() {
        if edge.end_vertex != previous_vertex_id {
            for j in previous_vertex_id + 1..=edge.end_vertex {
                offset_array[j] = edge_index;
            }
            previous_vertex_id = edge.end_vertex;
        }
    }
    offset_array
}

fn create_offset_array(edges: &Vec<Edge>, num_vertices: usize) -> Vec<usize> {
    let mut offset_array: Vec<usize> = vec![edges.len(); num_vertices + 1];

    // Initialize variables
    let mut previous_vertex_id = 0;
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
    offset_array
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
