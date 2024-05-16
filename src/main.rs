use core::num;
use std::{
    cmp::min,
    env,
    fs::File,
    io::{self, BufRead},
    path::Path,
    time::Instant,
};

extern crate itertools;

use itertools::Itertools;

use crate::dijkstra::Dijkstra;

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
    grid_cell: (f64, f64),
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

    let m = 8;
    let n = 8;

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
                grid_cell: (f64::INFINITY, f64::INFINITY),
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
        .iter()
        .partition(|edge| vertices[edge.start_vertex].level < vertices[edge.end_vertex].level);

    edges_down.iter_mut().for_each(|edge| {
        std::mem::swap(&mut edge.start_vertex, &mut edge.end_vertex);
    });

    edges_up.sort_by_key(|edge| edge.start_vertex);
    edges_down.sort_by_key(|edge| edge.start_vertex);

    let offset_array_up: Vec<usize> = create_offset_array(&edges_up, num_vertices);
    let offset_array_down: Vec<usize> = create_offset_array(&edges_down, num_vertices);

    // Create offset_array of predecessors as well
    let mut predecessor_upward_edges = edges_up.clone();
    let mut predecessor_downward_edges = edges_down.clone();

    predecessor_upward_edges.sort_by_key(|edge| edge.end_vertex);
    predecessor_downward_edges.sort_by_key(|edge| edge.end_vertex);

    println!("Before array up preds");
    let offset_array_up_predecessors: Vec<usize> =
        create_predecessor_offset_array(&predecessor_upward_edges, num_vertices);

    println!("Before array down preds");
    let offset_array_down_predecessors: Vec<usize> =
        create_predecessor_offset_array(&predecessor_downward_edges, num_vertices);

    println!("Before path finding");
    // Run bi-directional dijkstra
    let mut path_finding = dijkstra::Dijkstra::new(
        num_vertices,
        &offset_array_up,
        &offset_array_down,
        &edges_up,
        &edges_down,
        &offset_array_up_predecessors,
        &offset_array_down_predecessors,
    );

    let now = Instant::now();

    // stg 377371 - 754742
    // ger 8371825  - 16743651
    let s = 377371;
    let t = 754742;
    // let s = 8371825;
    // let t = 16743651;
    let (distance, current_min) = path_finding.bidirectional_ch_query(s, t);

    let elapsed = now.elapsed();

    let mut upwards_path_node_id = current_min;
    let mut upwards_path: Vec<usize> = Vec::new();
    while upwards_path_node_id != s {
        upwards_path.insert(0, upwards_path_node_id);
        upwards_path_node_id = path_finding.predecessors_up[upwards_path_node_id];
    }
    upwards_path.insert(0, s);

    let mut downwards_path_node_id = current_min;
    let mut downwards_path: Vec<usize> = Vec::new();
    while downwards_path_node_id != t {
        downwards_path.push(downwards_path_node_id);
        downwards_path_node_id = path_finding.predecessors_down[downwards_path_node_id];
    }
    downwards_path.push(t);

    // Germany 648681
    println!(
        "distance: {}/436627 - in time: {}",
        distance,
        elapsed.as_micros()
    );

    let mut final_edge_path: Vec<usize> = Vec::new();

    for vertex in upwards_path {
        let edge = path_finding.predecessor_edges_up[vertex];
        if edge == usize::MAX {
            continue;
        }
        let e = edges.get(edge).unwrap();
        final_edge_path.push(e.id);
    }

    for vertex in downwards_path {
        let edge = path_finding.predecessor_edges_down[vertex];
        if edge == usize::MAX {
            continue;
        }
        let e = edges.get(edge).unwrap();
        final_edge_path.push(e.id);
    }

    println!("Final edge path length: {:?}", final_edge_path.len());

    // Unpack shortcuts
    let mut unpack_stack: Vec<usize> = Vec::new();
    let mut sanitized_path: Vec<usize> = Vec::new();
    for edge in final_edge_path {
        unpack_stack.push(edge);
        let e = edges.get(edge).unwrap();
        while !unpack_stack.is_empty() {
            let edge_id = unpack_stack.pop().unwrap();
            let edge = edges.get(edge_id).unwrap();
            if edge.edge_id_a != -1 && edge.edge_id_b != -1 {
                unpack_stack.push(edge.edge_id_a as usize);
                unpack_stack.push(edge.edge_id_b as usize);
            } else {
                sanitized_path.push(edge.id);
            }
        }
    }

    println!("Unpacked path length: {:?}", sanitized_path.len());

    // PHAST one-to-all
    // s = 8371825
    let mut phast_path_finding = Dijkstra::new(
        num_vertices,
        &offset_array_up,
        &offset_array_down,
        &edges_up,
        &edges_down,
        &offset_array_up_predecessors,
        &offset_array_down_predecessors,
    );

    edges.sort_by_key(|edge| edge.end_vertex);
    let predecessor_offset_array = create_predecessor_offset_array(&edges, num_vertices);

    vertices.sort_by_key(|v| v.level);
    vertices.reverse();

    let distances = phast_query(
        &mut phast_path_finding,
        s,
        &vertices,
        &predecessor_offset_array,
        &edges,
    );

    // arc flags
    let (min_bound, max_bound) = find_bounds(&vertices);

    let mut grid: Vec<Vec<Vec<Vertex>>> = vec![vec![Vec::new(); m]; n];

    let mut vertices = vertices.clone();

    for vertex in &mut vertices {
        let (x_cell, y_cell) = get_grid_cell(*vertex, min_bound, max_bound, m, n);
        // println!("x_cell: {}, y_cell: {}", x_cell, y_cell);
        grid[x_cell][y_cell].push(*vertex);
        vertex.grid_cell = (x_cell as f64, y_cell as f64);
    }

    // Sort vertices based on grid_cell, so we can iterate over the grid cells one by one
    vertices.sort_by(|x, y| x.grid_cell.partial_cmp(&y.grid_cell).unwrap());

    // Iterate over grid_cells one by one to perform queries into all other cells
    let groups = vertices.iter().group_by(|v| v.grid_cell);

    let mut arc_flags_path_finding = Dijkstra::new(
        num_vertices,
        &offset_array_up,
        &offset_array_down,
        &edges_up,
        &edges_down,
        &offset_array_up_predecessors,
        &offset_array_down_predecessors,
    );

    for (cell, group) in groups.into_iter() {
        let mut boundary_edges: Vec<&Edge> = Vec::new();

        for vertex in group {
            for edge in predecessor_offset_array[vertex.id]..predecessor_offset_array[vertex.id + 1]
            {
                let edge = edges.get(edge).unwrap();
                if vertices.get(edge.start_vertex).unwrap().grid_cell
                    != vertices.get(edge.end_vertex).unwrap().grid_cell
                    && vertices.get(edge.end_vertex).unwrap().grid_cell == cell
                {
                    // println!("Found boundary node!");
                    boundary_edges.push(edge);
                }
            }
        }

        for edge in boundary_edges {
            let distances = phast_query(
                &mut arc_flags_path_finding,
                edge.end_vertex,
                &vertices,
                &predecessor_offset_array,
                &edges,
            );
        }
    }
}

fn phast_query(
    phast_path_finding: &mut Dijkstra,
    s: usize,
    vertices: &Vec<Vertex>,
    predecessor_offset_array: &Vec<usize>,
    edges: &Vec<Edge>,
) -> Vec<usize> {
    let phast_time = Instant::now();

    let mut d = phast_path_finding.ch_query(s, &vertices);

    println!(
        "Finished phast query step 1 in {:?}",
        phast_time.elapsed().as_micros()
    );

    let phast_time = Instant::now();

    for u in vertices {
        for e in predecessor_offset_array[u.id]..predecessor_offset_array[u.id + 1] {
            let edge = edges.get(e).unwrap();
            if d[edge.start_vertex] + edge.weight < d[u.id] {
                d[u.id] = d[edge.start_vertex] + edge.weight;
            }
        }
    }

    println!(
        "Finished phast query step 2 in {:?}",
        phast_time.elapsed().as_micros()
    );

    d.clone()
}

// Function to determine the bounds of the coordinates
fn find_bounds(vertices: &Vec<Vertex>) -> ((f64, f64), (f64, f64)) {
    let min_x = vertices.iter().map(|c| c.lon).fold(f64::INFINITY, f64::min);
    let max_x = vertices
        .iter()
        .map(|c| c.lon)
        .fold(f64::NEG_INFINITY, f64::max);
    let min_y = vertices.iter().map(|c| c.lat).fold(f64::INFINITY, f64::min);
    let max_y = vertices
        .iter()
        .map(|c| c.lat)
        .fold(f64::NEG_INFINITY, f64::max);

    ((min_x, min_y), (max_x, max_y))
}

// Function to determine which grid cell a coordinate belongs to
fn get_grid_cell(
    vertex: Vertex,
    min: (f64, f64),
    max: (f64, f64),
    m: usize,
    n: usize,
) -> (usize, usize) {
    let x_ratio = (vertex.lon - min.0) / (max.0 - min.0);
    let y_ratio = (vertex.lat - min.1) / (max.1 - min.1);

    let x_index = (x_ratio * (m as f64)).floor() as usize;
    let y_index = (y_ratio * (n as f64)).floor() as usize;

    (x_index.min(m - 1), y_index.min(n - 1)) // Ensure indices are within bounds
}

fn create_predecessor_offset_array(edges: &Vec<Edge>, num_vertices: usize) -> Vec<usize> {
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
