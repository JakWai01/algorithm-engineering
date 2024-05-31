use std::{
    fs::File,
    io::{self, BufRead},
    path::Path,
    time::Instant,
};

use bitvec::vec::BitVec;

use crate::{Edge, Vertex};

pub fn read_fmi(path: &str) -> (Vec<Vertex>, Vec<Edge>) {
    println!("Reading graph from file: {}", path);

    let mut lines = read_lines(path).unwrap();
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

    (vertices, edges)
}

pub fn cell_to_id(cell: (f64, f64), m_rows: usize, n_columns: usize) -> usize {
    return ((cell.0 * n_columns as f64) + cell.1) as usize;
}

pub fn find_bounds(vertices: &Vec<Vertex>) -> ((f64, f64), (f64, f64)) {
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

pub fn get_grid_cell(
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

pub fn create_predecessor_offset_array(edges: &Vec<Edge>, num_vertices: usize) -> Vec<usize> {
    let mut offset_array: Vec<usize> = vec![edges.len(); num_vertices + 1];

    let mut previous_vertex_id = 0;
    offset_array[0] = 0;

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

pub fn create_offset_array(edges: &Vec<Edge>, num_vertices: usize) -> Vec<usize> {
    let mut offset_array: Vec<usize> = vec![edges.len(); num_vertices + 1];

    let mut previous_vertex_id = 0;
    offset_array[0] = 0;

    for (edge_index, edge) in edges.iter().enumerate() {
        if edge.start_vertex != previous_vertex_id {
            // if edge.start_vertex == 32694 {
            //     println!("Construction offset: {:?}", edge);
            // }
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
pub fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

pub fn construct_spt(
    s: usize,
    vertices: &Vec<Vertex>,
    distances: &Vec<usize>,
    predecessors: &Vec<usize>,
    predecessor_edges: &Vec<usize>,
    current_cell_id: usize,
    arc_flags: &mut Vec<BitVec>,
) {
    let spt = Instant::now();
    for v in vertices {
        let mut current_vertex: usize = v.id;
        if current_vertex != s {
            // If infinite, then there is no path from s to current_vertex
            if distances[current_vertex] == ((usize::MAX / 2) - 1) {
                continue;
            }

            let mut predecessor_edge_id = predecessor_edges[current_vertex];

            if *arc_flags[predecessor_edge_id]
                .get(current_cell_id)
                .unwrap()
                .as_ref()
                == true
            {
                continue;
            }

            loop {
                arc_flags[predecessor_edge_id].set(current_cell_id, true);

                current_vertex = predecessors[current_vertex];
                if current_vertex == s {
                    break;
                }

                predecessor_edge_id = predecessor_edges[current_vertex];
            }
        }
    }
    // println!("Spt took: {:?}", spt.elapsed());
}
