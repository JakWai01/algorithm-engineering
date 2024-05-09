use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashSet},
    env,
    fs::File,
    io::{self, BufRead},
    path::Path, time::Instant,
};

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

    let (mut edges_up, mut edges_down): (Vec<Edge>, Vec<Edge>) =
        edges.drain(..).partition(|edge| {
            vertices[edge.start_vertex].level
                < vertices[edge.end_vertex].level
        });

    edges_down.iter_mut().for_each(|edge| {
        std::mem::swap(&mut edge.start_vertex, &mut edge.end_vertex);
    });

    // Create offset_array of predecessors as well
    let mut predecessor_upward_edges = edges_up.clone();
    let mut predecessor_downward_edges = edges_down.clone();

    predecessor_upward_edges.sort_by_key(|edge| edge.end_vertex);
    predecessor_downward_edges.sort_by_key(|edge| edge.end_vertex);

    edges_up.sort_by_key(|edge| edge.start_vertex);
    edges_down.sort_by_key(|edge| edge.start_vertex);

    let offset_array_up: Vec<usize> = create_offset_array(&edges_up, num_vertices);
    let offset_array_down: Vec<usize> = create_offset_array(&edges_down, num_vertices); let predecessor_upward_offset_array: Vec<usize> = create_predecessor_offset_array(predecessor_upward_edges, num_vertices);
    let predecessor_downward_offset_array: Vec<usize> = create_predecessor_offset_array(predecessor_downward_edges, num_vertices);

    // Run bi-directional dijkstra
    let mut path_finding = Dijkstra::new(&vertices, &offset_array_up, &offset_array_down, &edges_up, &edges_down);

    let now = Instant::now();

    let start_node = 377371;
    let target_node = 754742;
    let distance = path_finding.ch_query(start_node, target_node);

    let elapsed = now.elapsed();
    
    // desired 436627
    println!("distance: {} - in time: {}", distance, elapsed.as_millis());
// 
// 
//     // construct path from search
//     let mut upwards_path_node_id = current_min_node_id;
//     let mut upwards_path: Vec<usize> = Vec::new();
//     while upwards_path_node_id != start {
//         upwards_path.insert(0, upwards_path_node_id);
//         upwards_path_node_id = upward_distances_predecessors[upwards_path_node_id];
//     }
//     upwards_path.insert(0, start);
// 
//     println!("Upwards path: {:?}", upwards_path);
// 
//     // We should push here
//     let mut downwards_path_node_id = current_min_node_id;
//     let mut downwards_path: Vec<usize> = Vec::new();
//     while downwards_path_node_id != target {
//         downwards_path.push(downwards_path_node_id);
//         downwards_path_node_id = downward_distances_predecessors[downwards_path_node_id];
//     }
//     downwards_path.push(target);
// 
//     println!("Downwards path: {:?}", downwards_path);
//     downwards_path.remove(0);
// 
//     // Concatenate paths
//     upwards_path.append(&mut downwards_path);
// 
//     println!("Final path: {:?}", upwards_path);
    
    // Resolve shortcuts back to original graph

    // stall-on-demand

    // phast

    // arc flags
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

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct PQEntry {
    distance: usize,
    vertex: usize,
}

impl Ord for PQEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .cmp(&self.distance)
            .then_with(|| self.vertex.cmp(&other.vertex))
    }
}

impl PartialOrd for PQEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct Dijkstra<'a> {
    dist_up: Vec<usize>,
    dist_down: Vec<usize>,
    pq_up: BinaryHeap<PQEntry>,
    pq_down: BinaryHeap<PQEntry>,
    vertices: &'a Vec<Vertex>,
    offset_array_up: &'a Vec<usize>,
    offset_array_down: &'a Vec<usize>,
    edges_up: &'a Vec<Edge>,
    edges_down: &'a Vec<Edge>,
}

impl<'a> Dijkstra<'a> {
    fn new(
        vertices: &'a Vec<Vertex>,
        offset_array_up: &'a Vec<usize>,
        offset_array_down: &'a Vec<usize>,
        edges_up: &'a Vec<Edge>,
        edges_down: &'a Vec<Edge>,
    ) -> Self {
        let dist_up: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let dist_down: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let pq_up: BinaryHeap<PQEntry> = BinaryHeap::new();
        let pq_down: BinaryHeap<PQEntry> = BinaryHeap::new();

        Dijkstra {
            dist_up,
            dist_down,
            pq_up,
            pq_down,
            vertices,
            offset_array_up,
            offset_array_down,
            edges_up,
            edges_down,
        }
    }
        
    fn clear_data_structures(&mut self) {
        self.dist_up = (0..self.vertices.len()).map(|_| usize::MAX).collect();
        self.dist_down = (0..self.vertices.len()).map(|_| usize::MAX).collect();
        self.pq_up.clear();
        self.pq_down.clear();
    }

    fn relax_up(&mut self, vertex: usize, edge: &Edge) {
        if self.dist_up[edge.end_vertex] > self.dist_up[vertex] + edge.weight {
            self.dist_up[edge.end_vertex] = self.dist_up[vertex] + edge.weight;
            self.pq_up.push(PQEntry {
                distance: self.dist_up[vertex] + edge.weight,
                vertex: edge.end_vertex,
            });
        }
    }

    fn relax_down(&mut self, vertex: usize, edge: &Edge) {
        if self.dist_down[edge.end_vertex] > self.dist_down[vertex] + edge.weight {
            self.dist_down[edge.end_vertex] = self.dist_down[vertex] + edge.weight;
            self.pq_down.push(PQEntry {
                distance: self.dist_down[vertex] + edge.weight,
                vertex: edge.end_vertex,
            });
        }
    }

    fn ch_query(&mut self, start_node: usize, target_node: usize) -> usize {
            self.clear_data_structures();

            self.dist_up[start_node] = 0;
            self.dist_down[target_node] = 0;

            self.pq_up.push(PQEntry {
                distance: 0,
                vertex: start_node,
            });
            self.pq_down.push(PQEntry {
                distance: 0,
                vertex: target_node,
            });

            let mut visited: Vec<bool> = (0..self.vertices.len()).map(|_| false).collect();

            // TODO: Introduce neighbour edge at top level again to avoid 3 lookups
            while !self.pq_up.is_empty() && !self.pq_down.is_empty() {
                if let Some(PQEntry { distance: distance_up, vertex: vertex_up }) = self.pq_up.pop() {
                    println!("Current up_vertex: {:?}", self.vertices.get(vertex_up).unwrap());
                    if let Some(PQEntry { distance: distance_down, vertex: vertex_down }) = self.pq_down.pop() {
                        println!("Current down_vertex: {:?}", self.vertices.get(vertex_down).unwrap());
                        // Upwards search step
                        for neighbour in self.offset_array_up[vertex_up]..self.offset_array_up[vertex_up + 1] {
                            let edge = self.edges_up.get(neighbour).unwrap();
                            if self.vertices.get(self.edges_up.get(neighbour).unwrap().end_vertex).unwrap().level > self.vertices.get(self.edges_up.get(neighbour).unwrap().start_vertex).unwrap().level {
                                self.relax_up(vertex_up, edge); 
                                
                                if visited[edge.end_vertex] == true {
                                    return edge.end_vertex
                                } else {
                                    visited[edge.end_vertex] = true;
                                }
                            }
                        }
                        
                        // Downwards search step
                        for neighbour in self.offset_array_down[vertex_down]..self.offset_array_down[vertex_down + 1] {
                            let edge = self.edges_down.get(neighbour).unwrap();
                            if self.vertices.get(edge.end_vertex).unwrap().level > self.vertices.get(edge.start_vertex).unwrap().level {
                                self.relax_down(vertex_down, edge); 
                                
                                if visited[edge.end_vertex] == true {
                                    return edge.end_vertex
                                } else {
                                    visited[edge.end_vertex] = true;
                                }
                            }
                        }
                    }
                }
            }
            usize::max_value()
        }
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
