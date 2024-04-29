use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    env,
    fs::File,
    io::{self, BufRead},
    path::Path,
};

// Consider using usizes in all fields
#[derive(Debug)]
struct Vertex {
    id: usize,
    osm_id: usize,
    lon: f64,
    lat: f64,
    height: usize,
    level: usize,
}

#[derive(Debug)]
struct Edge {
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
    // let dijkstra_pairs_file_path: &String = &args[2];

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
            // This is kind of risky and only works if the id are ascending and sorted
            vertices.push(vertex);
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
            let edge_id_a = iter.next().unwrap().parse().unwrap();
            let edge_id_b = iter.next().unwrap().parse().unwrap();

            let edge = Edge {
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

    let (mut upwards_edges, mut downwards_edges): (Vec<Edge>, Vec<Edge>) =
        edges.drain(..).partition(|edge| {
            vertices[edge.start_vertex].level
                < vertices[edge.end_vertex].level
        });

    println!("First downwards edge: {:?}", downwards_edges[0]);

    downwards_edges.iter_mut().for_each(|edge| {
        let tmp = edge.start_vertex;
        edge.start_vertex = edge.end_vertex;
        edge.end_vertex = tmp;
    });

    println!("Flipped downwards edge: {:?}", downwards_edges[0]);

    // Sort by starting node in order to get offset array
    upwards_edges.sort_by_key(|edge| edge.start_vertex);
    downwards_edges.sort_by_key(|edge| edge.start_vertex);

    println!("First vertex: {:?}", vertices[0]);
    println!("First vertex: {:?}", vertices[1104356]);
    println!("First vertex: {:?}", vertices[1104362]);
    println!("First upwards edge: {:?}", upwards_edges[0]);
    // Read dijkstra source-target pairs
    // println!("Reading source-target pairs from file: {}", dijkstra_pairs_file_path);
    // let pair_lines = read_lines(dijkstra_pairs_file_path).unwrap();
    // let mut source_target_tuples: Vec<(usize, usize)> = Vec::new();
    //
    // for line in pair_lines {
    //     let l = line.unwrap();
    //     let mut iter = l.split_whitespace();
    //
    //     let source_target_tuple = (iter.next().unwrap().parse::<usize>().unwrap(), iter.next().unwrap().parse::<usize>().unwrap());
    //     source_target_tuples.push(source_target_tuple)
    // }

    // num_edges != edges.len() since we are converting the edges to an undirected graph
    // by inserting the edges another time in reverse direction
    let mut upwards_offset_array: Vec<usize> = vec![upwards_edges.len(); num_vertices + 1];

    // Initialize variables
    let mut previous_vertex_id = 0;
    upwards_offset_array[0] = 0;

    // If the the start_vertex changes in the edges vector, store the offset in the offset vector
    // and set this offset for all start_vertex id's that have been skipped in this last step.
    // However, I am not sure if this case even occurs in our road network.
    for (edge_index, edge) in upwards_edges.iter().enumerate() {
        if edge.start_vertex != previous_vertex_id {
            for j in previous_vertex_id + 1..=edge.start_vertex {
                upwards_offset_array[j] = edge_index;
            }
            previous_vertex_id = edge.start_vertex;
        }
    }

    // num_edges != edges.len() since we are converting the edges to an undirected graph
    // by inserting the edges another time in reverse direction
    let mut downwards_offset_array: Vec<usize> = vec![downwards_edges.len(); num_vertices + 1];

    // Initialize variables
    let mut previous_vertex_id = 0;
    downwards_offset_array[0] = 0;

    // If the the start_vertex changes in the edges vector, store the offset in the offset vector
    // and set this offset for all start_vertex id's that have been skipped in this last step.
    // However, I am not sure if this case even occurs in our road network.
    for (edge_index, edge) in downwards_edges.iter().enumerate() {
        if edge.start_vertex != previous_vertex_id {
            for j in previous_vertex_id + 1..=edge.start_vertex {
                upwards_offset_array[j] = edge_index;
            }
            previous_vertex_id = edge.start_vertex;
        }
    }

    // Finding connected components of a graph using DFS
    // All vertices are initially initialized with a marker value of 0 which can be interpreted as
    // the component not being marked
    // let now_cc = Instant::now();
    // let mut c: usize = 0;

    // let mut visited: HashSet<usize> = HashSet::new();

    // for v in vertices.values() {
    //     if !visited.contains(&v.id) {
    //         c += 1;
    //         dfs(v.id, &mut visited, &offset_array_cc, &edges_cc);
    //     }
    // }
    // let elapsed_cc = now_cc.elapsed();
    // println!("Number of connected components {} took {} ms to execute", c, elapsed_cc.as_millis());

    // let _path_finding = Dijkstra::new(&vertices, &offset_array, &edges);

    // let mut text: String = "".to_string();

    // for i in source_target_tuples {
    //     let now = Instant::now();
    //     let distance = path_finding.query(i.0, i.1);
    //     let elapsed_time = now.elapsed();
    //     text.push_str(format!("{} {} {} {}\n", i.0, i.1, distance, elapsed_time.as_millis()).as_str());
    //     println!("{} {} {} {}", i.0, i.1, distance, elapsed_time.as_millis());
    // }

    // let path = "output";

    // let mut output = File::create(path).unwrap();
    // write!(output, "{}", text).unwrap();
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
    dist: Vec<usize>,
    pq: BinaryHeap<PQEntry>,
    vertices: &'a Vec<Vertex>,
    offset_array: &'a Vec<usize>,
    edges: &'a Vec<Edge>,
    // We are storing a pointer (index) of the corresponding edge inside of the offset array
    predecessor_array: Vec<usize>,
}

impl<'a> Dijkstra<'a> {
    fn new(
        vertices: &'a Vec<Vertex>,
        offset_array: &'a Vec<usize>,
        edges: &'a Vec<Edge>,
    ) -> Self {
        let dist: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let pq: BinaryHeap<PQEntry> = BinaryHeap::new();
        let predecessor_array = (0..vertices.len()).map(|_| usize::MAX).collect();

        Dijkstra {
            dist,
            pq,
            vertices,
            offset_array,
            edges,
            predecessor_array
        }
    }

    fn query(&mut self, start_node: usize, target_node: usize) -> usize {
        self.dist = (0..self.vertices.len()).map(|_| usize::MAX).collect();
        self.pq.clear();

        self.dist[start_node] = 0;
        self.pq.push(PQEntry {
            distance: 0,
            vertex: start_node,
        });

        while let Some(PQEntry { distance, vertex }) = self.pq.pop() {
            if vertex == target_node {
                return distance;
            };

            for j in self.offset_array[vertex]..self.offset_array[vertex + 1] {
                let edge = self.edges.get(j).unwrap();
                if self.dist[edge.end_vertex] > self.dist[vertex] + edge.weight {
                    self.dist[edge.end_vertex] = self.dist[vertex] + edge.weight;
                    self.pq.push(PQEntry {
                        distance: self.dist[vertex] + edge.weight,
                        vertex: edge.end_vertex,
                    });
                    // Store offset index inside of predecessor array
                    // This should work in O(n)
                    self.predecessor_array[edge.start_vertex] = j;
                }
            }
        }
        usize::MAX
    }
}

fn dfs(
    start_node: usize,
    visited: &mut HashSet<usize>,
    offset_array: &Vec<usize>,
    edges: &Vec<Edge>,
) {
    let mut stack = Vec::new();

    stack.push(start_node);
    visited.insert(start_node);

    while !stack.is_empty() {
        if let Some(current_vertex) = stack.pop() {
            for j in offset_array[current_vertex]..offset_array[current_vertex + 1] {
                let edge = edges.get(j).unwrap();
                if !visited.contains(&edge.end_vertex) {
                    stack.push(edge.end_vertex);
                    visited.insert(edge.end_vertex);
                }
            }
        }
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
