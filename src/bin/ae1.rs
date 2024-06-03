use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    env,
    fs::File,
    io::{self, BufRead},
    io::{Error, Write},
    path::Path,
    time::{self, Instant},
};

// Consider using usizes in all fields
#[derive(Debug)]
struct Vertex {
    id: usize,
    osm_id: usize,
    lon: f64,
    lat: f64,
    height: usize,
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
    let dijkstra_pairs_file_path: &String = &args[2];

    println!("Reading graph from file: {}", file_path);

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
    let mut edges_cc: Vec<Edge> = Vec::new();

    for _ in 0..num_vertices {
        if let Some(line) = lines.next() {
            let l = line.unwrap();
            let mut iter = l.split_whitespace();

            let id = iter.next().unwrap().parse().unwrap();
            let osm_id = iter.next().unwrap().parse().unwrap();
            let lon = iter.next().unwrap().parse().unwrap();
            let lat = iter.next().unwrap().parse().unwrap();
            let height = iter.next().unwrap().parse().unwrap();

            let vertex = Vertex {
                id,
                osm_id,
                lon,
                lat,
                height,
            };
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
                start_vertex,
                end_vertex,
                weight,
                typ,
                max_speed,
            };
            edges.push(edge);

            let edge_cc = Edge {
                start_vertex,
                end_vertex,
                weight,
                typ,
                max_speed,
            };
            edges_cc.push(edge_cc);

            let back_edge_cc = Edge {
                start_vertex: end_vertex,
                end_vertex: start_vertex,
                weight,
                typ,
                max_speed,
            };
            edges_cc.push(back_edge_cc);
        }
    }

    // Sort by starting node in order to get offset array
    edges.sort_by_key(|edge| edge.start_vertex);
    edges_cc.sort_by_key(|edge| edge.start_vertex);

    // Read dijkstra source-target pairs
    println!(
        "Reading source-target pairs from file: {}",
        dijkstra_pairs_file_path
    );
    let pair_lines = read_lines(dijkstra_pairs_file_path).unwrap();
    let mut source_target_tuples: Vec<(usize, usize)> = Vec::new();

    for line in pair_lines {
        let l = line.unwrap();
        let mut iter = l.split_whitespace();

        let source_target_tuple = (
            iter.next().unwrap().parse::<usize>().unwrap(),
            iter.next().unwrap().parse::<usize>().unwrap(),
        );
        source_target_tuples.push(source_target_tuple)
    }

    // num_edges != edges.len() since we are converting the edges to an undirected graph
    // by inserting the edges another time in reverse direction
    let mut offset_array: Vec<usize> = vec![edges.len(); num_vertices + 1];
    let mut offset_array_cc: Vec<usize> = vec![edges_cc.len(); num_vertices + 1];

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

    let mut previous_vertex_id_cc = 0;
    offset_array_cc[0] = 0;

    // If the the start_vertex changes in the edges vector, store the offset in the offset vector
    // and set this offset for all start_vertex id's that have been skipped in this last step.
    // However, I am not sure if this case even occurs in our road network.
    for (edge_index, edge) in edges_cc.iter().enumerate() {
        if edge.start_vertex != previous_vertex_id_cc {
            for j in previous_vertex_id_cc + 1..=edge.start_vertex {
                offset_array_cc[j] = edge_index;
            }
            previous_vertex_id_cc = edge.start_vertex;
        }
    }

    // Finding connected components of a graph using DFS
    // All vertices are initially initialized with a marker value of 0 which can be interpreted as
    // the component not being marked
    let now_cc = Instant::now();
    let mut c: usize = 0;

    let mut visited: HashSet<usize> = HashSet::new();

    for v in vertices.values() {
        if !visited.contains(&v.id) {
            c += 1;
            dfs(v.id, &mut visited, &offset_array_cc, &edges_cc);
        }
    }
    let elapsed_cc = now_cc.elapsed();
    println!(
        "Number of connected components {} took {} ms to execute",
        c,
        elapsed_cc.as_millis()
    );

    let mut path_finding = Dijkstra::new(&vertices, &offset_array, &edges);

    let mut text: String = "".to_string();

    for i in source_target_tuples {
        let now = Instant::now();
        let distance = path_finding.query(i.0, i.1);
        let elapsed_time = now.elapsed();
        text.push_str(
            format!(
                "{} {} {} {}\n",
                i.0,
                i.1,
                distance,
                elapsed_time.as_millis()
            )
            .as_str(),
        );
        println!("{} {} {} {}", i.0, i.1, distance, elapsed_time.as_millis());
    }

    let path = "output";

    let mut output = File::create(path).unwrap();
    write!(output, "{}", text).unwrap();
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
    vertices: &'a HashMap<usize, Vertex>,
    offset_array: &'a Vec<usize>,
    edges: &'a Vec<Edge>,
}

impl<'a> Dijkstra<'a> {
    fn new(
        vertices: &'a HashMap<usize, Vertex>,
        offset_array: &'a Vec<usize>,
        edges: &'a Vec<Edge>,
    ) -> Self {
        let dist: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let pq: BinaryHeap<PQEntry> = BinaryHeap::new();

        Dijkstra {
            dist,
            pq,
            vertices,
            offset_array,
            edges,
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
