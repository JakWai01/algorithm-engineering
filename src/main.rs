use std::{
    cmp::Ordering, collections::{BinaryHeap, HashMap, HashSet}, env, fs::File, io::{self, BufRead}, path::Path, time::{self, Instant}
};
use rand::Rng;

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
                start_vertex: start_vertex,
                end_vertex: end_vertex,
                weight: weight,
                typ: typ,
                max_speed: max_speed,
            };
            edges.push(edge);

            let back_edge = Edge {
                start_vertex: end_vertex,
                end_vertex: start_vertex,
                weight: weight,
                typ: typ,
                max_speed: max_speed,
            };
            edges.push(back_edge);
        }
    }

    // Sort by starting node in order to get offset array
    edges.sort_by_key(|edge| edge.start_vertex);

    // num_edges != edges.len() since we are converting the edges to an undirected graph
    // by inserting the edges another time in reverse direction
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

    // Finding connected components of a graph using DFS
    // All vertices are initially initialized with a marker value of 0 which can be interpreted as
    // the component not being marked

    let now_cc = Instant::now();
    let mut c: usize = 0;

    let mut visited: HashSet<usize> = HashSet::new();

    for v in vertices.values() {
        if !visited.contains(&v.id) {
            c += 1;
            dfs(v.id, &mut visited, &offset_array, &edges);
        }
    }
    let elapsed_cc = now_cc.elapsed();
    println!("Number of connected components {} took {} ms to execute", c, elapsed_cc.as_millis());

    
    let mut random_node_tuples: Vec<(usize, usize)> = Vec::new();

    // for _ in 0..100 {
    //     let start = rand::thread_rng().gen_range(0..num_vertices);
    //     let target = rand::thread_rng().gen_range(0..num_vertices);
    //     random_node_tuples.push((start, target));
    // }
        
    let mut path_finding = Dijkstra::new(&vertices, &offset_array, &edges);
    
    let now = Instant::now();
    // for i in random_node_tuples {
    //     let distance = path_finding.query(i.0, i.1);
    //     println!("Distance from {} to {}: {}", i.0, i.1, distance);
    // }
    let distance = path_finding.query(377376, 754742); // should return 437160
    println!("Distance from {} to {}: {}", 377376, 754742, distance); 
    let elapsed_time = now.elapsed();
    
    println!("Running dijkstra took {} ms to execute", elapsed_time.as_millis());
    // println!("Distance {}", distance);
    
    // println!("distances: {:?}", distances);
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct PQEntry {
    distance: usize,
    vertex: usize,
}

impl Ord for PQEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.distance.cmp(&self.distance).then_with(|| self.vertex.cmp(&other.vertex))
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
    edges: &'a Vec<Edge>
}

impl<'a> Dijkstra<'a> {
    fn new(vertices: &'a HashMap<usize, Vertex>, offset_array: &'a Vec<usize>, edges: &'a Vec<Edge>) -> Self {
        let dist: Vec<usize> =  (0..vertices.len()).map(|_| usize::MAX).collect();
        let pq: BinaryHeap<PQEntry> = BinaryHeap::new();

        Dijkstra {
            dist,
            pq,
            vertices,
            offset_array,
            edges
        }
    }

    fn query(&mut self, start_node: usize, target_node: usize) -> usize {
        self.dist = (0..self.vertices.len()).map(|_| usize::MAX).collect();
        self.pq.clear();

        self.dist[start_node] = 0;
        self.pq.push(PQEntry{distance: 0, vertex: start_node});

        while let Some(PQEntry{ distance, vertex }) = self.pq.pop() {
            if vertex == target_node { return distance };

            for j in self.offset_array[vertex]..self.offset_array[vertex+1] {
                let edge = self.edges.get(j).unwrap();
                if self.dist[edge.end_vertex] > self.dist[vertex] + edge.weight {
                    self.dist[edge.end_vertex] = self.dist[vertex] + edge.weight;
                    self.pq.push(PQEntry{ distance: self.dist[vertex] + edge.weight, vertex: edge.end_vertex});
                }
            } 
        }
        0
    }
}

fn dfs(start_node: usize, visited: &mut HashSet<usize>, offset_array: &Vec<usize>, edges: &Vec<Edge>) {
    let mut stack = Vec::new();

    stack.push(start_node);
    visited.insert(start_node);

    while !stack.is_empty() {
        if let Some(current_vertex) = stack.pop() {
            for j in offset_array[current_vertex]..offset_array[current_vertex+1] {
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
