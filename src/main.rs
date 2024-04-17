use std::{env, fs::File, io::{self, BufRead}, path::Path, collections::HashMap};


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

    let mut lines =  read_lines(file_path).unwrap();
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
            // We don't need to store the whole vertex, lon and lat should be sufficient
            vertices.insert(vertex.id, vertex);
        }
    }

    for _ in 0..num_edges {
        if let Some(line) = lines.next() {
            let l = line.unwrap();
            let mut iter = l.split_whitespace();

            let edge = Edge {
                start_vertex: iter.next().unwrap().parse().unwrap(),
                end_vertex: iter.next().unwrap().parse().unwrap(),
                weight: iter.next().unwrap().parse().unwrap(),
                typ: iter.next().unwrap().parse().unwrap(),
                max_speed: iter.next().unwrap().parse().unwrap()
            };
            edges.push(edge);
        }
    }

    // Sort by starting node in order to get offset array
    edges.sort_by_key(|edge| edge.start_vertex);
    
    // num_edges != edges.len() since we are converting the edges to an undirected graph
    // by inserting the edges another time in reverse direction
    let mut offset_array: Vec<usize> = vec![edges.len(), num_vertices];
    
    // Initialize variables
    let mut previous_vertex_id = 0;
    offset_array.insert(0, 0);
    
    // If the the start_vertex changes in the edges vector, store the offset in the offset vector
    // and set this offset for all start_vertex id's that have been skipped in this last step.
    // However, I am not sure if this case even occurs in our road network.
    for (i, edge) in edges.iter().enumerate() {
        if edge.start_vertex != previous_vertex_id {
            for j in previous_vertex_id+1..=edge.start_vertex {
                offset_array.insert(j, i);
            }
            previous_vertex_id = edge.start_vertex;
        }
    }

    // Finding connected components of a graph using DFS
    // All vertices are initially initialized with a marker value of 0 which can be interpreted as
    // the component not being marked
    let mut c: usize = 0;

    let mut marked: HashMap<usize, usize> = HashMap::new();

    for vertex in vertices.values() {
        if !marked.contains_key(&vertex.id) {
            c += 1;
            depth_first_search(vertex, &offset_array, &edges, &vertices, &mut marked, c);
        }
    }
    
    println!("First five offsets of offset array {:?}", &offset_array[0..300]);

    println!("Last offsets of offset array {:?}", &offset_array.last());

    println!("First five edges of edge array {:?}", &edges[0..100]);

    println!("DONE");
}

fn get_neighbour_ids(vertex: &Vertex, offset_array: &Vec<usize>, edges: &Vec<Edge>) -> Vec<usize> {
    let mut offset_index = *offset_array.get(vertex.id).unwrap();
    let mut neighbour_ids: Vec<usize> = Vec::new();
    
    while edges.get(offset_index).unwrap().start_vertex == vertex.id {
        let neighbour_id = edges.get(offset_index).unwrap().end_vertex;
        neighbour_ids.push(neighbour_id);  
        offset_index += 1;
    }

    neighbour_ids
}

fn depth_first_search(vertex: &Vertex, offset_array: &Vec<usize>, edges: &Vec<Edge>, vertices: &HashMap<usize, Vertex>, marked: &mut HashMap<usize, usize>, c: usize) {
    marked.insert(vertex.id, c); 
    for id in get_neighbour_ids(vertex, offset_array, edges) {
        if let Some(ref mut neighbour) = vertices.get(&id) {
            if !marked.contains_key(&neighbour.id) {
                println!("Calling depth_first_search on neighbour: {:?}", neighbour);
                depth_first_search(neighbour, offset_array, edges, vertices, marked, c);
            }
        }
    }
}


// The output is wrapped in a Result to allow matching on errors.
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
