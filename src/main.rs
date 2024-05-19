use std::{
    env,
    fs::File,
    io::{self, BufRead},
    mem,
    path::Path,
    time::Instant,
};
extern crate itertools;
use crate::dijkstra::Dijkstra;
use itertools::Itertools;
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

    let offset_array_up_predecessors: Vec<usize> =
        create_predecessor_offset_array(&predecessor_upward_edges, num_vertices);

    let offset_array_down_predecessors: Vec<usize> =
        create_predecessor_offset_array(&predecessor_downward_edges, num_vertices);

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

    // Stuttgart
    // let s = 377371;
    // let t = 754742;
    let s = 695445;
    let t = 7620;

    // Germany
    // let s = 8371825;
    // let t = 16743651;
    let (distance, current_min) = path_finding.bidirectional_ch_query(s, t);

    println!("Distance: {}", distance);

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

    // Stuttgart 436627
    // Germany 648681
    // assert_eq!(distance, 436627.0);

    println!(
        "Finished bi-directional CH query in: {}us",
        elapsed.as_micros()
    );

    let mut edge_path: Vec<usize> = Vec::new();

    for vertex in upwards_path {
        let edge = path_finding.predecessor_edges_up[vertex];
        if edge == usize::MAX {
            continue;
        }
        let e = edges.get(edge).unwrap();
        edge_path.push(e.id);
    }

    for vertex in downwards_path {
        let edge = path_finding.predecessor_edges_down[vertex];
        if edge == usize::MAX {
            continue;
        }
        let e = edges.get(edge).unwrap();
        edge_path.push(e.id);
    }

    println!("Edge path: {:?}", edge_path);

    let qwe = edges.get(2827630).unwrap();
    println!("Edge: {:?}", qwe);

    println!(
        "Start {:?} End {:?}",
        vertices.get(qwe.start_vertex).unwrap(),
        vertices.get(qwe.end_vertex).unwrap()
    );

    // Unpack shortcuts
    let mut unpack_stack: Vec<usize> = Vec::new();
    let mut unpacked_path: Vec<usize> = Vec::new();

    for edge in edge_path {
        unpack_stack.push(edge);
        while !unpack_stack.is_empty() {
            let edge_id = unpack_stack.pop().unwrap();
            let edge = edges.get(edge_id).unwrap();
            if edge.edge_id_a != -1 && edge.edge_id_b != -1 {
                unpack_stack.push(edge.edge_id_b as usize);
                unpack_stack.push(edge.edge_id_a as usize);
            } else {
                unpacked_path.push(edge.id);
            }
        }
    }

    println!("Unpacked path length: {:?}", unpacked_path.len());

    // _____  _    _           _____ _______
    // |  __ \| |  | |   /\    / ____|__   __|
    // | |__) | |__| |  /  \  | (___    | |
    // |  ___/|  __  | / /\ \  \___ \   | |
    // | |    | |  | |/ ____ \ ____) |  | |
    // |_|    |_|  |_/_/    \_\_____/   |_|

    let mut phast_path_finding = Dijkstra::new(
        num_vertices,
        &offset_array_up,
        &offset_array_down,
        &edges_up,
        &edges_down,
        &offset_array_up_predecessors,
        &offset_array_down_predecessors,
    );

    let s = 695445;

    edges.sort_by_key(|edge: &Edge| edge.end_vertex);
    let predecessor_offset = create_predecessor_offset_array(&edges, num_vertices);

    let (distances, predecessors, predecessor_edges) = phast_query(
        &mut phast_path_finding,
        s,
        &vertices,
        &predecessor_offset,
        &edges,
    );

    let mut current = 7610;
    println!("LEVEL of 7610: {}", vertices.get(current).unwrap().level);

    while predecessors[current] != 695445 {
        println!(
            "Predecessor of {}: {}, level of {}: {}",
            current,
            predecessors[current],
            predecessors[current],
            vertices.get(predecessors[current]).unwrap().level
        );
        println!(
            "Predecessor edge of {}: {}",
            current, predecessor_edges[current]
        );
        current = predecessors[current]
    }

    println!(
        "Predecessor of 52908: {}, level: {}",
        predecessors[52908],
        vertices.get(predecessors[52908]).unwrap().level
    );
    println!("Predecessor edge of 52908: {}", predecessor_edges[52908]);

    println!("Distances {}", distances[7610]);

    // assert!(false);
    // Note: The PHAST part seems to be working!

    // assert_eq!(164584, distances[183053]);
    // assert_eq!(435351, distances[754743]);
    // assert_eq!(435675, distances[754751]);

    //                      ______ _
    //     /\              |  ____| |
    //    /  \   _ __ ___  | |__  | | __ _  __ _ ___
    //   / /\ \ | '__/ __| |  __| | |/ _` |/ _` / __|
    //  / ____ \| | | (__  | |    | | (_| | (_| \__ \
    // /_/    \_\_|  \___| |_|    |_|\__,_|\__, |___/
    //                                      __/ |
    //                                     |___/

    println!("ARC FLAGS");
    let m_rows = 8;
    let n_columns = 6;

    let (min_bound, max_bound) = find_bounds(&vertices);

    println!("Min bound: {:?}, max_bound: {:?}", min_bound, max_bound);

    for vertex in &mut vertices {
        let (x_cell, y_cell) = get_grid_cell(*vertex, min_bound, max_bound, m_rows, n_columns);
        vertex.grid_cell = (x_cell as f64, y_cell as f64);
    }

    let mut vertices_grid = vertices.clone();

    // The group_by function groups consecutive elements into groups
    vertices_grid.sort_by(|x, y| x.grid_cell.partial_cmp(&y.grid_cell).unwrap());

    let mut arc_flags: Vec<Vec<bool>> = vec![vec![false; m_rows * n_columns]; edges.len()];

    // Construct reverse graph for reverse dijkstra
    let mut reverse_edges = edges.clone();
    reverse_edges
        .iter_mut()
        .for_each(|edge| mem::swap(&mut edge.start_vertex, &mut edge.end_vertex));

    // Sort by end_vertex again for the creation of the predecessor array to work
    reverse_edges.sort_by_key(|edge: &Edge| edge.end_vertex);

    let iowe = reverse_edges.get(2827630).unwrap();
    println!("Edge: {:?}", iowe);

    println!(
        "Start {:?}, End {:?}",
        vertices.get(iowe.start_vertex).unwrap(),
        vertices.get(iowe.end_vertex).unwrap()
    );

    assert_eq!(7610, reverse_edges.get(2827630).unwrap().start_vertex);

    let reverse_predecessor_offset_array =
        create_predecessor_offset_array(&reverse_edges, num_vertices);

    // Hier ist auf jeden Fall der richtige predecessor dabei
    for pred in reverse_predecessor_offset_array[695491]..reverse_predecessor_offset_array[695492] {
        println!(
            "Predecessor of 695491: {:?}",
            reverse_edges.get(pred).unwrap()
        );
    }

    // Bis hier hin ist es richtig

    // Ist die edge in reverse_edges?
    println!(
        "DAS IST DIE EDGE IM REVERSE ARRAY: {:?}",
        reverse_edges.get(2827630).unwrap()
    );
    let abc = reverse_edges.get(2827630).unwrap();
    println!(
        "DAS SIND DIE LEVEL VON START UND ENDPUKT: {} {}",
        vertices.get(abc.start_vertex).unwrap().level,
        vertices.get(abc.end_vertex).unwrap().level
    );

    // Prepare the data again but this time for the reverse query
    let (mut reverse_edges_up, mut reverse_edges_down): (Vec<Edge>, Vec<Edge>) = reverse_edges
        .iter()
        // Flip levels?
        .partition(|edge| {
            if edge.id == 2827630 {
                println!("edge.start_vertex: {}", edge.start_vertex);
                println!("LEVEL: {}", vertices.get(edge.start_vertex).unwrap().level);
                println!("edge.end_vertex: {}", edge.end_vertex);
                println!("LEVEL: {}", vertices.get(edge.end_vertex).unwrap().level);
            }
            vertices.get(edge.start_vertex).unwrap().level
                < vertices.get(edge.end_vertex).unwrap().level
        });

    for edge in &reverse_edges_down {
        if edge.id == 2827630 {
            println!("IM FALSCHEN ARRAY")
        }
    }

    println!("reverse_edges_up len: {}", reverse_edges_up.len());
    for edge in &reverse_edges_up {
        if edge.id == 2827630 {
            println!("DAS IST DIE EDGE: {:?}", edge);
        }
        if edge.start_vertex == 7610 && edge.end_vertex == 695491 {
            println!("REVERSE EDGES UP IS CORRECT")
        }

        if edge.end_vertex == 7610 && edge.start_vertex == 695491 {
            println!("REVERSE EDGES UP IS WRONG")
        }
    }

    // Ich glaube die brauchen wir gar nicht - wir machen ja nur die "upwards" search
    reverse_edges_down.iter_mut().for_each(|edge| {
        std::mem::swap(&mut edge.start_vertex, &mut edge.end_vertex);
    });

    reverse_edges_up.sort_by_key(|edge| edge.start_vertex);
    reverse_edges_down.sort_by_key(|edge| edge.start_vertex);

    let reverse_offset_array_up: Vec<usize> = create_offset_array(&reverse_edges_up, num_vertices);
    let reverse_offset_array_down: Vec<usize> =
        create_offset_array(&reverse_edges_down, num_vertices);

    // Create offset_array of predecessors as well
    let mut reverse_predecessor_upward_edges = reverse_edges_up.clone();
    let mut reverse_predecessor_downward_edges = reverse_edges_down.clone();

    reverse_predecessor_upward_edges.sort_by_key(|edge| edge.end_vertex);
    reverse_predecessor_downward_edges.sort_by_key(|edge| edge.end_vertex);

    let reverse_offset_array_up_predecessors: Vec<usize> =
        create_predecessor_offset_array(&reverse_predecessor_upward_edges, num_vertices);

    let reverse_offset_array_down_predecessors: Vec<usize> =
        create_predecessor_offset_array(&reverse_predecessor_downward_edges, num_vertices);

    let mut arc_flags_path_finding = Dijkstra::new(
        num_vertices,
        &reverse_offset_array_up,
        &reverse_offset_array_down,
        &reverse_edges_up,
        &reverse_edges_down,
        &reverse_offset_array_up_predecessors,
        &reverse_offset_array_down_predecessors,
    );

    let preproc = Instant::now();
    // Here the main arc-flags logic starts
    for (cell, group) in &vertices_grid.iter().group_by(|v| v.grid_cell) {
        let mut boundary_edges: Vec<&Edge> = Vec::new();
        println!("Looking at cell: {:?}", cell);
        // println!("Elements in group: {:?}", group.collect::<Vec<_>>().len());
        // Determine all boundary edges in this group
        for vertex in group {
            for edge in predecessor_offset[vertex.id]..predecessor_offset[vertex.id + 1] {
                let edge = edges.get(edge).unwrap();
                if vertices.get(edge.start_vertex).unwrap().grid_cell
                    != vertices.get(edge.end_vertex).unwrap().grid_cell
                    && vertices.get(edge.end_vertex).unwrap().grid_cell == cell
                {
                    // println!("Boundary Edge for vertex: {:?}, {:?}", vertex, edge);
                    // println!(
                    //     "Cell of boundary edge: start {:?} end {:?}",
                    //     vertices.get(edge.start_vertex).unwrap().grid_cell,
                    //     vertices.get(edge.end_vertex).unwrap().grid_cell
                    // );
                    // println!("{} {}", edge.start_vertex, edge.end_vertex);
                    // println!("Bounding edge has start_vertex_grid_cell: {:?} and end_vertex_grid_cell: {:?}", vertices.get(edge.start_vertex).unwrap().grid_cell, vertices.get(edge.end_vertex).unwrap().grid_cell);
                    boundary_edges.push(edge);
                }
            }
        }

        // println!("Boundary edges length: {:?}", boundary_edges.len());

        for boundary_edge in boundary_edges {
            // println!("Current boundary edge: {:?}", boundary_edge);
            // println!("{} {}", edge.start_vertex, edge.end_vertex);
            // Flip edges to create the reverse graph
            // let mut reverse_edges = edges.clone();
            // reverse_edges
            //     .iter_mut()
            //     .for_each(|edge| mem::swap(&mut edge.start_vertex, &mut edge.end_vertex));

            // println!("{} {}", edge.start_vertex, edge.end_vertex);
            // Run one-to-all query on reverse graph
            let start = boundary_edge.end_vertex;
            // println!("Start of one-to-all search {:?}", start);

            let (distances, predecessors, predecessor_edges) = phast_query(
                &mut arc_flags_path_finding,
                start,
                &vertices,
                &reverse_predecessor_offset_array,
                &reverse_edges,
            );

            // println!(
            //     "Distance after phast-query in arc flags from {}: {}",
            //     start, distances[695445]
            // );

            // Hier kommt ein falscher Wert raus, obwohl der Algorithmus funktioniert
            // Es muss also am input liegen
            // Wie verhalten sich die levels im korrekten Pfad?
            // println!("Predecessor of 695445 {}", predecessors[695445]);
            // assert_eq!(1611, distances[695445]);

            // Construct shortest path tree
            for vertex in &vertices {
                // println!("True vertex.id {}", vertex.id);
                let mut current_vertex = vertex.id;

                if current_vertex != start {
                    // Handle case where vertex was not reachable from s
                    if predecessor_edges[current_vertex] == usize::MAX {
                        continue;
                    }

                    println!("Current vertex: {:?}", current_vertex);

                    let mut predecessor_edge_id = predecessor_edges[current_vertex];
                    println!("Predecessor edge id: {}", predecessor_edge_id);

                    let mut predecessor_edge = reverse_edges.get(predecessor_edge_id).unwrap();

                    // Go back from the current node until we reach the start node inside of the target region
                    // while predecessor_edge.start_vertex != edge.end_vertex {
                    //     println!("Current predecessor_edge id: {}", predecessor_edge_id);
                    //     println!("Current predecessor_edge: {:?}", predecessor_edge);
                    //     // Mark arc flag
                    //     arc_flags[predecessor_edge.id]
                    //         [cell_to_id(cell, m_rows, n_columns) as usize] = true;

                    //     // Prepare data for next iteration
                    //     current_pred = predecessors[predecessor_edge.start_vertex];
                    //     predecessor_edge_id = predecessor_edges[current_pred];
                    //     predecessor_edge = reverse_edges.get(predecessor_edge_id).unwrap();
                    // }

                    // We want do-while since we also want to set the arc flags a last time when the end_vertex is the start vertex
                    loop {
                        // println!("Current predecessor_edge id: {}", predecessor_edge_id);
                        // println!("Current predecessor_edge: {:?}", predecessor_edge);
                        // Mark arc flag
                        arc_flags[predecessor_edge.id]
                            [cell_to_id(cell, m_rows, n_columns) as usize] = true;

                        // println!("ARC FLAG: {:?}", arc_flags[predecessor_edge_id]);

                        current_vertex = predecessors[current_vertex];
                        if current_vertex == start {
                            break;
                        }

                        // Prepare data for next iteration
                        // current_pred = predecessors[predecessor_edge.start_vertex];
                        // should be the same as current_pred = predecessor_edge.start_vertex;
                        // current_vertex = predecessors[vertex.id];
                        // println!("Updated current predecessor: {:?}", current_vertex);
                        predecessor_edge_id = predecessor_edges[current_vertex];
                        println!("Updated predecessor edge id {:?}", predecessor_edge_id);
                        if predecessor_edge_id == 336 {
                            println!("Edge: {:?}, Shown predecessor: 3221772", predecessor_edge);
                        }
                        if predecessor_edge_id == 823276 {
                            println!("Predecessor_edge: {:?}", predecessor_edge);
                        }
                        predecessor_edge = reverse_edges.get(predecessor_edge_id).unwrap();
                        // println!("Updated predecessor edge {:?}", predecessor_edge);
                    }
                }

                // break;
            }

            // break;
        }
    }

    println!(
        "Arc Flags preproc took: {:?}",
        preproc.elapsed().as_millis()
    );
}

fn cell_to_id(cell: (f64, f64), m_rows: usize, n_columns: usize) -> f64 {
    return (cell.0 * n_columns as f64) + cell.1;
}

fn phast_query(
    phast_path_finding: &mut Dijkstra,
    s: usize,
    vertices: &Vec<Vertex>,
    predecessor_offset: &Vec<usize>,
    edges: &Vec<Edge>,
) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
    // 1. Step: Execute a Dijkstra on the up-graph of source node s
    let up_graph_ch = Instant::now();
    let (mut distances, mut predecessors, mut predecessor_edges) =
        phast_path_finding.ch_query(s, &vertices);
    println!(
        "Executed PHAST up-graph search in: {:?}ms",
        up_graph_ch.elapsed().as_millis()
    );

    // Distance to known peek node from s to t (This is not actually the peek node in this search)
    // assert_eq!(164584, distances[183053]);

    // 2. Step: Consider all nodes u from high to low level and set d(u) = min{d(u), d(v) + c(v, u)}
    //          for nodes v with level(v) > level(u) and (v, u) ∈ E
    let phast_relaxation = Instant::now();
    let mut vertices_by_level_desc = vertices.clone();
    vertices_by_level_desc.sort_by_key(|v| v.level);
    vertices_by_level_desc.reverse();

    assert_eq!(vertices_by_level_desc.first().unwrap().level, 138);
    assert_eq!(vertices_by_level_desc.last().unwrap().level, 0);

    // edges.sort_by_key(|edge: &Edge| edge.end_vertex);
    // let predecessor_offset = create_predecessor_offset_array(&edges, num_vertices);

    // Consider all nodes in inverse level order
    for vertex in &vertices_by_level_desc {
        // Check incoming edges (u,v) with level(v) > level(u)
        for incoming_edge_id in predecessor_offset[vertex.id]..predecessor_offset[vertex.id + 1] {
            let incoming_edge = edges.get(incoming_edge_id).unwrap();

            // We do not want to check the peeks, we want to check the lower nodes and update them
            if vertices.get(incoming_edge.start_vertex).unwrap().level
                > vertices.get(incoming_edge.end_vertex).unwrap().level
            {
                if distances[vertex.id]
                    > distances[incoming_edge.start_vertex] + incoming_edge.weight
                {
                    distances[vertex.id] =
                        distances[incoming_edge.start_vertex] + incoming_edge.weight;
                    predecessors[vertex.id] = incoming_edge.start_vertex;
                    predecessor_edges[vertex.id] = incoming_edge.id;
                    // println!(
                    //     "Relaxed distance for vertex: {:?} and assigned predecessor {:?}",
                    //     vertex, incoming_edge.start_vertex
                    // );
                }
            }
        }
    }

    // println!("Distance from {} to 7610: {:?}", s, distances[7610]);

    println!(
        "Executed PHAST relaxation in: {:?}ms",
        phast_relaxation.elapsed().as_millis()
    );

    (
        distances.clone(),
        predecessors.clone(),
        predecessor_edges.clone(),
    )
}

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

fn create_offset_array(edges: &Vec<Edge>, num_vertices: usize) -> Vec<usize> {
    let mut offset_array: Vec<usize> = vec![edges.len(); num_vertices + 1];

    let mut previous_vertex_id = 0;
    offset_array[0] = 0;

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
