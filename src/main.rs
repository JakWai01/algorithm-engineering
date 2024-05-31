use core::num;
use std::{collections::BinaryHeap, env, fs::File, io, mem, thread::current, time::Instant};
extern crate itertools;
use crate::{
    bidirectional_ch::BidirectionalContractionHierarchies,
    ch::ContractionHierarchies,
    dijkstra::Dijkstra,
    objects::Edge,
    utils::{
        cell_to_id, create_offset_array, create_predecessor_offset_array, find_bounds,
        get_grid_cell, read_fmi, read_lines,
    },
};
use bitvec::vec::BitVec;
use objects::Vertex;
use pq::PQEntry;
mod dijkstra;
mod pq;
use io::Write;
mod bidirectional_ch;
mod ch;
mod objects;
mod phast;
mod utils;

fn main() {
    let args: Vec<String> = env::args().collect();
    let file_path: &String = &args[1];
    let dijkstra_pairs_file_path: &String = &args[2];

    let (mut vertices, mut edges) = read_fmi(file_path);

    let num_vertices = vertices.len();

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

    let (mut edges_up, mut edges_down): (Vec<Edge>, Vec<Edge>) = edges
        .iter()
        .partition(|edge| vertices[edge.start_vertex].level < vertices[edge.end_vertex].level);

    let mut edge_clone_down = edges_down.clone();

    edge_clone_down.sort_by_key(|edge| edge.start_vertex);

    let edges_down_offset = create_offset_array(&edge_clone_down, num_vertices);

    edges_down.iter_mut().for_each(|edge| {
        std::mem::swap(&mut edge.start_vertex, &mut edge.end_vertex);
    });

    edges_up.sort_by_key(|edge| edge.start_vertex);
    edges_down.sort_by_key(|edge| edge.start_vertex);

    let mut reverse_edge_up = edges_up.clone();

    reverse_edge_up.iter_mut().for_each(|edge| {
        std::mem::swap(&mut edge.start_vertex, &mut edge.end_vertex);
    });
    reverse_edge_up.sort_by_key(|edge| edge.start_vertex);

    let up_edges_reversed_offset = create_offset_array(&reverse_edge_up, num_vertices);

    let offset_array: Vec<usize> = create_offset_array(&edges, num_vertices);
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

    // Stuttgart
    // let s = 377371;
    // let t = 754742;

    let mut text: String = "".to_string();

    for i in &source_target_tuples {
        let mut ch = BidirectionalContractionHierarchies::new(
            num_vertices,
            &offset_array_up,
            &offset_array_down,
            &edges_up,
            &edges_down,
            &offset_array_up_predecessors,
            &offset_array_down_predecessors,
        );
        let now = Instant::now();
        let (distance, _) = ch.bidirectional_ch_query(i.0, i.1);
        let elapsed_time: std::time::Duration = now.elapsed();
        text.push_str(
            format!(
                "{} {} {} {}us\n",
                i.0,
                i.1,
                distance,
                elapsed_time.as_micros()
            )
            .as_str(),
        );
        println!("{} {} {} {}", i.0, i.1, distance, elapsed_time.as_micros());
    }

    let path = "Waibel.t0";

    let mut output = File::create(path).unwrap();
    write!(output, "{}", text).unwrap();

    let mut path_finding = dijkstra::Dijkstra::new(
        num_vertices,
        &offset_array_up,
        &offset_array_down,
        &edges_up,
        &edges_down,
        &offset_array_up_predecessors,
        &offset_array_down_predecessors,
    );
    let s = source_target_tuples[0].0;
    let t = source_target_tuples[0].1;
    let (distance, _) = path_finding.bidirectional_ch_query(s, t);

    println!("Distance: {}", distance);

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

    let s = source_target_tuples[0].0;
    let mut text: String = "".to_string();

    edges.sort_by_key(|edge: &Edge| edge.end_vertex);
    let predecessor_offset = create_predecessor_offset_array(&edges, num_vertices);

    let mut vertices_by_level_desc = vertices.clone();
    vertices_by_level_desc.sort_by_key(|v| v.level);
    vertices_by_level_desc.reverse();

    let phast_timer = Instant::now();

    let (distances, _, _) = phast::phast_new(
        &mut phast_path_finding,
        s,
        &vertices,
        &vertices_by_level_desc,
        &edges_down_offset,
        &edge_clone_down,
    );

    let phast_time = phast_timer.elapsed();

    assert_eq!(436627, distances[754742]);
    println!("peek-target = {}", 436627 - 164584);

    for i in 0..num_vertices {
        text.push_str(format!("{} {}\n", i, distances[i]).as_str());
    }

    // Laufzeit in letzter Zeile anzeigen
    text.push_str(format!("Laufzeit: {:?}", phast_time).as_str());

    let path = "Waibel.t1";

    let mut output = File::create(path).unwrap();
    write!(output, "{}", text).unwrap();

    //                      ______ _
    //     /\              |  ____| |
    //    /  \   _ __ ___  | |__  | | __ _  __ _ ___
    //   / /\ \ | '__/ __| |  __| | |/ _` |/ _` / __|
    //  / ____ \| | | (__  | |    | | (_| | (_| \__ \
    // /_/    \_\_|  \___| |_|    |_|\__,_|\__, |___/
    //                                      __/ |
    //                                     |___/

    let m_rows = 100;
    let n_columns = 100;

    let (min_bound, max_bound) = find_bounds(&vertices);

    println!("Min bound: {:?}, max_bound: {:?}", min_bound, max_bound);

    for vertex in &mut vertices {
        let (x_cell, y_cell) = get_grid_cell(*vertex, min_bound, max_bound, m_rows, n_columns);
        vertex.grid_cell = (x_cell as f64, y_cell as f64);
    }

    println!(
        "Cell {} of start: {:?}",
        s,
        cell_to_id(vertices.get(s).unwrap().grid_cell, m_rows, n_columns)
    );
    println!(
        "Cell {} of destination: {:?}",
        t,
        cell_to_id(vertices.get(t).unwrap().grid_cell, m_rows, n_columns)
    );

    let mut vertices_grid = vertices.clone();

    // The group_by function groups consecutive elements into groups
    vertices_grid.sort_by(|x, y| x.grid_cell.partial_cmp(&y.grid_cell).unwrap());

    use bitvec::bitvec;

    let mut arc_flags: Vec<BitVec> = vec![bitvec![0; m_rows * n_columns]; edges.len()];

    edges.sort_by_key(|edge: &Edge| edge.id);

    // Construct reverse graph for reverse dijkstra
    let mut reverse_edges = edges.clone();
    reverse_edges
        .iter_mut()
        .for_each(|edge| mem::swap(&mut edge.start_vertex, &mut edge.end_vertex));

    // Sort by end_vertex again for the creation of the predecessor array to work
    // reverse_edges.sort_by_key(|edge: &Edge| edge.start_vertex);

    // let reverse_predecessor_offset_array =
    // create_predecessor_offset_array(&reverse_edges, num_vertices);

    // Prepare the data again but this time for the reverse query
    let (mut reverse_edges_up, mut reverse_edges_down): (Vec<Edge>, Vec<Edge>) =
        reverse_edges.iter().partition(|edge| {
            vertices.get(edge.start_vertex).unwrap().level
                < vertices.get(edge.end_vertex).unwrap().level
        });

    // reverse_edges_down.iter_mut().for_each(|edge| {
    //     std::mem::swap(&mut edge.start_vertex, &mut edge.end_vertex);
    // });

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

    // Swap all up and downs here
    let mut arc_flags_path_finding = Dijkstra::new(
        num_vertices,
        &reverse_offset_array_up,
        &reverse_offset_array_down,
        // Reverse edges up sind die edges aus edges_down
        &reverse_edges_up,
        &reverse_edges_down,
        &reverse_offset_array_up_predecessors,
        &reverse_offset_array_down_predecessors,
    );

    let s = 754742;

    // Debug
    // edges.sort_by_key(|edge: &Edge| edge.end_vertex);

    // for neighbour in predecessor_offset[32694]..predecessor_offset[32694 + 1] {
    //     println!(
    //         "Normal neighbours of peek: {:?}",
    //         edges.get(neighbour).unwrap()
    //     );
    // }
    for neighbour in reverse_offset_array_down[32694]..reverse_offset_array_down[32694 + 1] {
        println!(
            "Neighbour of peek: {:?}",
            reverse_edges_down.get(neighbour).unwrap()
        )
    }

    // If there is a fault at this, execute a single fast before that, panic after but use the data structures and the example. We should be able to find a path between our standard nodes
    let (distances, predecessors, predecessor_edges) = phast::phast_new(
        &mut arc_flags_path_finding,
        s,
        &vertices,
        &vertices_by_level_desc,
        &reverse_offset_array_down,
        &reverse_edges_down,
    );

    println!(
        "Predecessor edge of 621003: {:?}",
        reverse_edges.get(predecessor_edges[621003]).unwrap()
    );
    // if s == 754742 {
    //     println!(
    //         "Predecessor edge of 173169: {:?}",
    //         reverse_edges.get(predecessor_edges[173169]).unwrap()
    //     );
    //     assert_eq!(572116, predecessor_edges[173169]);
    // }

    let path: Vec<usize> = vec![
        377371, 377370, 357425, 357426, 119334, 119339, 173169, 119364, 1104076, 415851, 10557,
        10938, 621003, 183053, 872669, 16825, 407460, 6193, 6215, 64916, 71162, 92904, 306882,
        754755, 754745, 754736, 911476, 754741, 754742,
    ];

    for v in path {
        println!("Predecessor edge of {} is {:?}", v, predecessor_edges[v])
    }

    // This should yield the correct distance but it doesnt. Debug the fast query next
    assert_eq!(436627, distances[377371]);

    println!("Distance to 436627: {}", distances[377371]);

    println!("predecessor of 377371: {}", predecessors[377371]);
    println!("predecessor of 377370: {}", predecessors[377370]);

    // Rewrite arc flags
    let preproc = Instant::now();
    for vertex in &vertices {
        let current_cell = vertex.grid_cell;
        let current_cell_id = cell_to_id(current_cell, m_rows, n_columns);

        if current_cell_id != 2470 {
            continue;
        }

        println!("Vertex {}/{}", vertex.id, num_vertices);

        for edge in predecessor_offset[vertex.id]..predecessor_offset[vertex.id + 1] {
            let target_edge = edges[edge];
            let target_node = vertices[target_edge.start_vertex];
            let neighbour_cell = target_node.grid_cell;

            if current_cell != neighbour_cell {
                let s = vertex.id;

                // Swap all up and downs here
                let mut arc_flags_path_finding = Dijkstra::new(
                    num_vertices,
                    &reverse_offset_array_up,
                    &reverse_offset_array_down,
                    // Reverse edges up sind die edges aus edges_down
                    &reverse_edges_up,
                    &reverse_edges_down,
                    &reverse_offset_array_up_predecessors,
                    &reverse_offset_array_down_predecessors,
                );

                let (distances, predecessors, predecessor_edges) = phast::phast_new(
                    &mut arc_flags_path_finding,
                    s,
                    &vertices,
                    &vertices_by_level_desc,
                    &reverse_offset_array_down,
                    &reverse_edges_down,
                );

                // if s == 754742 {
                //     println!(
                //         "Predecessor edge of 173169: {:?}",
                //         reverse_edges.get(predecessor_edges[173169]).unwrap()
                //     );
                //     assert_eq!(572116, predecessor_edges[173169]);
                // }

                // Construct shortest path tree
                for v in &vertices {
                    let mut current_vertex: usize = v.id;

                    if s == 754742 && current_vertex == 377371 {
                        println!("Distance: {:?}", distances[377371]);
                        if distances[current_vertex] != ((usize::MAX / 2) - 1) {
                            println!(
                                "Predecessor of our node: {:?}",
                                predecessors[current_vertex]
                            );
                        }
                    }

                    if current_vertex != s {
                        // If infinite, then there is no path from s to current_vertex
                        if distances[current_vertex] == ((usize::MAX / 2) - 1) {
                            continue;
                        }

                        if s == 754742 && v.id == 377371 {
                            println!("Path start: {}", current_vertex);
                        }

                        // Für 119339 ist hier eine random Edge als predecessor eingetragen, die nichtmal zu 119339 führt
                        let mut predecessor_edge_id = predecessor_edges[current_vertex];

                        loop {
                            if s == 754742 && v.id == 377371 {
                                println!("Marking edge {}", predecessor_edge_id);
                            }

                            // We are marking the reverse edges down here. That will be the error
                            arc_flags[predecessor_edge_id].set(current_cell_id, true);

                            current_vertex = predecessors[current_vertex];
                            if s == 754742 && v.id == 377371 {
                                println!("Next one: {}", current_vertex);
                            }
                            if current_vertex == s {
                                break;
                            }

                            predecessor_edge_id = predecessor_edges[current_vertex];
                        }
                    }
                }
            } else {
                // In the 0 0 case, we are always in here
                arc_flags[edge].set(current_cell_id, true);
            }
        }
    }

    println!(
        "Arc Flags preproc took: {:?}",
        preproc.elapsed().as_millis()
    );

    // This is absolutely useless, we are looking at edges here
    println!(
        "Arc flag edge 939840: {:?}",
        arc_flags[939840]
            .iter()
            .filter(|item| *item.as_ref() == true)
            .collect::<BitVec>()
    );
    println!(
        "Arc flag edge 3015456: {:?}",
        arc_flags[3015456]
            .iter()
            .filter(|item| *item.as_ref() == true)
            .collect::<BitVec>()
    );

    println!("Edge that was marked: {:?}", edges.get(473811).unwrap());
    println!(
        "Edge that should have been marked: {:?}",
        edges.get(572116).unwrap()
    );

    println!("Sanity check: {:?}", edges.get(939840).unwrap());

    let (arc_dist, predecessors) = arc_flags_query(
        source_target_tuples[0].0,
        source_target_tuples[0].1,
        vertices,
        edges,
        offset_array,
        m_rows,
        n_columns,
        arc_flags,
        num_vertices,
    );

    println!("Arc dist {}", arc_dist);

    let mut current = source_target_tuples[0].1;
    while current != source_target_tuples[0].0 {
        println!("Path current: {}", current);
        current = predecessors[current]
    }
    println!("Path current: {}", source_target_tuples[0].0);

    assert_eq!(436627, arc_dist);
}

fn arc_flags_query(
    start_node: usize,
    target_node: usize,
    vertices: Vec<Vertex>,
    edges: Vec<Edge>,
    offset_array: Vec<usize>,
    m_rows: usize,
    n_columns: usize,
    arc_flags: Vec<BitVec>,
    num_vertices: usize,
) -> (usize, Vec<usize>) {
    let mut dist: Vec<usize> = (0..vertices.len())
        .map(|_| ((usize::MAX / 2) - 1))
        .collect();
    let mut pq: BinaryHeap<PQEntry> = BinaryHeap::new();
    let mut predecessors = (0..num_vertices).map(|_| usize::MAX).collect();

    // Determine cell of target
    let id = cell_to_id(
        vertices.get(target_node).unwrap().grid_cell,
        m_rows,
        n_columns,
    );

    println!("Cell of target: {}", id);

    dist[start_node] = 0;
    pq.push(PQEntry {
        distance: 0,
        vertex: start_node,
    });

    let path: Vec<usize> = vec![
        377371, 377370, 357425, 357426, 119334, 119339, 173169, 119364, 1104076, 415851, 10557,
        10938, 621003, 183053, 872669, 16825, 407460, 6193, 6215, 64916, 71162, 92904, 306882,
        754755, 754745, 754736, 911476, 754741, 754742,
    ];
    while let Some(PQEntry { distance, vertex }) = pq.pop() {
        if vertex == target_node {
            return (distance, predecessors);
        };

        for j in offset_array[vertex]..offset_array[vertex + 1] {
            let edge = edges.get(j).unwrap();
            // if path.contains(&edge.end_vertex) {
            //     println!("Current neighbour: {}", edge.end_vertex);
            // }
            if vertex == 119339 {
                println!("My neighbour: {:?}", edge);
            }
            if let Some(bit) = arc_flags[j].get(id as usize) {
                if vertex == 119339 {
                    println!("Bit of my neighbour: {:?}", *bit.as_ref());
                }
                if *bit.as_ref() == true {
                    let edge = edges.get(j).unwrap();
                    if vertex == 119339 {
                        println!(
                            "{} > {} + {}",
                            dist[edge.end_vertex], dist[vertex], edge.weight
                        );
                    }
                    if dist[edge.end_vertex] > dist[vertex] + edge.weight {
                        if vertex == 119339 {
                            println!("Relaxing {:?}", edge);
                        }
                        if edge.end_vertex == 173169 {
                            println!("Relaxing {:?}", edge);
                            println!(
                                "with distance: {} > {} {}",
                                dist[edge.end_vertex], dist[vertex], edge.weight
                            );
                        }
                        // if path.contains(&edge.end_vertex) {
                        //     println!("Relaxing {:?}", edge);
                        // }
                        dist[edge.end_vertex] = dist[vertex] + edge.weight;
                        pq.push(PQEntry {
                            distance: dist[vertex] + edge.weight,
                            vertex: edge.end_vertex,
                        });
                        predecessors[edge.end_vertex] = vertex;
                    }
                }
            }
        }
    }
    (usize::MAX, Vec::new())
}
