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

    let (distances, _, _) = phast::phast_new(
        &mut phast_path_finding,
        s,
        &vertices,
        &vertices_by_level_desc,
        &edges_down_offset,
        &edge_clone_down,
    );

    assert_eq!(436627, distances[754742]);
    println!("peek-target = {}", 436627 - 164584);

    for i in 0..num_vertices {
        text.push_str(format!("{} {}\n", i, distances[i]).as_str());
    }

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
    reverse_edges.sort_by_key(|edge: &Edge| edge.start_vertex);

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

    // If there is a fault at this, execute a single fast before that, panic after but use the data structures and the example. We should be able to find a path between our standard nodes
    let (distances, predecessors, predecessor_edges) = phast::phast_new(
        &mut arc_flags_path_finding,
        754742,
        &vertices,
        &vertices_by_level_desc,
        &reverse_offset_array_down,
        &reverse_edges_down,
    );

    // This should yield the correct distance but it doesnt. Debug the fast query next
    assert_eq!(436627, distances[377371]);

    println!("Distance to 436627: {}", distances[377371]);
    assert!(false);

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

                let mut arc_flags_path_finding = Dijkstra::new(
                    num_vertices,
                    &reverse_offset_array_up,
                    &reverse_offset_array_down,
                    &reverse_edges_up,
                    &reverse_edges_down,
                    &reverse_offset_array_up_predecessors,
                    &reverse_offset_array_down_predecessors,
                );

                // If there is a fault at this, execute a single fast before that, panic after but use the data structures and the example. We should be able to find a path between our standard nodes
                let (distances, predecessors, predecessor_edges) = phast::phast_new(
                    &mut arc_flags_path_finding,
                    s,
                    &vertices,
                    &vertices_by_level_desc,
                    &up_edges_reversed_offset,
                    &reverse_edges,
                );

                // Construct shortest path tree
                for v in &vertices {
                    let mut current_vertex: usize = v.id;

                    if s == 754742 && v.id == 377371 {
                        println!("Distance: {:?}", distances[377371]);
                        if distances[current_vertex] != ((usize::MAX / 2) - 1) {
                            println!(
                                "Predecessor of our node: {:?}",
                                predecessor_edges[current_vertex]
                            );
                        }
                    }
                    // if v.id == 377371 {
                    //     println!("pred edge id: {:?}", predecessor_edges[current_vertex]);
                    // }
                    if current_vertex != s {
                        // If infinite, then there is no path from s to current_vertex
                        if distances[current_vertex] == ((usize::MAX / 2) - 1) {
                            continue;
                        }
                        // println!("Vertex id {}", vertex.id);

                        let mut predecessor_edge_id = predecessor_edges[current_vertex];

                        // let mut predecessor_edge = reverse_edges.get(predecessor_edge_id).unwrap();

                        loop {
                            // println!("Predecessor edge id: {}", predecessor_edge_id);
                            // if predecessor_edge_id == 27771 {
                            //     println!(
                            //         "Set arc flag of vertex: {:?} for cell {}",
                            //         predecessor_edge_id, current_cell_id
                            //     );
                            // }
                            // println!("Set arc flag for vertex: {}", predecessor_edge.id);

                            // This is true
                            // println!("Predecessor edge id general: {:?}", predecessor_edge_id);
                            arc_flags[predecessor_edge_id].set(current_cell_id, true);

                            // println!(
                            //     "Sanity in cell {}check: {:?}",
                            //     current_cell_id,
                            //     *arc_flags[predecessor_edge_id]
                            //         .get(current_cell_id)
                            //         .unwrap()
                            //         .as_ref()
                            // );
                            current_vertex = predecessors[current_vertex];
                            if current_vertex == s {
                                break;
                            }

                            predecessor_edge_id = predecessor_edges[current_vertex];
                            // predecessor_edge = reverse_edges.get(predecessor_edge_id).unwrap();
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

    // for edge in &edges {
    //     if *arc_flags[edge.id].get(2470).unwrap().as_ref() {
    //         println!("Got one");
    //     }
    // }

    println!(
        "Arc flag edge s: {:?}",
        arc_flags[377371]
            .iter()
            .filter(|item| *item.as_ref() == true)
            .collect::<BitVec>()
    );
    println!(
        "Arc flag edge t: {:?}",
        arc_flags[754742]
            .iter()
            .filter(|item| *item.as_ref() == true)
            .collect::<BitVec>()
    );
    println!(
        "Arc flag edge t: {:?}",
        arc_flags[213251]
            .iter()
            .filter(|item| *item == true)
            .collect::<BitVec>()
    );
    let arc_dist = arc_flags_query(
        source_target_tuples[0].0,
        source_target_tuples[0].1,
        vertices,
        edges,
        offset_array,
        m_rows,
        n_columns,
        arc_flags,
    );

    println!("Arc dist {}", arc_dist);

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
) -> usize {
    let mut dist: Vec<usize> = (0..vertices.len())
        .map(|_| ((usize::MAX / 2) - 1))
        .collect();
    let mut pq: BinaryHeap<PQEntry> = BinaryHeap::new();

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

    while let Some(PQEntry { distance, vertex }) = pq.pop() {
        if vertex == target_node {
            return distance;
        };

        for j in offset_array[vertex]..offset_array[vertex + 1] {
            if let Some(bit) = arc_flags[j].get(id as usize) {
                if *bit.as_ref() == true {
                    println!("Visiting any node!");
                    if j == 754742 {
                        println!("Visiting target node");
                    }
                    let edge = edges.get(j).unwrap();
                    if j == 754742 {
                        println!("Target Edge {:?}", edge);
                        println!(
                            "Edge {} > {} + {}",
                            dist[edge.end_vertex], dist[vertex], edge.weight
                        );
                    }
                    if dist[edge.end_vertex] > dist[vertex] + edge.weight {
                        if edge.end_vertex == 754742 {
                            println!("Relaxing target edge!");
                        }
                        dist[edge.end_vertex] = dist[vertex] + edge.weight;
                        pq.push(PQEntry {
                            distance: dist[vertex] + edge.weight,
                            vertex: edge.end_vertex,
                        });
                    }
                } else {
                    println!("MARKING MISTAKE");
                }
            }
        }
    }
    usize::MAX
}
