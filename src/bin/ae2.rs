use algorithm_engineering::ae2::ch::ContractionHierarchies;
use algorithm_engineering::ae2::pq::PQEntry;
use bitvec::vec::BitVec;
use indicatif::ProgressIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use std::io::Write;
use std::{
    collections::BinaryHeap,
    collections::HashSet,
    env,
    fs::File,
    io::{self, BufWriter},
    mem,
    time::Instant,
};

extern crate itertools;

use algorithm_engineering::ae2::bidirectional_ch::BidirectionalContractionHierarchies;
use algorithm_engineering::ae2::objects::{Edge, Vertex};
use algorithm_engineering::ae2::utils::{
    cell_to_id, construct_spt, create_offset_array, create_predecessor_offset_array, find_bounds,
    get_grid_cell, read_fmi, read_lines,
};
use algorithm_engineering::ae2::{dj, phast};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    ch: String,

    #[arg(short, long)]
    queries: String,

    #[arg(short, long, default_value_t = 1)]
    m: u8,

    #[arg(short, long, default_value_t = 1)]
    n: u8,
}

fn main() {
    let args = Args::parse();

    // let args: Vec<String> = env::args().collect();
    let file_path: &String = &args.ch;
    let dijkstra_pairs_file_path: &String = &args.queries;

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

    // Dijkstra

    let mut dj = dj::Dijkstra::new(&vertices, &offset_array, &edges);

    let (_, edge_search) = dj.query(source_target_tuples[0].0, source_target_tuples[0].1);

    let mut search: String = "".to_string();

    search.push_str(format!("{} \n", vertices.len()).as_str());
    search.push_str(format!("{} \n", edge_search.len()).as_str());

    for vertex in &vertices {
        // let v = vertices.get(vertex).unwrap();
        search.push_str(format!("{} {}\n", vertex.lon, vertex.lat).as_str());
    }

    for edge in edge_search {
        let e = edges.get(edge).unwrap();
        search.push_str(format!("{} {} {} {}\n", e.start_vertex, e.end_vertex, 8, 2).as_str());
    }

    let path = "Waibel.t30";

    let mut output = File::create(path).unwrap();
    write!(output, "{}", search).unwrap();

    // CH
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
        let (distance, _, _, _) = ch.bidirectional_ch_query(i.0, i.1);
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

    // Suchräume
    // let mut search: String = "".to_string();
    let path = "Waibel.t31";
    let mut fmi_writer = BufWriter::new(File::create(path).unwrap());

    let mut chs = BidirectionalContractionHierarchies::new(
        num_vertices,
        &offset_array_up,
        &offset_array_down,
        &edges_up,
        &edges_down,
        &offset_array_up_predecessors,
        &offset_array_down_predecessors,
    );

    let (_, _, _, edge_search_space) =
        chs.bidirectional_ch_query(source_target_tuples[0].0, source_target_tuples[0].1);

    writeln!(fmi_writer, "{}", vertices.len()).unwrap();
    writeln!(fmi_writer, "{}", edge_search_space.len()).unwrap();

    for vertex in &vertices {
        writeln!(fmi_writer, "{} {}", vertex.lon, vertex.lat).unwrap();
    }

    for edge in edge_search_space {
        let e = edges.get(edge).unwrap();
        writeln!(
            fmi_writer,
            "{} {} {} {}",
            e.start_vertex, e.end_vertex, 8, 1
        )
        .unwrap();
    }

    fmi_writer.flush().unwrap();

    let mut output = File::create(path).unwrap();
    write!(output, "{}", search).unwrap();

    // _____  _    _           _____ _______
    // |  __ \| |  | |   /\    / ____|__   __|
    // | |__) | |__| |  /  \  | (___    | |
    // |  ___/|  __  | / /\ \  \___ \   | |
    // | |    | |  | |/ ____ \ ____) |  | |
    // |_|    |_|  |_/_/    \_\_____/   |_|

    let phast_path_finding = ContractionHierarchies::new(num_vertices, &offset_array_up, &edges_up);

    // let mut phast_path_finding =
    //     ContractionHierarchies::new(num_vertices, &offset_array_up, &edges_up);

    let s = source_target_tuples[0].0;

    edges.sort_by_key(|edge: &Edge| edge.end_vertex);
    let predecessor_offset = create_predecessor_offset_array(&edges, num_vertices);

    let mut vertices_by_level_desc = vertices.clone();
    vertices_by_level_desc.sort_by_key(|v| v.level);
    vertices_by_level_desc.reverse();

    let phast_timer = Instant::now();

    let (distances, _, _) = phast::phast(
        phast_path_finding,
        s,
        &vertices_by_level_desc,
        &edges_down_offset,
        &edge_clone_down,
    );

    let phast_time = phast_timer.elapsed();
    println!("PHAST time: {:?}", phast_time);

    // assert_eq!(436627, distances[754742]);
    // println!("peek-target = {}", 436627 - 164584);

    let path = "Waibel.t1";

    let mut fmi_writer = BufWriter::new(File::create(path).unwrap());

    for i in 0..num_vertices {
        writeln!(fmi_writer, "{} {}", i, distances[i]).unwrap();
    }

    // Laufzeit in letzter Zeile anzeigen
    // text.push_str(format!("Laufzeit: {:?}", phast_time).as_str());
    writeln!(fmi_writer, "{:?}", phast_time).unwrap();

    fmi_writer.flush().unwrap();

    //                      ______ _
    //     /\              |  ____| |
    //    /  \   _ __ ___  | |__  | | __ _  __ _ ___
    //   / /\ \ | '__/ __| |  __| | |/ _` |/ _` / __|
    //  / ____ \| | | (__  | |    | | (_| | (_| \__ \
    // /_/    \_\_|  \___| |_|    |_|\__,_|\__, |___/
    //                                      __/ |
    //                                     |___/

    let m_rows = args.m as usize;
    let n_columns = args.n as usize;

    let (min_bound, max_bound) = find_bounds(&vertices);

    println!("Min bound: {:?}, max_bound: {:?}", min_bound, max_bound);

    for vertex in &mut vertices {
        let (x_cell, y_cell) = get_grid_cell(*vertex, min_bound, max_bound, m_rows, n_columns);
        vertex.grid_cell = (x_cell as f64, y_cell as f64);
    }

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

    // Prepare the data again but this time for the reverse query
    let (mut reverse_edges_up, mut reverse_edges_down): (Vec<Edge>, Vec<Edge>) =
        reverse_edges.iter().partition(|edge| {
            vertices.get(edge.start_vertex).unwrap().level
                < vertices.get(edge.end_vertex).unwrap().level
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

    // Sort edges by level instead of vertices
    let mut red_by_level = reverse_edges_down.clone();
    red_by_level.sort_by_key(|e| vertices.get(e.end_vertex).unwrap().level);
    red_by_level.reverse();

    let preproc = Instant::now();

    // Determine boundary nodes -- 2094229
    let mut boundary_nodes = HashSet::new();

    for vertex in &vertices {
        let current_cell = vertex.grid_cell;
        let current_cell_id = cell_to_id(current_cell, m_rows, n_columns);

        for edge in predecessor_offset[vertex.id]..predecessor_offset[vertex.id + 1] {
            let target_edge: Edge = reverse_edges[edge];
            let target_node = vertices[target_edge.end_vertex];
            let neighbour_cell_id = cell_to_id(target_node.grid_cell, m_rows, n_columns);

            if current_cell_id != neighbour_cell_id {
                boundary_nodes.insert(vertex);
            } else {
                // In the 0 0 case, we are always in here
                arc_flags[edge].set(current_cell_id, true);
            }
        }
    }

    println!("Number of boundary nodes: {}", boundary_nodes.len());

    boundary_nodes
        .iter()
        .progress_count(boundary_nodes.len() as u64)
        .for_each(|node| {
            let object_creation = Instant::now();
            let arc_flags_path_finding = ContractionHierarchies::new(
                num_vertices,
                &reverse_offset_array_up,
                &reverse_edges_up,
            );
            // println!("Object creation took: {:?}", object_creation.elapsed());

            let object_creation = Instant::now();
            let (distances, predecessors, predecessor_edges) = phast::phast_by_edges(
                arc_flags_path_finding,
                node.id,
                &red_by_level,
                &reverse_offset_array_down,
                &reverse_edges_down,
            );
            // println!("PHAST took: {:?}", object_creation.elapsed());

            let object_creation = Instant::now();
            construct_spt(
                node.id,
                &vertices,
                &distances,
                &predecessors,
                &predecessor_edges,
                cell_to_id(node.grid_cell, m_rows, n_columns),
                &mut arc_flags,
            );
            // println!("SPT took: {:?}", object_creation.elapsed());
        });
    // for node in &boundary_nodes {}

    // for vertex in &vertices {
    //     let current_cell = vertex.grid_cell;
    //     let current_cell_id = cell_to_id(current_cell, m_rows, n_columns);

    //     // if current_cell_id != 2470 {
    //     //     continue;
    //     // }

    //     if vertex.id % 1000 == 0 {
    //         println!("Vertex {}/{}", vertex.id, num_vertices);
    //     }

    //     for edge in predecessor_offset[vertex.id]..predecessor_offset[vertex.id + 1] {
    //         let target_edge: Edge = reverse_edges[edge];
    //         let target_node = vertices[target_edge.end_vertex];
    //         let neighbour_cell = target_node.grid_cell;

    //         if current_cell != neighbour_cell {
    //             let s = vertex.id;

    //             let dijk_phast = Instant::now();
    //             // Swap all up and downs here
    //             let arc_flags_path_finding = ContractionHierarchies::new(
    //                 num_vertices,
    //                 &reverse_offset_array_up,
    //                 &reverse_edges_up,
    //             );
    //             // let mut arc_flags_path_finding =
    //             // Dijkstra::new(num_vertices, &reverse_offset_array_up, &reverse_edge_up);

    //             // println!("Dijkstra object creation: {:?}", dijk_phast.elapsed());

    //             let arc_phast = Instant::now();
    //             let (distances, predecessors, predecessor_edges) = phast::phast_by_edges(
    //                 arc_flags_path_finding,
    //                 s,
    //                 &red_by_level,
    //                 &reverse_offset_array_down,
    //                 &reverse_edges_down,
    //             );
    //             // println!("Arc Phast took: {:?}", arc_phast.elapsed());

    //             construct_spt(
    //                 s,
    //                 &vertices,
    //                 &distances,
    //                 &predecessors,
    //                 &predecessor_edges,
    //                 current_cell_id,
    //                 &mut arc_flags,
    //             )
    //         } else {
    //             // In the 0 0 case, we are always in here
    //             arc_flags[edge].set(current_cell_id, true);
    //         }
    //     }
    // }

    println!("Arcflag preproc took: {:?}", preproc.elapsed());

    let path = "Waibel.t21";
    let mut fmi_writer = BufWriter::new(File::create(path).unwrap());

    for i in &source_target_tuples {
        let arc_t = Instant::now();
        let (arc_dist, _, _) = arc_flags_query(
            i.0,
            i.1,
            &vertices,
            &edges,
            &offset_array,
            m_rows,
            n_columns,
            &arc_flags,
            num_vertices,
        );
        writeln!(
            fmi_writer,
            "{} {} {} {:?}",
            i.0,
            i.1,
            arc_dist,
            arc_t.elapsed()
        )
        .unwrap();
    }

    fmi_writer.flush().unwrap();

    let path = "Waibel.t20";
    let mut fmi_writer = BufWriter::new(File::create(path).unwrap());

    let arc_query = Instant::now();
    let (arc_dist, _, edge_search) = arc_flags_query(
        source_target_tuples[0].0,
        source_target_tuples[0].1,
        &vertices,
        &edges,
        &offset_array,
        m_rows,
        n_columns,
        &arc_flags,
        num_vertices,
    );

    let arc_t = arc_query.elapsed();
    println!("Arc flags query took: {:?}", arc_t);

    println!("Arc dist {}", arc_dist);

    // assert_eq!(436627, arc_dist);

    for edge in &edges {
        let bitstring = arc_flags[edge.id]
            .iter()
            .map(|bit| if *bit { '1' } else { '0' })
            .collect::<String>();

        writeln!(
            fmi_writer,
            "{} {} {}",
            edge.start_vertex, edge.end_vertex, bitstring
        )
        .unwrap();
    }

    fmi_writer.flush().unwrap();

    let path = "Waibel.t32";
    let mut fmi_writer = BufWriter::new(File::create(path).unwrap());

    writeln!(fmi_writer, "{}", vertices.len()).unwrap();
    writeln!(fmi_writer, "{}", edge_search.len()).unwrap();

    // search.push_str(format!("{} \n", vertices.len()).as_str());
    // search.push_str(format!("{} \n", edge_search_space.len()).as_str());

    for vertex in &vertices {
        // let v = vertices.get(vertex).unwrap();
        // search.push_str(format!("{} {}\n", vertex.lon, vertex.lat).as_str());
        writeln!(fmi_writer, "{} {}", vertex.lon, vertex.lat).unwrap();
    }

    for edge in edge_search {
        let e = edges.get(edge).unwrap();
        // search.push_str(format!("{} {} {} {}\n", e.start_vertex, e.end_vertex, 8, 1).as_str());
        writeln!(
            fmi_writer,
            "{} {} {} {}",
            e.start_vertex, e.end_vertex, 8, 1
        )
        .unwrap();
    }

    fmi_writer.flush().unwrap();
}

fn arc_flags_query(
    start_node: usize,
    target_node: usize,
    vertices: &Vec<Vertex>,
    edges: &Vec<Edge>,
    offset_array: &Vec<usize>,
    m_rows: usize,
    n_columns: usize,
    arc_flags: &Vec<BitVec>,
    num_vertices: usize,
) -> (usize, Vec<usize>, Vec<usize>) {
    let mut dist: Vec<usize> = (0..vertices.len())
        .map(|_| ((usize::MAX / 2) - 1))
        .collect();
    let mut pq: BinaryHeap<PQEntry> = BinaryHeap::new();
    let mut predecessors = (0..num_vertices).map(|_| usize::MAX).collect();

    let id = cell_to_id(
        vertices.get(target_node).unwrap().grid_cell,
        m_rows,
        n_columns,
    );

    dist[start_node] = 0;
    pq.push(PQEntry {
        distance: 0,
        vertex: start_node,
    });

    let mut search = Vec::new();

    while let Some(PQEntry { distance, vertex }) = pq.pop() {
        if vertex == target_node {
            return (distance, predecessors, search);
        };
        for j in offset_array[vertex]..offset_array[vertex + 1] {
            if let Some(bit) = arc_flags[j].get(id as usize) {
                if *bit.as_ref() == true {
                    let edge = edges.get(j).unwrap();
                    search.push(edge.id);
                    if dist[edge.end_vertex] > dist[vertex] + edge.weight {
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
    (usize::MAX, Vec::new(), Vec::new())
}
