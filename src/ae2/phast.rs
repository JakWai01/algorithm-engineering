use std::time::Instant;

use crate::ae2::ch::ContractionHierarchies;
use crate::ae2::objects::{Edge, Vertex};

pub fn phast<'a>(
    mut phast_path_finding: ContractionHierarchies,
    s: usize,
    vertices_by_level_desc: &'a Vec<Vertex>,
    edges_down_offset: &'a Vec<usize>,
    edges: &'a Vec<Edge>,
) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
    // 1. Step: Execute a Dijkstra on the up-graph of source node s
    // let up_graph_ch = Instant::now();
    // let (mut distances, mut predecessors, mut predecessor_edges) =
    // phast_path_finding.ch_query(s, &vertices);
    let (mut distances, mut predecessors, mut predecessor_edges) = phast_path_finding.ch_query(s);

    // 2. Step: Consider all nodes u from high to low level and set d(u) = min{d(u), d(v) + c(v, u)}
    //          for nodes v with level(v) > level(u) and (v, u) ∈ E
    for vertex in vertices_by_level_desc {
        for neighbour in edges_down_offset[vertex.id]..edges_down_offset[vertex.id + 1] {
            let edge = edges.get(neighbour).unwrap();

            // if distances[edge.end_vertex] == ((usize::MAX / 2) - 1) {
            //     continue;
            // }

            if distances[edge.end_vertex] >= distances[vertex.id] + edge.weight {
                distances[edge.end_vertex] = distances[vertex.id] + edge.weight;
                predecessors[edge.end_vertex] = vertex.id;
                predecessor_edges[edge.end_vertex] = edge.id;
            }
        }
    }

    (distances, predecessors, predecessor_edges)
}

pub fn phast_by_edges<'a>(
    mut phast_path_finding: ContractionHierarchies,
    s: usize,
    edges_down_level_desc: &'a Vec<Edge>,
    edges_down_offset: &'a Vec<usize>,
    edges: &'a Vec<Edge>,
) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
    // 1. Step: Execute a Dijkstra on the up-graph of source node s
    let up_graph_ch = Instant::now();
    // let (mut distances, mut predecessors, mut predecessor_edges) =
    // phast_path_finding.ch_query(s, &vertices);
    let (mut distances, mut predecessors, mut predecessor_edges) = phast_path_finding.ch_query(s);
    // println!("CH took: {:?}", up_graph_ch.elapsed());
    // 2. Step: Consider all nodes u from high to low level and set d(u) = min{d(u), d(v) + c(v, u)}
    //          for nodes v with level(v) > level(u) and (v, u) ∈ E

    let down_sweep = Instant::now();
    for edge in edges_down_level_desc {
        // if distances[edge.end_vertex] == ((usize::MAX / 2) - 1) {
        //     continue;
        // }

        if distances[edge.end_vertex] >= distances[edge.start_vertex] + edge.weight {
            distances[edge.end_vertex] = distances[edge.start_vertex] + edge.weight;
            predecessors[edge.end_vertex] = edge.start_vertex;
            predecessor_edges[edge.end_vertex] = edge.id;
        }
    }
    // println!("Down sweep took: {:?}", down_sweep.elapsed());

    (distances, predecessors, predecessor_edges)
}
