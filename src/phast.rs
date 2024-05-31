use std::time::Instant;

use crate::{
    dijkstra::Dijkstra,
    objects::{Edge, Vertex},
};

pub fn PHAST(
    phast_path_finding: &mut Dijkstra,
    s: usize,
    vertices: &Vec<Vertex>,
    vertices_by_level_desc: &Vec<Vertex>,
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

    // 2. Step: Consider all nodes u from high to low level and set d(u) = min{d(u), d(v) + c(v, u)}
    //          for nodes v with level(v) > level(u) and (v, u) ∈ E
    let phast_relaxation = Instant::now();
    // Consider all nodes in inverse level order
    for vertex in vertices_by_level_desc {
        // Look at downward edges only
        for incoming_edge_id in predecessor_offset[vertex.id]..predecessor_offset[vertex.id + 1] {
            let t0 = Instant::now();
            let incoming_edge = edges[incoming_edge_id];
            if distances[incoming_edge.start_vertex] == (usize::MAX / 2) - 1 {
                continue;
            }

            if vertices[incoming_edge.start_vertex].level > vertex.level {
                if distances[vertex.id]
                    >= distances[incoming_edge.start_vertex] + incoming_edge.weight
                {
                    distances[vertex.id] =
                        distances[incoming_edge.start_vertex] + incoming_edge.weight;
                    predecessors[vertex.id] = incoming_edge.start_vertex;
                    predecessor_edges[vertex.id] = incoming_edge_id;
                }
            }
            println!("QQ!@#: {:?}", t0.elapsed());
        }
    }

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

pub fn phast_new(
    phast_path_finding: &mut Dijkstra,
    s: usize,
    vertices: &Vec<Vertex>,
    vertices_by_level_desc: &Vec<Vertex>,
    edges_down_offset: &Vec<usize>,
    edges: &Vec<Edge>,
) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
    // 1. Step: Execute a Dijkstra on the up-graph of source node s
    // let up_graph_ch = Instant::now();
    let (mut distances, mut predecessors, mut predecessor_edges) =
        phast_path_finding.ch_query(s, &vertices);

    // println!("Distance to peek: {}", distances[183053]);

    // Haven't found the edge yet
    // println!(
    //     "Predecessor edges before 2nd step: {:?}",
    //     predecessor_edges[173169]
    // );

    // 2. Step: Consider all nodes u from high to low level and set d(u) = min{d(u), d(v) + c(v, u)}
    //          for nodes v with level(v) > level(u) and (v, u) ∈ E
    for vertex in vertices_by_level_desc {
        for neighbour in edges_down_offset[vertex.id]..edges_down_offset[vertex.id + 1] {
            let edge = edges.get(neighbour).unwrap();

            // Eigentlich sollte hier 32694
            // if vertex.id == 183053 {
            //     println!("Wrong edge: {:?}", edge);
            // }
            assert_eq!(vertex.id, edge.start_vertex);

            if distances[edge.end_vertex] >= distances[vertex.id] + edge.weight {
                distances[edge.end_vertex] = distances[vertex.id] + edge.weight;
                predecessors[edge.end_vertex] = vertex.id;
                if s == 754742 && vertex.id == 183053 && edge.end_vertex == 621003 {
                    println!(
                        "Setting predecessor edge for {}: {:?}, {}",
                        edge.end_vertex,
                        edges.get(neighbour).unwrap(),
                        neighbour,
                    );
                }
                predecessor_edges[edge.end_vertex] = edge.id;
                if s == 754742 && vertex.id == 183053 && edge.end_vertex == 621003 {
                    println!("Pred: {}", predecessor_edges[edge.end_vertex]);
                }
            }
        }
    }

    (
        distances.clone(),
        predecessors.clone(),
        predecessor_edges.clone(),
    )
}
