use crate::{
    dijkstra::ContractionHierarchies,
    objects::{Edge, Vertex},
};

pub fn phast(
    phast_path_finding: &mut ContractionHierarchies,
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

    // 2. Step: Consider all nodes u from high to low level and set d(u) = min{d(u), d(v) + c(v, u)}
    //          for nodes v with level(v) > level(u) and (v, u) ∈ E
    for vertex in vertices_by_level_desc {
        for neighbour in edges_down_offset[vertex.id]..edges_down_offset[vertex.id + 1] {
            let edge = edges.get(neighbour).unwrap();

            if distances[edge.end_vertex] >= distances[vertex.id] + edge.weight {
                distances[edge.end_vertex] = distances[vertex.id] + edge.weight;
                predecessors[edge.end_vertex] = vertex.id;
                predecessor_edges[edge.end_vertex] = edge.id;
            }
        }
    }

    (distances, predecessors, predecessor_edges)
}
