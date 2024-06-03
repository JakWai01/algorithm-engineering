use std::collections::BinaryHeap;
use crate::ae2::objects::Edge;
use crate::ae2::pq::PQEntry;


pub fn dijkstra<'a>(
    distances: &'a mut Vec<usize>,
    predecessors: &'a mut Vec<usize>,
    predecessor_edges: &'a mut Vec<usize>,
    start_node: usize,
    offset_array: &Vec<usize>,
    edges: &Vec<Edge>,
) -> (&'a mut Vec<usize>, &'a mut Vec<usize>, &'a mut Vec<usize>) {
    distances[start_node] = 0;
    let mut pq = BinaryHeap::new();
    pq.push(PQEntry {
        distance: 0,
        vertex: start_node,
    });

    while let Some(PQEntry {
        distance: _,
        vertex,
    }) = pq.pop()
    {
        for j in offset_array[vertex]..offset_array[vertex + 1] {
            let edge = edges.get(j).unwrap();

            if distances[edge.end_vertex] > distances[vertex] + edge.weight {
                distances[edge.end_vertex] = distances[vertex] + edge.weight;

                pq.push(PQEntry {
                    distance: distances[vertex] + edge.weight,
                    vertex: edge.end_vertex,
                });
                predecessors[edge.end_vertex] = vertex;
                predecessor_edges[edge.end_vertex] = edge.id
            }
        }
    }
    (distances, predecessors, predecessor_edges)
}
