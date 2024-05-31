use std::collections::BinaryHeap;

use crate::{
    objects::{Edge, Vertex},
    pq::PQEntry,
};

// pub struct Dijkstra<'a> {
//     dist: Vec<usize>,
//     pq: BinaryHeap<PQEntry>,
//     num_vertices: usize,
//     offset_array: &'a Vec<usize>,
//     edges: &'a Vec<Edge>,
//     predecessors: Vec<usize>,
//     predecessor_edges: Vec<usize>,
// }

// impl<'a> Dijkstra<'a> {
//     pub fn new(num_vertices: usize, offset_array: &'a Vec<usize>, edges: &'a Vec<Edge>) -> Self {
//         let dist: Vec<usize> = (0..num_vertices).map(|_| usize::MAX).collect();
//         let pq: BinaryHeap<PQEntry> = BinaryHeap::new();
//         let predecessors: Vec<usize> = (0..num_vertices).map(|_| usize::MAX).collect();
//         let predecessor_edges: Vec<usize> = (0..num_vertices).map(|_| usize::MAX).collect();

//         Dijkstra {
//             dist,
//             pq,
//             num_vertices,
//             offset_array,
//             edges,
//             predecessors,
//             predecessor_edges,
//         }
//     }

//     pub fn query(&mut self, start_node: usize) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
//         // self.dist = (0..self.num_vertices).map(|_| usize::MAX).collect();
//         // self.pq.clear();

//         self.dist[start_node] = 0;
//         self.pq.push(pqentry {
//             distance: 0,
//             vertex: start_node,
//         });

//         while let some(pqentry {
//             distance: _,
//             vertex,
//         }) = self.pq.pop()
//         {
//             for j in self.offset_array[vertex]..self.offset_array[vertex + 1] {
//                 let edge = self.edges.get(j).unwrap();

//                 if self.dist[edge.end_vertex] > self.dist[vertex] + edge.weight {
//                     self.dist[edge.end_vertex] = self.dist[vertex] + edge.weight;

//                     self.pq.push(pqentry {
//                         distance: self.dist[vertex] + edge.weight,
//                         vertex: edge.end_vertex,
//                     });
//                     self.predecessors[edge.end_vertex] = vertex;
//                     self.predecessor_edges[edge.end_vertex] = edge.id
//                 }
//             }
//         }
//         (
//             self.dist.clone(),
//             self.predecessors.clone(),
//             self.predecessor_edges.clone(),
//         )
//     }
// }

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
