use std::collections::{BinaryHeap, HashMap};
use crate::ae2::objects::{Edge, Vertex};
use crate::ae2::pq::PQEntry;


pub struct Dijkstra<'a> {
    dist: Vec<usize>,
    pq: BinaryHeap<PQEntry>,
    vertices: &'a Vec<Vertex>,
    offset_array: &'a Vec<usize>,
    edges: &'a Vec<Edge>,
}

impl<'a> Dijkstra<'a> {
    pub fn new(
        vertices: &'a Vec<Vertex>,
        offset_array: &'a Vec<usize>,
        edges: &'a Vec<Edge>,
    ) -> Self {
        let dist: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let pq: BinaryHeap<PQEntry> = BinaryHeap::new();

        Dijkstra {
            dist,
            pq,
            vertices,
            offset_array,
            edges,
        }
    }

    pub fn query(&mut self, start_node: usize, target_node: usize) -> (usize, Vec<usize>) {
        let mut edge_search = Vec::new();

        self.dist = (0..self.vertices.len()).map(|_| usize::MAX).collect();
        self.pq.clear();

        self.dist[start_node] = 0;
        self.pq.push(PQEntry {
            distance: 0,
            vertex: start_node,
        });

        while let Some(PQEntry { distance, vertex }) = self.pq.pop() {
            if vertex == target_node {
                return (distance, edge_search);
            };

            for j in self.offset_array[vertex]..self.offset_array[vertex + 1] {
                edge_search.push(j);
                let edge = self.edges.get(j).unwrap();
                if self.dist[edge.end_vertex] > self.dist[vertex] + edge.weight {
                    self.dist[edge.end_vertex] = self.dist[vertex] + edge.weight;
                    self.pq.push(PQEntry {
                        distance: self.dist[vertex] + edge.weight,
                        vertex: edge.end_vertex,
                    });
                    // predecessors[edge.end_vertex] = vertex;
                }
            }
        }
        (usize::MAX, Vec::new())
    }
}
