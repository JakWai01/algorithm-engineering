use std::collections::BinaryHeap;

use crate::{Edge, Vertex, pq::PQEntry};

pub(crate) struct Dijkstra<'a> {
    dist_up: Vec<usize>,
    dist_down: Vec<usize>,
    pq_up: BinaryHeap<PQEntry>,
    pq_down: BinaryHeap<PQEntry>,
    vertices: &'a Vec<Vertex>,
    offset_array_up: &'a Vec<usize>,
    offset_array_down: &'a Vec<usize>,
    edges_up: &'a Vec<Edge>,
    edges_down: &'a Vec<Edge>,
    predecessors_up: Vec<usize>,
    predecessors_down: Vec<usize>,
    visited_up: Vec<bool>,
    visited_down: Vec<bool>
}

impl<'a> Dijkstra<'a> {
    pub fn new(
        vertices: &'a Vec<Vertex>,
        offset_array_up: &'a Vec<usize>,
        offset_array_down: &'a Vec<usize>,
        edges_up: &'a Vec<Edge>,
        edges_down: &'a Vec<Edge>,
    ) -> Self {
        let dist_up: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let dist_down: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let pq_up: BinaryHeap<PQEntry> = BinaryHeap::new();
        let pq_down: BinaryHeap<PQEntry> = BinaryHeap::new();
        let predecessors_up: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let predecessors_down: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let mut visited_up: Vec<bool> = (0..vertices.len()).map(|_| false).collect();
        let mut visited_down: Vec<bool> = (0..vertices.len()).map(|_| false).collect(); 

        Dijkstra {
            dist_up,
            dist_down,
            pq_up,
            pq_down,
            vertices,
            offset_array_up,
            offset_array_down,
            edges_up,
            edges_down,
            predecessors_up,
            predecessors_down,
            visited_up,
            visited_down
        }
    }
        
    fn clear_data_structures(&mut self) {
        self.dist_up = (0..self.vertices.len()).map(|_| usize::MAX).collect();
        self.dist_down = (0..self.vertices.len()).map(|_| usize::MAX).collect();
        self.pq_up.clear();
        self.pq_down.clear();
    }

    fn relax_up(&mut self, vertex: usize, edge: &Edge) {
        if self.dist_up[edge.end_vertex] > self.dist_up[vertex] + edge.weight {
            self.dist_up[edge.end_vertex] = self.dist_up[vertex] + edge.weight;
            self.pq_up.push(PQEntry {
                distance: self.dist_up[vertex] + edge.weight,
                vertex: edge.end_vertex,
            });
            self.predecessors_up[edge.end_vertex] = vertex;
        }
    }

    fn relax_down(&mut self, vertex: usize, edge: &Edge) {
        if self.dist_down[edge.end_vertex] > self.dist_down[vertex] + edge.weight {
            self.dist_down[edge.end_vertex] = self.dist_down[vertex] + edge.weight;
            self.pq_down.push(PQEntry {
                distance: self.dist_down[vertex] + edge.weight,
                vertex: edge.end_vertex,
            });
            self.predecessors_down[edge.end_vertex] = vertex;
        }
    }

    pub fn ch_query(&mut self, start_node: usize, target_node: usize) -> (Vec<usize>, Vec<usize>, usize, usize, Vec<usize>, Vec<usize>) {
            self.clear_data_structures();

            self.dist_up[start_node] = 0;
            self.dist_down[target_node] = 0;

            self.pq_up.push(PQEntry {
                distance: 0,
                vertex: start_node,
            });
            self.pq_down.push(PQEntry {
                distance: 0,
                vertex: target_node,
            });

            while !self.pq_up.is_empty() && !self.pq_down.is_empty() {
                if let Some(PQEntry { distance: _, vertex: vertex_up }) = self.pq_up.pop() {
                    if let Some(PQEntry { distance: _, vertex: vertex_down }) = self.pq_down.pop() {

                        if self.visited_down[vertex_up] == true {
                            return (self.predecessors_up.clone(), self.predecessors_down.clone(), vertex_up, self.dist_up[vertex_up] + self.dist_down[vertex_up], self.dist_up.clone(), self.dist_down.clone())
                        }

                        if self.visited_up[vertex_down] == true {
                            return (self.predecessors_up.clone(), self.predecessors_down.clone(), vertex_down, self.dist_up[vertex_down] + self.dist_down[vertex_down], self.dist_up.clone(), self.dist_down.clone())
                        }

                        // Upwards search step
                        for edge_id in self.offset_array_up[vertex_up]..self.offset_array_up[vertex_up + 1] {
                            let edge = self.edges_up.get(edge_id).unwrap();
                            if self.vertices.get(edge.end_vertex).unwrap().level > self.vertices.get(edge.start_vertex).unwrap().level {
                                self.relax_up(vertex_up, edge);
                                self.visited_up[edge.end_vertex] = true;
                            }
                        }
                        
                        // Downwards search step
                        for neighbour in self.offset_array_down[vertex_down]..self.offset_array_down[vertex_down + 1] {
                            let edge = self.edges_down.get(neighbour).unwrap();
                            if self.vertices.get(edge.end_vertex).unwrap().level > self.vertices.get(edge.start_vertex).unwrap().level {
                                self.relax_down(vertex_down, edge);
                                self.visited_down[edge.end_vertex] = true;
                            }
                        }
                    }
                }
            }
            (Vec::new(), Vec::new(), usize::max_value(), usize::max_value(), Vec::new(), Vec::new())
        }
}