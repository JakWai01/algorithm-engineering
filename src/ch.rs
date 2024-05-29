use std::collections::BinaryHeap;

use crate::{
    objects::{Edge, Vertex},
    pq::PQEntry,
};

pub struct ContractionHierarchies<'a> {
    df: Vec<usize>,
    fq: BinaryHeap<PQEntry>,
    num_vertices: usize,
    offset_array_up: &'a Vec<usize>,
    offset_array_up_predecessors: &'a Vec<usize>,
    edges_up: &'a Vec<Edge>,
    predecessors_up: Vec<usize>,
    predecessor_edges_up: Vec<usize>,
}

impl<'a> ContractionHierarchies<'a> {
    pub fn ch_query(
        &mut self,
        start_node: usize,
        vertices: &Vec<Vertex>,
    ) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
        self.df = (0..self.num_vertices)
            .map(|_| (usize::MAX / 2) - 1)
            .collect();
        self.fq.clear();
        self.df[start_node] = 0;
        self.fq.push(PQEntry {
            distance: 0,
            vertex: start_node,
        });

        while let Some(PQEntry { distance, vertex }) = self.fq.pop() {
            for e in self.offset_array_up_predecessors[vertex]
                ..self.offset_array_up_predecessors[vertex + 1]
            {
                let edge = self.edges_up.get(e).unwrap();
                if self.df[edge.start_vertex] + edge.weight <= distance {
                    continue;
                }
            }

            for j in self.offset_array_up[vertex]..self.offset_array_up[vertex + 1] {
                let edge = self.edges_up.get(j).unwrap();
                if vertices.get(edge.end_vertex).unwrap().level
                    > vertices.get(edge.start_vertex).unwrap().level
                {
                    if self.df[edge.end_vertex] > self.df[vertex] + edge.weight {
                        self.df[edge.end_vertex] = self.df[vertex] + edge.weight;
                        self.fq.push(PQEntry {
                            distance: self.df[vertex] + edge.weight,
                            vertex: edge.end_vertex,
                        });

                        if start_node == 163217 {
                            println!("Vertex: {}", vertex);
                            println!("Edge: {:?}", edge);
                            println!(
                                "Setting {} as the predecessor of {}",
                                edge.start_vertex, edge.end_vertex
                            );
                        }

                        self.predecessors_up[edge.end_vertex] = edge.start_vertex;
                        self.predecessor_edges_up[edge.end_vertex] = edge.id;
                    }
                }
            }
        }
        (
            self.df.clone(),
            self.predecessors_up.clone(),
            self.predecessor_edges_up.clone(),
        )
    }
}
