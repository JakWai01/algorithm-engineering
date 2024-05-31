use std::collections::BinaryHeap;

use crate::{pq::PQEntry, Edge, Vertex};

pub(crate) struct ContractionHierarchies<'a> {
    df: Vec<usize>,
    fq: BinaryHeap<PQEntry>,
    offset_array_up: &'a Vec<usize>,
    edges_up: &'a Vec<Edge>,
    pub predecessors_up: Vec<usize>,
    pub predecessor_edges_up: Vec<usize>,
}

impl<'a> ContractionHierarchies<'a> {
    pub fn new(
        num_vertices: usize,
        offset_array_up: &'a Vec<usize>,
        edges_up: &'a Vec<Edge>,
    ) -> Self {
        let df: Vec<usize> = (0..num_vertices).map(|_| (usize::MAX / 2) - 1).collect();
        let fq: BinaryHeap<PQEntry> = BinaryHeap::new();
        let predecessors_up: Vec<usize> = (0..num_vertices).map(|_| usize::MAX).collect();
        let predecessor_edges_up: Vec<usize> = (0..num_vertices).map(|_| usize::MAX).collect();

        ContractionHierarchies {
            df,
            fq,
            offset_array_up,
            edges_up,
            predecessors_up,
            predecessor_edges_up,
        }
    }

    pub fn ch_query(
        &mut self,
        start_node: usize,
        vertices: &Vec<Vertex>,
    ) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
        self.df[start_node] = 0;
        self.fq.push(PQEntry {
            distance: 0,
            vertex: start_node,
        });

        while let Some(PQEntry {
            distance: _,
            vertex,
        }) = self.fq.pop()
        {
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
