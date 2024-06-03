use crate::ae2::objects::Edge;
use crate::ae2::pq::PQEntry;
use std::{cmp::min, collections::BinaryHeap, f64::INFINITY};

pub struct BidirectionalContractionHierarchies<'a> {
    pub df: Vec<usize>,
    pub db: Vec<usize>,
    pub fq: BinaryHeap<PQEntry>,
    pub bq: BinaryHeap<PQEntry>,
    pub offset_array_up: &'a Vec<usize>,
    pub offset_array_down: &'a Vec<usize>,
    pub edges_up: &'a Vec<Edge>,
    pub edges_down: &'a Vec<Edge>,
    pub predecessors_up: Vec<usize>,
    pub predecessors_down: Vec<usize>,
    pub predecessor_edges_up: Vec<usize>,
    pub predecessor_edges_down: Vec<usize>,
    pub offset_array_up_predecessors: &'a Vec<usize>,
    pub offset_array_down_predecessors: &'a Vec<usize>,
}

impl<'a> BidirectionalContractionHierarchies<'a> {
    pub fn new(
        num_vertices: usize,
        offset_array_up: &'a Vec<usize>,
        offset_array_down: &'a Vec<usize>,
        edges_up: &'a Vec<Edge>,
        edges_down: &'a Vec<Edge>,
        offset_array_up_predecessors: &'a Vec<usize>,
        offset_array_down_predecessors: &'a Vec<usize>,
    ) -> Self {
        let df: Vec<usize> = (0..num_vertices).map(|_| (usize::MAX / 2) - 1).collect();
        let db: Vec<usize> = (0..num_vertices).map(|_| (usize::MAX / 2) - 1).collect();
        let fq: BinaryHeap<PQEntry> = BinaryHeap::new();
        let bq: BinaryHeap<PQEntry> = BinaryHeap::new();
        let predecessors_up: Vec<usize> = (0..num_vertices).map(|_| usize::MAX).collect();
        let predecessors_down: Vec<usize> = (0..num_vertices).map(|_| usize::MAX).collect();
        let predecessor_edges_up: Vec<usize> = (0..num_vertices).map(|_| usize::MAX).collect();
        let predecessor_edges_down: Vec<usize> = (0..num_vertices).map(|_| usize::MAX).collect();
        Self {
            df,
            db,
            fq,
            bq,
            offset_array_up,
            offset_array_down,
            edges_up,
            edges_down,
            predecessors_up,
            predecessors_down,
            predecessor_edges_up,
            predecessor_edges_down,
            offset_array_up_predecessors,
            offset_array_down_predecessors,
        }
    }

    pub fn bidirectional_ch_query(
        &mut self,
        s: usize,
        t: usize,
    ) -> (f64, usize, Vec<usize>, Vec<usize>) {
        self.df[s] = 0;
        self.db[t] = 0;

        self.fq.push(PQEntry {
            distance: 0,
            vertex: s,
        });
        self.bq.push(PQEntry {
            distance: 0,
            vertex: t,
        });

        let mut d = INFINITY;
        let mut current_min = usize::MAX;

        let mut forward = true;

        let mut pq;
        let mut offset_array;
        let mut dist;
        let mut not_dist;
        let mut edges;
        let mut predecessors;
        let mut predecessor_edges;
        let mut offset_array_predecessors;

        let mut vertices_search_space = Vec::new();
        let mut edge_search_space = Vec::new();

        while (!self.fq.is_empty() || !self.bq.is_empty())
            && d > min(
                self.fq
                    .peek()
                    .unwrap_or(&PQEntry {
                        distance: usize::MAX,
                        vertex: 0,
                    })
                    .distance,
                self.bq
                    .peek()
                    .unwrap_or(&PQEntry {
                        distance: usize::MAX,
                        vertex: 0,
                    })
                    .distance,
            ) as f64
        {
            forward = !forward;

            if forward {
                pq = &mut self.fq;
                offset_array = &mut self.offset_array_up;
                dist = &mut self.df;
                not_dist = &mut self.db;
                edges = &mut self.edges_up;
                predecessors = &mut self.predecessors_up;
                predecessor_edges = &mut self.predecessor_edges_up;
                offset_array_predecessors = &mut self.offset_array_up_predecessors
            } else {
                pq = &mut self.bq;
                offset_array = &mut self.offset_array_down;
                dist = &mut self.db;
                not_dist = &mut self.df;
                edges = &mut self.edges_down;
                predecessors = &mut self.predecessors_down;
                predecessor_edges = &mut self.predecessor_edges_down;
                offset_array_predecessors = &mut self.offset_array_down_predecessors
            }

            if let Some(PQEntry {
                distance,
                vertex: u,
            }) = pq.pop()
            {
                vertices_search_space.push(u);

                for e in offset_array_predecessors[u]..offset_array_predecessors[u + 1] {
                    let edge = edges.get(e).unwrap();
                    if dist[edge.start_vertex] + edge.weight <= distance {
                        continue;
                    }
                }

                if ((dist[u] + not_dist[u]) as f64) < d {
                    d = (dist[u] + not_dist[u]) as f64;
                    current_min = u;
                }

                for e in offset_array[u]..offset_array[u + 1] {
                    edge_search_space.push(e);
                    let edge = edges.get(e).unwrap();
                    let v = edge.end_vertex;
                    if dist[u] + edge.weight < dist[v] {
                        dist[v] = dist[u] + edge.weight;
                        pq.push(PQEntry {
                            distance: dist[v],
                            vertex: v,
                        });
                        predecessors[v] = u;
                        predecessor_edges[v] = edge.id;
                    }
                }
            }
        }
        (d, current_min, vertices_search_space, edge_search_space)
    }
}
