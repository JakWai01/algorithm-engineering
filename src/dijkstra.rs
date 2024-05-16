use std::{cmp::min, collections::BinaryHeap, f64::INFINITY, thread::current};

use crate::{pq::PQEntry, Edge, Vertex};

pub(crate) struct Dijkstra<'a> {
    df: Vec<usize>,
    db: Vec<usize>,
    fq: BinaryHeap<PQEntry>,
    bq: BinaryHeap<PQEntry>,
    num_vertices: usize,
    offset_array_up: &'a Vec<usize>,
    offset_array_down: &'a Vec<usize>,
    edges_up: &'a Vec<Edge>,
    edges_down: &'a Vec<Edge>,
    pub predecessors_up: Vec<usize>,
    pub predecessors_down: Vec<usize>,
    pub predecessor_edges_up: Vec<usize>,
    pub predecessor_edges_down: Vec<usize>,
    offset_array_up_predecessors: &'a Vec<usize>,
    offset_array_down_predecessors: &'a Vec<usize>,
}

impl<'a> Dijkstra<'a> {
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

        Dijkstra {
            df,
            db,
            fq,
            bq,
            num_vertices,
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

    fn reset(&mut self) {
        self.df = (0..self.num_vertices)
            .map(|_| (usize::MAX / 2) - 1)
            .collect();
        self.db = (0..self.num_vertices)
            .map(|_| (usize::MAX / 2) - 1)
            .collect();
        self.fq.clear();
        self.bq.clear();
        self.predecessors_up = (0..self.num_vertices).map(|_| usize::MAX).collect();
        self.predecessors_down = (0..self.num_vertices).map(|_| usize::MAX).collect();
        self.predecessor_edges_up = (0..self.num_vertices).map(|_| usize::MAX).collect();
        self.predecessor_edges_down = (0..self.num_vertices).map(|_| usize::MAX).collect();
    }
    // Based on https://scholar.archive.org/work/kxils2sde5dwpbbqhddzyltabq/access/wayback/https://publikationen.bibliothek.kit.edu/1000028701/142973925
    pub fn bidirectional_ch_query(&mut self, s: usize, t: usize) -> (f64, usize) {
        // TODO: This takes way too long
        // self.reset();

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

        // length of the bet path yet seen
        let mut d = INFINITY;
        let mut current_min = usize::MAX;

        let mut forward = true;

        let mut pq = &mut self.fq;
        let mut offset_array = &mut self.offset_array_up;
        let mut dist = &mut self.df;
        let mut not_dist = &mut self.db;
        let mut edges = &mut self.edges_up;
        let mut predecessors = &mut self.predecessors_up;
        let mut predecessor_edges = &mut self.predecessor_edges_up;
        let mut offset_array_predecessors = &mut self.offset_array_up_predecessors;

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
                // Only store predecessors with higher level for further optimization
                // TODO: At the moment, this is slower than without sod
                // Germany sod: 1822
                // Germany without sod: 1042
                for e in offset_array_predecessors[u]..offset_array_predecessors[u + 1] {
                    // println!("Found predecessor: {}", e);
                    let edge = edges.get(e).unwrap();
                    if dist[edge.start_vertex] + edge.weight <= distance {
                        // println!("Found a better path!");
                        continue;
                    }
                }

                if ((dist[u] + not_dist[u]) as f64) < d {
                    d = (dist[u] + not_dist[u]) as f64;
                    current_min = u;
                }

                for e in offset_array[u]..offset_array[u + 1] {
                    let edge = edges.get(e).unwrap();
                    let v = edge.end_vertex;
                    if dist[u] + edge.weight < dist[v] {
                        dist[v] = dist[u] + edge.weight;
                        pq.push(PQEntry {
                            distance: dist[v],
                            vertex: v,
                        });
                        predecessors[v] = u;
                        // We store the predecessor_edge for the corresponding vertex
                        predecessor_edges[v] = edge.id;
                    }
                }
            }
        }
        (d, current_min)
    }

    pub fn ch_query(&mut self, start_node: usize, vertices: &Vec<Vertex>) -> Vec<usize> {
        self.df = (0..self.num_vertices).map(|_| usize::MAX).collect();
        self.fq.clear();

        self.df[start_node] = 0;
        self.fq.push(PQEntry {
            distance: 0,
            vertex: start_node,
        });

        while let Some(PQEntry { distance, vertex }) = self.fq.pop() {
            // with 27s
            for e in self.offset_array_up_predecessors[vertex]
                ..self.offset_array_up_predecessors[vertex + 1]
            {
                // println!("Found predecessor: {}", e);
                let edge = self.edges_up.get(e).unwrap();
                if self.df[edge.start_vertex] + edge.weight <= distance {
                    // println!("Found a better path!");
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
                        // Store offset index inside of predecessor array
                        // This should work in O(n)
                        self.predecessors_up[edge.start_vertex] = j;
                    }
                }
            }
        }
        self.df.clone()
    }
}
