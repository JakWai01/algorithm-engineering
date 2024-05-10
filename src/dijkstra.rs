use std::{collections::BinaryHeap, f64::INFINITY};

use crate::{Edge, Vertex, pq::PQEntry};

pub(crate) struct Dijkstra<'a> {
    df: Vec<usize>,
    db: Vec<usize>,
    fq: BinaryHeap<PQEntry>,
    bq: BinaryHeap<PQEntry>,
    vertices: &'a Vec<Vertex>,
    offset_array_up: &'a Vec<usize>,
    offset_array_down: &'a Vec<usize>,
    edges_up: &'a Vec<Edge>,
    edges_down: &'a Vec<Edge>,
    predecessors_up: Vec<usize>,
    predecessors_down: Vec<usize>,
    sf: Vec<usize>,
    sb: Vec<usize>
}

impl<'a> Dijkstra<'a> {
    pub fn new(
        vertices: &'a Vec<Vertex>,
        offset_array_up: &'a Vec<usize>,
        offset_array_down: &'a Vec<usize>,
        edges_up: &'a Vec<Edge>,
        edges_down: &'a Vec<Edge>,
    ) -> Self {
        let df: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let db: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let fq: BinaryHeap<PQEntry> = BinaryHeap::new();
        let bq: BinaryHeap<PQEntry> = BinaryHeap::new();
        let predecessors_up: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let predecessors_down: Vec<usize> = (0..vertices.len()).map(|_| usize::MAX).collect();
        let sf: Vec<usize> = Vec::new();
        let sb: Vec<usize> = Vec::new(); 

        Dijkstra {
            df,
            db,
            fq,
            bq,
            vertices,
            offset_array_up,
            offset_array_down,
            edges_up,
            edges_down,
            predecessors_up,
            predecessors_down,
            sf,
            sb
        }
    }

    pub fn ch_query(&mut self, s: usize, t: usize) -> f64 {
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
        let mut mu = INFINITY;

        while !self.fq.is_empty() && !self.bq.is_empty() {
            if let Some(PQEntry { distance: _, vertex: u }) = self.fq.pop() {
                if let Some(PQEntry { distance: _, vertex: v }) = self.bq.pop() {
                    // self.sf.insert(u, true);
                    self.sf.push(u);
                    // self.sb.insert(v, true);
                    self.sb.push(v);

                    for edge_id in self.offset_array_up[u]..self.offset_array_up[u + 1] {

                        let edge = self.edges_up.get(edge_id).unwrap();
                        let x = edge.end_vertex;
                        let weight = edge.weight;

                        if self.vertices.get(x).unwrap().level > self.vertices.get(u).unwrap().level {

                            // relax u-x
                            println!("U: Currently looking at this distance {} for this node {:?} with weight {}", self.df[u], self.vertices.get(x), weight);
                            if !self.sf.contains(&x) && self.df[x] > self.df[u] + weight {
                                self.df[x] = self.df[u] + weight;
                                self.fq.push(PQEntry {
                                    distance: self.df[x],
                                    vertex: x,
                                });
                                self.predecessors_up[x] = u;
                            }

                            // check for a path s --- u - x --- t, and update mu
                            if self.sb.contains(&x) && ((self.df[u] + weight + self.db[x]) as f64) < mu {
                                mu = (self.df[u] + weight + self.db[x]) as f64
                            }
                        }
                    }
                    
                    for edge_id in self.offset_array_down[v]..self.offset_array_down[v + 1] {

                        let edge = self.edges_down.get(edge_id).unwrap();
                        let x = edge.end_vertex;
                        let weight = edge.weight;

                        if self.vertices.get(x).unwrap().level > self.vertices.get(v).unwrap().level {

                            // relax v-x
                            println!("D: Currently looking at this distance {} for this node {:?} with weight {}", self.db[v], self.vertices.get(x), weight);
                            if !self.sb.contains(&x) && self.db[x] > self.db[v] + weight {
                                self.db[x] = self.db[v] + weight;
                                self.bq.push(PQEntry {
                                    distance: self.db[x],
                                    vertex: x,
                                });
                                self.predecessors_down[x] = v;
                            }
                            
                            // check for a path t --- v - x --- s, and update mu
                            if self.sf.contains(&x) && ((self.db[v] + weight + self.df[x]) as f64) < mu {
                                mu = (self.db[v] + weight + self.df[x]) as f64
                            }
                        }
                    }

                    // check the termination condition
                    if (self.df[u] + self.db[v]) as f64 >= mu {
                        return mu;
                    }
                }
            }
        }
        f64::INFINITY
    }
}