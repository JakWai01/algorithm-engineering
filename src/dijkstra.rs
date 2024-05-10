use std::{cmp::min, collections::BinaryHeap, f64::INFINITY, thread::current};

use crate::{pq::PQEntry, Edge, Vertex};

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
    sb: Vec<usize>,
}

impl<'a> Dijkstra<'a> {
    pub fn new(
        vertices: &'a Vec<Vertex>,
        offset_array_up: &'a Vec<usize>,
        offset_array_down: &'a Vec<usize>,
        edges_up: &'a Vec<Edge>,
        edges_down: &'a Vec<Edge>,
    ) -> Self {
        let df: Vec<usize> = (0..vertices.len()).map(|_| (usize::MAX / 2) - 1).collect();
        let db: Vec<usize> = (0..vertices.len()).map(|_| (usize::MAX / 2) - 1).collect();
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
            sb,
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
        let mut d = INFINITY;
        let mut current_min = usize::MAX;

        let mut forward = true;

        let mut pq = &mut self.fq;
        let mut offset_array = &mut self.offset_array_up;
        let mut dist = &mut self.df;
        let mut not_dist = &mut self.db;
        let mut edges = &mut self.edges_up;

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
            } else {
                pq = &mut self.bq;
                offset_array = &mut self.offset_array_down;
                dist = &mut self.db;
                not_dist = &mut self.df;
                edges = &mut self.edges_down;
            }

            if let Some(PQEntry {
                distance: _,
                vertex: u,
            }) = pq.pop()
            {
                if ((dist[u] + not_dist[u]) as f64) < d {
                    d = (dist[u] + not_dist[u]) as f64;
                }

                for e in offset_array[u]..offset_array[u + 1] {
                    let edge = edges.get(e).unwrap();
                    let v = edge.end_vertex;
                    if dist[u] + edge.weight < dist[v] {
                        dist[v] = dist[u] + edge.weight;
                        pq.push(PQEntry {
                            distance: dist[v],
                            vertex: v,
                        })
                    }
                }

                // if let Some(PQEntry {
                //     distance: _,
                //     vertex: v,
                // }) = self.bq.pop()
                // {
                //     self.sf.push(u);
                //     self.sb.push(v);

                //     for edge_id in self.offset_array_up[u]..self.offset_array_up[u + 1] {
                //         let edge = self.edges_up.get(edge_id).unwrap();
                //         let x = edge.end_vertex;
                //         let weight = edge.weight;

                //         if self.vertices.get(x).unwrap().level > self.vertices.get(u).unwrap().level
                //         {
                //             if !self.sf.contains(&x) && self.df[x] > self.df[u] + weight {
                //                 self.df[x] = self.df[u] + weight;
                //                 self.fq.push(PQEntry {
                //                     distance: self.df[x],
                //                     vertex: x,
                //                 });

                //                 self.predecessors_up[x] = u;
                //             }

                //             // check for a path s --- u - x --- t, and update mu
                //             if self.sb.contains(&x)
                //                 && ((self.df[u] + weight + self.db[x]) as f64) < mu
                //             {
                //                 mu = (self.df[u] + weight + self.db[x]) as f64;
                //                 current_min = x;
                //                 if u == 1104076 {
                //                     println!(
                //                         "Updated current minimum path with neighbour: {:?}",
                //                         x
                //                     );
                //                 }
                //             }
                //         }
                //     }

                //     for edge_id in self.offset_array_down[v]..self.offset_array_down[v + 1] {
                //         let edge = self.edges_down.get(edge_id).unwrap();
                //         let x = edge.end_vertex;
                //         let weight = edge.weight;

                //         if self.vertices.get(x).unwrap().level > self.vertices.get(v).unwrap().level
                //         {
                //             // relax v-x
                //             if !self.sb.contains(&x) && self.db[x] > self.db[v] + weight {
                //                 self.db[x] = self.db[v] + weight;
                //                 self.bq.push(PQEntry {
                //                     distance: self.db[x],
                //                     vertex: x,
                //                 });
                //                 self.predecessors_down[x] = v;
                //             }

                //             // check for a path t --- v - x --- s, and update mu
                //             if self.sf.contains(&x)
                //                 && ((self.db[v] + weight + self.df[x]) as f64) < mu
                //             {
                //                 mu = (self.db[v] + weight + self.df[x]) as f64;
                //                 current_min = x;
                //                 if v == 16825 {
                //                     println!(
                //                         "Updated current minimum path with neighbour: {:?}",
                //                         x
                //                     );
                //                 }
                //             }
                //         }
                //     }

                //     // check the termination condition
                //     // Das funktioniert weil wir dann schon alle kürzeren Pfade (nehmen ja immer den niedrigsten zuerst) und d.h.
                //     // der pfad den wir jetzt anschauen ist größer als der bisher kürzeste s-t Pfad obwohl wir alle Pfade beleuchtet haben
                //     // und es keinen Pfad mehr geben kann, der kürzer ist. Aber d.h. irgendwas stimmt nicht, da der kürzeste Pfad noch in der Queue sein muss
                //     if (self.df[u] + self.db[v]) as f64 >= mu {
                //         println!("Terminating with u {:?} with distance {} and v {:?} with distance {}. 183053 is currently settled with {}", u, self.df[u], v, self.db[v], self.df[183053]);
                //         println!(
                //             "Exiting since {} + {} = {} and mu was {}",
                //             self.df[u],
                //             self.db[v],
                //             self.df[u] + self.db[v],
                //             mu
                //         );
                //         println!("Backwards queue: {:?}", self.bq);
                //         return (
                //             mu,
                //             current_min,
                //             self.predecessors_up.clone(),
                //             self.predecessors_down.clone(),
                //         );
                //     }
                // }
            }
        }
        d
    }
}
