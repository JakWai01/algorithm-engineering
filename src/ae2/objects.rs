use std::hash::Hash;

#[derive(Debug, Copy, Clone)]
pub struct Vertex {
    pub id: usize,
    pub osm_id: usize,
    pub lon: f64,
    pub lat: f64,
    pub height: usize,
    pub level: usize,
    pub grid_cell: (f64, f64),
}

impl Eq for Vertex {}

impl PartialEq for Vertex {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Hash for Vertex {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Edge {
    pub id: usize,
    pub start_vertex: usize,
    pub end_vertex: usize,
    pub weight: usize,
    pub typ: usize,
    pub max_speed: i64,
    pub edge_id_a: i64,
    pub edge_id_b: i64,
}
