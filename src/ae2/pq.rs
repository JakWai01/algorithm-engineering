use std::cmp::Ordering;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct PQEntry {
    pub distance: usize,
    pub vertex: usize,
}

impl Ord for PQEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .cmp(&self.distance)
            .then_with(|| self.vertex.cmp(&other.vertex))
    }
}

impl PartialOrd for PQEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
