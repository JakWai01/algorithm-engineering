use crate::{dijkstra, Edge};

pub fn unpack_path(
    current_min: usize,
    path_finding: dijkstra::Dijkstra,
    s: usize,
    t: usize,
    edges: &Vec<Edge>,
) -> Vec<usize> {
    let mut upwards_path_node_id = current_min;
    let mut upwards_path: Vec<usize> = Vec::new();
    while upwards_path_node_id != s {
        upwards_path.insert(0, upwards_path_node_id);
        upwards_path_node_id = path_finding.predecessors_up[upwards_path_node_id];
    }
    upwards_path.insert(0, s);

    let mut downwards_path_node_id = current_min;
    let mut downwards_path: Vec<usize> = Vec::new();
    while downwards_path_node_id != t {
        downwards_path.push(downwards_path_node_id);
        downwards_path_node_id = path_finding.predecessors_down[downwards_path_node_id];
    }
    downwards_path.push(t);

    let mut edge_path: Vec<usize> = Vec::new();

    for vertex in upwards_path {
        let edge = path_finding.predecessor_edges_up[vertex];
        if edge == usize::MAX {
            continue;
        }
        let e = edges.get(edge).unwrap();
        edge_path.push(e.id);
    }

    for vertex in downwards_path {
        let edge = path_finding.predecessor_edges_down[vertex];
        if edge == usize::MAX {
            continue;
        }
        let e = edges.get(edge).unwrap();
        edge_path.push(e.id);
    }

    // Unpack shortcuts
    let mut unpack_stack: Vec<usize> = Vec::new();
    let mut unpacked_path: Vec<usize> = Vec::new();

    for edge in edge_path {
        unpack_stack.push(edge);
        while !unpack_stack.is_empty() {
            let edge_id = unpack_stack.pop().unwrap();
            let edge = edges.get(edge_id).unwrap();
            if edge.edge_id_a != -1 && edge.edge_id_b != -1 {
                unpack_stack.push(edge.edge_id_b as usize);
                unpack_stack.push(edge.edge_id_a as usize);
            } else {
                unpacked_path.push(edge.id);
            }
        }
    }

    unpacked_path
}
