#[cfg(test)] mod common;

use std::collections::HashMap;
use std::rc::Rc;

fn single_point_failure(sim: &common::PaintingSim) {
    let nticks = sim.get_nticks();
    println!("nticks = {}", nticks);
    for i in 0..nticks {
        print!("fail pos = {}, ", i);
        let mut state = common::WALStoreEmulState::new();
        let mut canvas = sim.new_canvas();
        let mut ops: Vec<common::PaintStrokes> = Vec::new();
        let mut ringid_map = HashMap::new();
        let fgen = common::SingleFailGen::new(i);
        if sim
            .run(
                &mut state,
                &mut canvas,
                sim.get_walloader(),
                &mut ops,
                &mut ringid_map,
                Rc::new(fgen),
            )
            .is_err()
        {
            assert!(sim.check(
                &mut state,
                &mut canvas,
                sim.get_walloader(),
                &ops,
                &ringid_map,
            ))
        }
    }
}

#[test]
fn test_rand_fail() {
    let sim = common::PaintingSim {
        block_nbit: 8,
        file_nbit: 9,
        file_cache: 1000,
        n: 100,
        m: 10,
        k: 1000,
        csize: 1000,
        stroke_max_len: 10,
        stroke_max_col: 256,
        stroke_max_n: 5,
        seed: 0,
    };
    single_point_failure(&sim);
}
