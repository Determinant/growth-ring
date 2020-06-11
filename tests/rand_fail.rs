#[cfg(test)]
mod common;

fn single_point_failure(sim: &common::PaintingSim) {
    let nticks = sim.get_nticks();
    println!("nticks = {}", nticks);
    for i in 0..nticks {
        print!("fail pos = {}, ", i);
        assert!(sim.test(common::SingleFailGen::new(i)));
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
        k: 100,
        csize: 1000,
        stroke_max_len: 10,
        stroke_max_col: 256,
        stroke_max_n: 5,
        seed: 0,
    };
    single_point_failure(&sim);
}
