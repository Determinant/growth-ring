#[cfg(test)]

mod common;
use growthring::wal::{WALLoader, WALWriter, WALStore, WALRingId, WALBytes, WALPos};
use common::{FailGen, SingleFailGen, Canvas, WALStoreEmulState, WALStoreEmul, PaintStrokes};
use std::collections::HashMap;

fn run<G: 'static + FailGen, R: rand::Rng>(n: usize, m: usize, k: usize,
                    state: &mut WALStoreEmulState, canvas: &mut Canvas, wal: WALLoader,
                    ops: &mut Vec<PaintStrokes>, ringid_map: &mut HashMap<WALRingId, usize>,
                    fgen: G, rng: &mut R) -> Result<(), ()> {
    let mut wal = wal.recover(WALStoreEmul::new(state, fgen, |_, _|{})).unwrap();
    for _ in 0..n {
        let s = (0..m).map(|_|
            PaintStrokes::gen_rand(1000, 10, 256, 5, rng)).collect::<Vec<PaintStrokes>>();
        let recs = s.iter().map(|e| e.to_bytes()).collect::<Vec<WALBytes>>();
        // write ahead
        let rids = wal.grow(recs)?;
        for rid in rids.iter() {
            println!("got ring id: {:?}", rid);
        }
        // WAL append done
        // prepare data writes
        for (e, rid) in s.into_iter().zip(rids.iter()) {
            canvas.prepaint(&e, &*rid);
            ops.push(e);
            ringid_map.insert(*rid, ops.len() - 1);
        }
        // run the scheduler for a bit
        for _ in 0..k {
            if let Some((fin_rid, t)) = canvas.rand_paint(rng) {
                if let Some(rid) = fin_rid {
                    wal.peel(&[rid])?
                }
                //trace.push(t);
            } else { break }
        }
    }
    while let Some((fin_rid, t)) = canvas.rand_paint(rng) {
        if let Some(rid) = fin_rid {
            wal.peel(&[rid])?
        }
        //trace.push(t);
    }
    canvas.print(40);
    Ok(())
}

fn check(state: &mut WALStoreEmulState, canvas: &mut Canvas,
         wal: WALLoader,
         ops: &Vec<PaintStrokes>, ringid_map: &HashMap<WALRingId, usize>) -> bool {
    let mut last_idx = ops.len() - 1;
    canvas.clear_queued();
    wal.recover(WALStoreEmul::new(state, common::ZeroFailGen, |payload, ringid| {
        let s = PaintStrokes::from_bytes(&payload);
        canvas.prepaint(&s, &ringid);
        last_idx = *ringid_map.get(&ringid).unwrap();
    })).unwrap();
    println!("last = {}/{}", last_idx, ops.len() - 1);
    canvas.paint_all();
    // recover complete
    let canvas0 = canvas.new_reference(&ops[..last_idx + 1]);
    let res = canvas.is_same(&canvas0);
    if !res {
        canvas.print(40);
        canvas0.print(40);
    }
    res
}

#[test]
fn test_rand_fail() {
    let fgen = SingleFailGen::new(105);
    let n = 100;
    let m = 10;
    let k = 100;
    let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::from_seed([0; 32]); //rand::thread_rng();
    let mut state = WALStoreEmulState::new();
    let wal = WALLoader::new(9, 8, 1000);
    let mut ops: Vec<PaintStrokes> = Vec::new();
    let mut ringid_map = HashMap::new();
    let mut canvas = Canvas::new(1000);
    run(n, m, k, &mut state, &mut canvas, wal, &mut ops, &mut ringid_map, fgen, &mut rng);
    let wal = WALLoader::new(9, 8, 1000);
    assert!(check(&mut state, &mut canvas, wal, &ops, &ringid_map));
}
