#[cfg(test)]

mod common;
use std::collections::HashMap;
use std::rc::Rc;
use growthring::wal::{WALLoader, WALWriter, WALStore, WALRingId, WALBytes, WALPos};
use common::{FailGen, SingleFailGen, CountFailGen, Canvas, WALStoreEmulState, WALStoreEmul, PaintStrokes};

fn run<G: 'static + FailGen, R: rand::Rng>(n: usize, m: usize, k: usize,
                    state: &mut WALStoreEmulState, canvas: &mut Canvas, wal: WALLoader,
                    ops: &mut Vec<PaintStrokes>, ringid_map: &mut HashMap<WALRingId, usize>,
                    fgen: Rc<G>, rng: &mut R) -> Result<(), ()> {
    let mut wal = wal.recover(WALStoreEmul::new(state, fgen, |_, _|{}))?;
    for _ in 0..n {
        let s = (0..m).map(|_|
            PaintStrokes::gen_rand(1000, 10, 256, 5, rng)).collect::<Vec<PaintStrokes>>();
        let recs = s.iter().map(|e| e.to_bytes()).collect::<Vec<WALBytes>>();
        // write ahead
        let (rids, ok) = wal.grow(recs);
        for (e, rid) in s.iter().zip(rids.iter()) {
            ops.push(e.clone());
            ringid_map.insert(*rid, ops.len() - 1);
        }
        ok?;
        //for rid in rids.iter() {
        //    println!("got ring id: {:?}", rid);
        //}
        // WAL append done
        // prepare data writes
        for (e, rid) in s.into_iter().zip(rids.iter()) {
            canvas.prepaint(&e, &*rid);
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
    if ops.is_empty() { return true }
    let mut last_idx = 0;
    canvas.clear_queued();
    wal.recover(WALStoreEmul::new(state, Rc::new(common::ZeroFailGen), |payload, ringid| {
        let s = PaintStrokes::from_bytes(&payload);
        canvas.prepaint(&s, &ringid);
        last_idx = *ringid_map.get(&ringid).unwrap() + 1;
    })).unwrap();
    println!("last = {}/{}", last_idx, ops.len() - 1);
    canvas.paint_all();
    // recover complete
    let canvas0 = canvas.new_reference(&ops[..last_idx]);
    let res = canvas.is_same(&canvas0);
    if !res {
        canvas.print(40);
        canvas0.print(40);
    }
    res
}

fn get_nticks(n: usize, m: usize, k: usize, csize: usize) -> usize {
    let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::from_seed([0; 32]);
    let mut state = WALStoreEmulState::new();
    let wal = WALLoader::new(9, 8, 1000);
    let mut ops: Vec<PaintStrokes> = Vec::new();
    let mut ringid_map = HashMap::new();
    let mut canvas = Canvas::new(csize);
    let fgen = Rc::new(CountFailGen::new());
    run(n, m, k, &mut state, &mut canvas, wal, &mut ops, &mut ringid_map, fgen.clone(), &mut rng).unwrap();
    fgen.get_count()
}

fn run_(n: usize, m: usize, k: usize, csize: usize) {
    let nticks = get_nticks(n, m, k, csize);
    println!("nticks = {}", nticks);
    for i in 0..nticks {
        let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::from_seed([0; 32]);
        let mut state = WALStoreEmulState::new();
        let wal = WALLoader::new(9, 8, 1000);
        let mut ops: Vec<PaintStrokes> = Vec::new();
        let mut ringid_map = HashMap::new();
        let mut canvas = Canvas::new(csize);
        let fgen = Rc::new(SingleFailGen::new(i));
        if run(n, m, k, &mut state, &mut canvas, wal, &mut ops, &mut ringid_map, fgen, &mut rng).is_err() {
            let wal = WALLoader::new(9, 8, 1000);
            assert!(check(&mut state, &mut canvas, wal, &ops, &ringid_map));
        }
    }
}

#[test]
fn test_rand_fail() {
    run_(100, 10, 100, 1000)
}
