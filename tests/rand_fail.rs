#[cfg(test)]

mod common;
use growthring::wal::{WALLoader, WALWriter, WALStore, WALRingId, WALBytes, WALPos};
use common::{FailGen, SingleFailGen, Canvas, WALStoreEmulState, WALStoreEmul, PaintStrokes};

fn run<F: WALStore, R: rand::Rng>(n: usize, m: usize, k: usize,
                    canvas: &mut Canvas, wal: &mut WALWriter<F>, trace: &mut Vec<u32>,
                    rng: &mut R) -> Result<(), ()> {
    for i in 0..n {
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
        for (e, rid) in s.iter().zip(rids.iter()) {
            canvas.prepaint(e, &*rid)
        }
        // run the scheduler for a bit
        for _ in 0..k {
            if let Some((fin_rid, t)) = canvas.rand_paint(rng) {
                if let Some(rid) = fin_rid {
                    wal.peel(&[rid])?
                }
                trace.push(t);
            } else { break }
        }
    }
    while let Some((fin_rid, t)) = canvas.rand_paint(rng) {
        if let Some(rid) = fin_rid {
            wal.peel(&[rid])?
        }
        trace.push(t);
    }
    canvas.print(40);
    Ok(())
}

fn check<F: WALStore>(canvas: &mut Canvas, wal: &mut WALLoader<F>, trace: &Vec<u8>) -> bool {
    true
}

#[test]
fn test_rand_fail() {
    let fgen = SingleFailGen::new(100000);
    let n = 100;
    let m = 10;
    let k = 100;
    let mut rng = rand::thread_rng();
    let mut state = WALStoreEmulState::new();
    let mut wal = WALLoader::new(WALStoreEmul::new(&mut state, fgen), 9, 8, 1000).recover().unwrap();
    let mut trace: Vec<u32> = Vec::new();
    let mut canvas = Canvas::new(1000);
    run(n, m, k, &mut canvas, &mut wal, &mut trace, &mut rng).unwrap();
    WALLoader::new(common::WALStoreEmul::new(&mut state, common::ZeroFailGen), 9, 8, 1000).recover().unwrap();
}
