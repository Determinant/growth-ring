use growthring::{
    wal::{WALBytes, WALRingId, WALLoader, WALWriter},
    WALStoreAIO,
};
use rand::{seq::SliceRandom, Rng};

fn test(
    records: Vec<String>,
    wal: &mut WALWriter<WALStoreAIO>,
) -> Vec<WALRingId> {
    let mut res = Vec::new();
    for r in wal.grow(records).into_iter() {
        let ring_id = futures::executor::block_on(r).unwrap().1;
        println!("got ring id: {:?}", ring_id);
        res.push(ring_id);
    }
    res
}

fn recover(payload: WALBytes, ringid: WALRingId) -> Result<(), ()> {
    println!(
        "recover(payload={}, ringid={:?}",
        std::str::from_utf8(&payload).unwrap(),
        ringid
    );
    Ok(())
}

fn main() {
    let wal_dir = "./wal_demo1";
    let mut rng = rand::thread_rng();
    let mut loader = WALLoader::new();
    loader.file_nbit(9).block_nbit(8);

    let store = WALStoreAIO::new(&wal_dir, true, None).unwrap();
    let mut wal = loader.load(store, recover).unwrap();
    for _ in 0..3 {
        test(
            ["hi", "hello", "lol"]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            &mut wal,
        );
    }
    for _ in 0..3 {
        test(
            vec!["a".repeat(10), "b".repeat(100), "c".repeat(1000)],
            &mut wal,
        );
    }

    let store = WALStoreAIO::new(&wal_dir, false, None).unwrap();
    let mut wal = loader.load(store, recover).unwrap();
    for _ in 0..3 {
        test(
            vec![
                "a".repeat(10),
                "b".repeat(100),
                "c".repeat(300),
                "d".repeat(400),
            ],
            &mut wal,
        );
    }

    let store = WALStoreAIO::new(&wal_dir, false, None).unwrap();
    let mut wal = loader.load(store, recover).unwrap();
    for _ in 0..3 {
        let mut ids = Vec::new();
        for _ in 0..3 {
            let mut records = Vec::new();
            for _ in 0..100 {
                records.push("a".repeat(rng.gen_range(1, 10)))
            }
            for id in test(records, &mut wal).iter() {
                ids.push(*id)
            }
        }
        ids.shuffle(&mut rng);
        for e in ids.chunks(20) {
            println!("peel(20)");
            futures::executor::block_on(wal.peel(e)).unwrap();
        }
    }
}
