#[cfg(test)]

extern crate growthring;
use growthring::wal::{WALLoader, WALWriter, WALStore, WALRingId, WALBytes};

mod common;

fn test<S: WALStore>(records: Vec<String>, wal: &mut WALWriter<S>) -> Box<[WALRingId]> {
    let records: Vec<WALBytes> = records.into_iter().map(|s| s.into_bytes().into_boxed_slice()).collect();
    let ret = wal.grow(&records).unwrap();
    for ring_id in ret.iter() {
        println!("got ring id: {:?}", ring_id);
    }
    ret
}

#[test]
fn test_rand_fail() {
    let fgen = common::SingleFailGen::new(100);
    let mut state = common::WALStoreEmulState::new();
    let mut wal = WALLoader::new(common::WALStoreEmul::new(&mut state, fgen), 9, 8, 1000).recover().unwrap();
    for _ in 0..3 {
        test(["hi", "hello", "lol"].iter().map(|s| s.to_string()).collect::<Vec<String>>(), &mut wal);
    }
    WALLoader::new(common::WALStoreEmul::new(&mut state, common::ZeroFailGen), 9, 8, 1000).recover().unwrap();
}
