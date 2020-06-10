#[cfg(test)]

extern crate growthring;
use growthring::wal::{WALLoader, WALWriter, WALRingId, WALBytes};

mod common;

fn test(records: Vec<String>, wal: &mut WALWriter<common::WALStoreEmul>) -> Box<[WALRingId]> {
    let records: Vec<WALBytes> = records.into_iter().map(|s| s.into_bytes().into_boxed_slice()).collect();
    let ret = wal.grow(&records).unwrap();
    for ring_id in ret.iter() {
        println!("got ring id: {:?}", ring_id);
    }
    ret
}

#[test]
fn test_rand_fail() {
    let mut state = common::WALStoreEmulState::new();
    let mut wal = WALLoader::new(common::WALStoreEmul::new(&mut state), 9, 8, 1000).recover().unwrap();
    for _ in 0..3 {
        test(["hi", "hello", "lol"].iter().map(|s| s.to_string()).collect::<Vec<String>>(), &mut wal);
    }
    let mut wal = WALLoader::new(common::WALStoreEmul::new(&mut state), 9, 8, 1000).recover().unwrap();
}
