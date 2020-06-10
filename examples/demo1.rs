use growthring::{WALFile, WALStore, WALPos, WALBytes, WALLoader, WALWriter};

struct WALFileFake {
    filename: String
}

impl WALFile for WALFileFake {
    fn allocate(&self, offset: WALPos, length: usize) {
        println!("{}.allocate(offset=0x{:x}, end=0x{:x})", self.filename, offset, offset + length as u64);
    }
    fn write(&self, offset: WALPos, data: WALBytes) {
        println!("{}.write(offset=0x{:x}, end=0x{:x}, data=0x{})", self.filename, offset, offset + data.len() as u64, hex::encode(data));
    }
    fn read(&self, offset: WALPos, length: usize) -> WALBytes {
        Vec::new().into_boxed_slice()
    }
}

struct WALStoreFake;
impl WALStore for WALStoreFake {
    fn open_file(&self, filename: &str, touch: bool) -> Option<Box<dyn WALFile>> {
        println!("open_file(filename={}, touch={}", filename, touch);
        let filename = filename.to_string();
        Some(Box::new(WALFileFake{ filename }))
    }
    fn remove_file(&self, filename: &str) -> bool {
        println!("remove_file(filename={})", filename);
        true
    }
    fn enumerate_files(&self) -> Box<[String]> {
        println!("enumerate_files()");
        Vec::new().into_boxed_slice()
    }
    fn apply_payload(&self, payload: WALBytes) {
        println!("apply_payload(payload=0x{})", hex::encode(payload))
    }
}

fn test(records: Vec<String>, wal: &mut WALWriter<WALStoreFake>) {
    let records: Vec<WALBytes> = records.into_iter().map(|s| s.into_bytes().into_boxed_slice()).collect();
    let ret = wal.grow(&records);
    for ring_id in ret.iter() {
        println!("got ring id: {:?}", ring_id);
    }
}

fn main() {
    let store = WALStoreFake;
    let mut wal = WALLoader::new(store, 9, 8, 1000).recover();
    for _ in 0..3 {
        test(["hi", "hello", "lol"].iter().map(|s| s.to_string()).collect::<Vec<String>>(), &mut wal)
    }
    for _ in 0..3 {
        test(["a".repeat(10), "b".repeat(100), "c".repeat(1000)].iter().map(|s| s.to_string()).collect::<Vec<String>>(), &mut wal)
    }
}
