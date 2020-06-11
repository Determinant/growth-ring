use std::os::unix::io::RawFd;
use nix::unistd::{close, mkdir, unlinkat, UnlinkatFlags, ftruncate};
use nix::fcntl::{open, openat, OFlag, fallocate, FallocateFlags};
use nix::sys::{stat::Mode, uio::{pwrite, pread}};
use rand::{Rng, seq::SliceRandom};
use libc::off_t;

use growthring::wal::{WALFile, WALStore, WALPos, WALBytes, WALLoader, WALWriter, WALRingId};

struct WALFileTest {
    filename: String,
    fd: RawFd,
}

impl WALFileTest {
    fn new(rootfd: RawFd, filename: &str) -> Result<Self, ()> {
        openat(rootfd, filename,
            OFlag::O_CREAT | OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR).and_then(|fd| {
            let filename = filename.to_string();
            Ok (WALFileTest {
                filename,
                fd,
            })
        }).or_else(|_| Err(()))
    }
}

impl Drop for WALFileTest {
    fn drop(&mut self) {
        close(self.fd).unwrap();
    }
}

impl WALFile for WALFileTest {
    fn allocate(&self, offset: WALPos, length: usize) -> Result<(), ()> {
        println!("{}.allocate(offset=0x{:x}, end=0x{:x})", self.filename, offset, offset + length as u64);
        fallocate(self.fd,
                FallocateFlags::FALLOC_FL_ZERO_RANGE,
                offset as off_t, length as off_t).and_then(|_| Ok(())).or_else(|_| Err(()))
    }

    fn truncate(&self, length: usize) -> Result<(), ()> {
        println!("{}.truncate(length={})", self.filename, length);
        ftruncate(self.fd, length as off_t).or_else(|_| Err(()))
    }

    fn write(&self, offset: WALPos, data: WALBytes) -> Result<(), ()> {
        println!("{}.write(offset=0x{:x}, end=0x{:x}, data=0x{})",
                self.filename, offset, offset + data.len() as u64, hex::encode(&data));
        pwrite(self.fd, &*data, offset as off_t)
            .or_else(|_| Err(()))
            .and_then(|nwrote| if nwrote == data.len() { Ok(()) } else { Err(()) })
    }

    fn read(&self, offset: WALPos, length: usize) -> Result<Option<WALBytes>, ()> {
        let mut buff = Vec::new();
        buff.resize(length, 0);
        pread(self.fd, &mut buff[..], offset as off_t)
            .or_else(|_| Err(()))
            .and_then(|nread| Ok(if nread == length {Some(buff.into_boxed_slice())} else {None}))
    }
}

struct WALStoreTest {
    rootfd: RawFd,
    rootpath: String
}

impl WALStoreTest {
    fn new(wal_dir: &str, truncate: bool) -> Self {
        let rootpath = wal_dir.to_string();
        if truncate {
            let _ = std::fs::remove_dir_all(wal_dir);
        }
        match mkdir(wal_dir, Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IXUSR) {
            Err(e) => if truncate { panic!("error while creating directory: {}", e) },
            Ok(_) => ()
        }
        let rootfd = match open(wal_dir, OFlag::O_DIRECTORY | OFlag::O_PATH, Mode::empty()) {
            Ok(fd) => fd,
            Err(_) => panic!("error while opening the DB")
        };
        WALStoreTest { rootfd, rootpath }
    }
}

impl Drop for WALStoreTest {
    fn drop(&mut self) {
        close(self.rootfd).unwrap();
    }
}

impl WALStore for WALStoreTest {
    type FileNameIter = std::vec::IntoIter<String>;

    fn open_file(&mut self, filename: &str, touch: bool) -> Result<Box<dyn WALFile>, ()> {
        println!("open_file(filename={}, touch={})", filename, touch);
        let filename = filename.to_string();
        WALFileTest::new(self.rootfd, &filename).and_then(|f| Ok(Box::new(f) as Box<dyn WALFile>))
    }

    fn remove_file(&mut self, filename: &str) -> Result<(), ()> {
        println!("remove_file(filename={})", filename);
        unlinkat(Some(self.rootfd), filename, UnlinkatFlags::NoRemoveDir).or_else(|_| Err(()))
    }

    fn enumerate_files(&self) -> Result<Self::FileNameIter, ()> {
        println!("enumerate_files()");
        let mut logfiles = Vec::new();
        for fname in std::fs::read_dir(&self.rootpath).unwrap() {
            logfiles.push(fname.unwrap().file_name().into_string().unwrap())
        }
        Ok(logfiles.into_iter())
    }

    fn apply_payload(&mut self, payload: WALBytes, wal_off: WALPos) -> Result<(), ()> {
        println!("apply_payload(payload={}, wal_off={})",
                 std::str::from_utf8(&payload).unwrap(),
                 wal_off);
        Ok(())
    }
}

fn test(records: Vec<String>, wal: &mut WALWriter<WALStoreTest>) -> Box<[WALRingId]> {
    let records: Vec<WALBytes> = records.into_iter().map(|s| s.into_bytes().into_boxed_slice()).collect();
    let ret = wal.grow(&records).unwrap();
    for ring_id in ret.iter() {
        println!("got ring id: {:?}", ring_id);
    }
    ret
}

fn main() {
    let mut rng = rand::thread_rng();
    let store = WALStoreTest::new("./wal_demo1", true);
    let mut wal = WALLoader::new(store, 9, 8, 1000).recover().unwrap();
    for _ in 0..3 {
        test(["hi", "hello", "lol"].iter().map(|s| s.to_string()).collect::<Vec<String>>(), &mut wal);
    }
    for _ in 0..3 {
        test(vec!["a".repeat(10), "b".repeat(100), "c".repeat(1000)], &mut wal);
    }

    let store = WALStoreTest::new("./wal_demo1", false);
    let mut wal = WALLoader::new(store, 9, 8, 1000).recover().unwrap();
    for _ in 0..3 {
        test(vec!["a".repeat(10), "b".repeat(100), "c".repeat(300), "d".repeat(400)], &mut wal);
    }

    let store = WALStoreTest::new("./wal_demo1", false);
    let mut wal = WALLoader::new(store, 9, 8, 1000).recover().unwrap();
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
            wal.peel(e);
        }
    }
}
