use growthring::{WALFile, WALStore, WALPos, WALBytes, WALLoader, WALWriter};

use std::os::unix::io::RawFd;
use nix::Error::Sys;
use nix::errno::Errno;
use nix::unistd::{close, mkdir, sysconf, SysconfVar};
use nix::fcntl::{open, openat, OFlag, fallocate, FallocateFlags};
use nix::sys::{stat::Mode, uio::pwrite};
use libc::off_t;

struct WALFileTest {
    filename: String,
    fd: RawFd,
}

impl WALFileTest {
    fn new(rootfd: RawFd, filename: &str) -> Self {
        let fd = openat(rootfd, filename,
            OFlag::O_CREAT | OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
        let filename = filename.to_string();
        WALFileTest {
            filename,
            fd,
        }
    }
}

impl Drop for WALFileTest {
    fn drop(&mut self) {
        close(self.fd).unwrap();
    }
}

impl WALFile for WALFileTest {
    fn allocate(&self, offset: WALPos, length: usize) {
        println!("{}.allocate(offset=0x{:x}, end=0x{:x})", self.filename, offset, offset + length as u64);
        fallocate(self.fd, FallocateFlags::FALLOC_FL_ZERO_RANGE, offset as off_t, length as off_t);
    }
    fn write(&self, offset: WALPos, data: WALBytes) {
        println!("{}.write(offset=0x{:x}, end=0x{:x}, data=0x{})",
                self.filename, offset, offset + data.len() as u64, hex::encode(&data));
        pwrite(self.fd, &*data, offset as off_t);
    }
    fn read(&self, offset: WALPos, length: usize) -> WALBytes {
        unreachable!()
    }
}

struct WALStoreTest {
    rootfd: RawFd
}

impl WALStoreTest {
    fn new(wal_dir: &str, truncate: bool) -> Self {
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
        WALStoreTest { rootfd }
    }
}

impl Drop for WALStoreTest {
    fn drop(&mut self) {
        close(self.rootfd).unwrap();
    }
}

impl WALStore for WALStoreTest {
    fn open_file(&self, filename: &str, touch: bool) -> Option<Box<dyn WALFile>> {
        println!("open_file(filename={}, touch={}", filename, touch);
        let filename = filename.to_string();
        Some(Box::new(WALFileTest::new(self.rootfd, &filename)))
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

fn test(records: Vec<String>, wal: &mut WALWriter<WALStoreTest>) {
    let records: Vec<WALBytes> = records.into_iter().map(|s| s.into_bytes().into_boxed_slice()).collect();
    let ret = wal.grow(&records);
    for ring_id in ret.iter() {
        println!("got ring id: {:?}", ring_id);
    }
}

fn main() {
    let store = WALStoreTest::new("./wal_demo1", true);
    let mut wal = WALLoader::new(store, 9, 8, 1000).recover();
    for _ in 0..3 {
        test(["hi", "hello", "lol"].iter().map(|s| s.to_string()).collect::<Vec<String>>(), &mut wal)
    }
    for _ in 0..3 {
        test(["a".repeat(10), "b".repeat(100), "c".repeat(1000)].iter().map(|s| s.to_string()).collect::<Vec<String>>(), &mut wal)
    }
}
