//! Simple and modular write-ahead-logging implementation.
//!
//! # Examples
//!
//! ```
//! use growthring::{WALStoreAIO, wal::WALLoader};
//! use futures::executor::block_on;
//! let mut loader = WALLoader::new();
//! loader.file_nbit(9).block_nbit(8);
//!
//!
//! // Start with empty WAL (truncate = true).
//! let store = WALStoreAIO::new("./walfiles", true, |_, _| {Ok(())}, None).unwrap();
//! let mut wal = loader.load(store).unwrap();
//! // Write a vector of records to WAL.
//! for f in wal.grow(vec!["record1(foo)", "record2(bar)", "record3(foobar)"]).into_iter() {
//!     let ring_id = block_on(f).unwrap().1;
//!     println!("WAL recorded record to {:?}", ring_id);
//! }
//!
//!
//! // Load from WAL (truncate = false).
//! let store = WALStoreAIO::new("./walfiles", false, |payload, ringid| {
//!     // redo the operations in your application
//!     println!("recover(payload={}, ringid={:?})",
//!              std::str::from_utf8(&payload).unwrap(),
//!              ringid);
//!     Ok(())
//! }, None).unwrap();
//! let mut wal = loader.load(store).unwrap();
//! // We saw some log playback, even there is no failure.
//! // Let's try to grow the WAL to create many files.
//! let ring_ids = wal.grow((0..100).into_iter().map(|i| "a".repeat(i)).collect::<Vec<_>>())
//!                   .into_iter().map(|f| block_on(f).unwrap().1).collect::<Vec<_>>();
//! // Then assume all these records are not longer needed. We can tell WALWriter by the `peel`
//! // method.
//! block_on(wal.peel(ring_ids)).unwrap();
//! // There will only be one remaining file in ./walfiles.
//!
//! let store = WALStoreAIO::new("./walfiles", false, |payload, _| {
//!     println!("payload.len() = {}", payload.len());
//!     Ok(())
//! }, None).unwrap();
//! let wal = loader.load(store).unwrap();
//! // After each recovery, the ./walfiles is empty.
//! ```

#[macro_use] extern crate scan_fmt;
pub mod wal;

use async_trait::async_trait;
use futures::executor::block_on;
use libaiofut::{new_batch_scheduler, AIOBatchSchedulerIn, AIOManager};
use libc::off_t;
use nix::fcntl::{fallocate, open, openat, FallocateFlags, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{close, ftruncate, mkdir, unlinkat, UnlinkatFlags};
use std::cell::RefCell;
use std::os::unix::io::RawFd;
use std::rc::Rc;
use wal::{WALBytes, WALFile, WALPos, WALRingId, WALStore};

pub struct WALFileAIO {
    fd: RawFd,
    aiomgr: Rc<AIOManager<AIOBatchSchedulerIn>>,
}

impl WALFileAIO {
    pub fn new(
        rootfd: RawFd,
        filename: &str,
        aiomgr: Rc<AIOManager<AIOBatchSchedulerIn>>,
    ) -> Result<Self, ()> {
        openat(
            rootfd,
            filename,
            OFlag::O_CREAT | OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )
        .and_then(|fd| Ok(WALFileAIO { fd, aiomgr }))
        .or_else(|_| Err(()))
    }
}

impl Drop for WALFileAIO {
    fn drop(&mut self) {
        close(self.fd).unwrap();
    }
}

#[async_trait(?Send)]
impl WALFile for WALFileAIO {
    async fn allocate(&self, offset: WALPos, length: usize) -> Result<(), ()> {
        // TODO: is there any async version of fallocate?
        fallocate(
            self.fd,
            FallocateFlags::FALLOC_FL_ZERO_RANGE,
            offset as off_t,
            length as off_t,
        )
        .and_then(|_| Ok(()))
        .or_else(|_| Err(()))
    }

    fn truncate(&self, length: usize) -> Result<(), ()> {
        ftruncate(self.fd, length as off_t).or_else(|_| Err(()))
    }

    async fn write(&self, offset: WALPos, data: WALBytes) -> Result<(), ()> {
        self.aiomgr
            .write(self.fd, offset, data, None)
            .await
            .or_else(|_| Err(()))
            .and_then(|(nwrote, data)| {
                if nwrote == data.len() {
                    Ok(())
                } else {
                    Err(())
                }
            })
    }

    fn read(
        &self,
        offset: WALPos,
        length: usize,
    ) -> Result<Option<WALBytes>, ()> {
        block_on(self.aiomgr.read(self.fd, offset, length, None))
            .or_else(|_| Err(()))
            .and_then(|(nread, data)| {
                Ok(if nread == length { Some(data) } else { None })
            })
    }
}

pub struct WALStoreAIO<F: FnMut(WALBytes, WALRingId) -> Result<(), ()>> {
    rootfd: RawFd,
    rootpath: String,
    recover_func: RefCell<F>,
    aiomgr: Rc<AIOManager<AIOBatchSchedulerIn>>,
}

impl<F: FnMut(WALBytes, WALRingId) -> Result<(), ()>> WALStoreAIO<F> {
    pub fn new(
        wal_dir: &str,
        truncate: bool,
        recover_func: F,
        aiomgr: Option<AIOManager<AIOBatchSchedulerIn>>,
    ) -> Result<Self, ()> {
        let recover_func = RefCell::new(recover_func);
        let rootpath = wal_dir.to_string();
        let aiomgr = Rc::new(aiomgr.ok_or(Err(())).or_else(
            |_: Result<AIOManager<AIOBatchSchedulerIn>, ()>| {
                AIOManager::new(new_batch_scheduler(None), 128, None, None)
                    .or(Err(()))
            },
        )?);

        if truncate {
            let _ = std::fs::remove_dir_all(wal_dir);
        }
        match mkdir(wal_dir, Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IXUSR) {
            Err(e) => {
                if truncate {
                    panic!("error while creating directory: {}", e)
                }
            }
            Ok(_) => (),
        }
        let rootfd = match open(
            wal_dir,
            OFlag::O_DIRECTORY | OFlag::O_PATH,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(_) => panic!("error while opening the WAL directory"),
        };
        Ok(WALStoreAIO {
            rootfd,
            rootpath,
            recover_func,
            aiomgr,
        })
    }
}

#[async_trait(?Send)]
impl<F: FnMut(WALBytes, WALRingId) -> Result<(), ()>> WALStore
    for WALStoreAIO<F>
{
    type FileNameIter = std::vec::IntoIter<String>;

    async fn open_file(
        &self,
        filename: &str,
        _touch: bool,
    ) -> Result<Box<dyn WALFile>, ()> {
        let filename = filename.to_string();
        WALFileAIO::new(self.rootfd, &filename, self.aiomgr.clone())
            .and_then(|f| Ok(Box::new(f) as Box<dyn WALFile>))
    }

    async fn remove_file(&self, filename: String) -> Result<(), ()> {
        unlinkat(
            Some(self.rootfd),
            filename.as_str(),
            UnlinkatFlags::NoRemoveDir,
        )
        .or_else(|_| Err(()))
    }

    fn enumerate_files(&self) -> Result<Self::FileNameIter, ()> {
        let mut logfiles = Vec::new();
        for fname in std::fs::read_dir(&self.rootpath).unwrap() {
            logfiles.push(fname.unwrap().file_name().into_string().unwrap())
        }
        Ok(logfiles.into_iter())
    }

    fn apply_payload(
        &self,
        payload: WALBytes,
        ringid: WALRingId,
    ) -> Result<(), ()> {
        (&mut *self.recover_func.borrow_mut())(payload, ringid)
    }
}
