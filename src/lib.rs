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
//! let store = WALStoreAIO::new("./walfiles", true, None, None).unwrap();
//! let mut wal = block_on(loader.load(store, |_, _| {Ok(())}, 0)).unwrap();
//! // Write a vector of records to WAL.
//! for f in wal.grow(vec!["record1(foo)", "record2(bar)", "record3(foobar)"]).into_iter() {
//!     let ring_id = block_on(f).unwrap().1;
//!     println!("WAL recorded record to {:?}", ring_id);
//! }
//!
//!
//! // Load from WAL (truncate = false).
//! let store = WALStoreAIO::new("./walfiles", false, None, None).unwrap();
//! let mut wal = block_on(loader.load(store, |payload, ringid| {
//!     // redo the operations in your application
//!     println!("recover(payload={}, ringid={:?})",
//!              std::str::from_utf8(&payload).unwrap(),
//!              ringid);
//!     Ok(())
//! }, 0)).unwrap();
//! // We saw some log playback, even there is no failure.
//! // Let's try to grow the WAL to create many files.
//! let ring_ids = wal.grow((1..100).into_iter().map(|i| "a".repeat(i)).collect::<Vec<_>>())
//!                   .into_iter().map(|f| block_on(f).unwrap().1).collect::<Vec<_>>();
//! // Then assume all these records are not longer needed. We can tell WALWriter by the `peel`
//! // method.
//! block_on(wal.peel(ring_ids, 0)).unwrap();
//! // There will only be one remaining file in ./walfiles.
//!
//! let store = WALStoreAIO::new("./walfiles", false, None, None).unwrap();
//! let wal = block_on(loader.load(store, |payload, _| {
//!     println!("payload.len() = {}", payload.len());
//!     Ok(())
//! }, 0)).unwrap();
//! // After each recovery, the ./walfiles is empty.
//! ```

#[macro_use] extern crate scan_fmt;
pub mod wal;

use aiofut::{AIOBuilder, AIOManager};
use async_trait::async_trait;
use libc::off_t;
use nix::fcntl::{fallocate, open, openat, FallocateFlags, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{ftruncate, mkdir, unlinkat, UnlinkatFlags};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::sync::Arc;
use wal::{WALBytes, WALFile, WALPos, WALStore};

pub struct WALFileAIO {
    fd: OwnedFd,
    aiomgr: Arc<AIOManager>,
}

impl WALFileAIO {
    pub fn new(
        rootfd: BorrowedFd, filename: &str, aiomgr: Arc<AIOManager>,
    ) -> Result<Self, ()> {
        openat(
            rootfd.as_raw_fd(),
            filename,
            OFlag::O_CREAT | OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )
        .and_then(|fd| {
            Ok(WALFileAIO {
                fd: unsafe { OwnedFd::from_raw_fd(fd) },
                aiomgr,
            })
        })
        .or_else(|_| Err(()))
    }
}

#[async_trait(?Send)]
impl WALFile for WALFileAIO {
    async fn allocate(&self, offset: WALPos, length: usize) -> Result<(), ()> {
        // TODO: is there any async version of fallocate?
        fallocate(
            self.fd.as_raw_fd(),
            FallocateFlags::FALLOC_FL_ZERO_RANGE,
            offset as off_t,
            length as off_t,
        )
        .and_then(|_| Ok(()))
        .or_else(|_| Err(()))
    }

    fn truncate(&self, length: usize) -> Result<(), ()> {
        ftruncate(&self.fd, length as off_t).or_else(|_| Err(()))
    }

    async fn write(&self, offset: WALPos, data: WALBytes) -> Result<(), ()> {
        let (res, data) = self
            .aiomgr
            .write(self.fd.as_raw_fd(), offset, data, None)
            .await;
        res.or_else(|_| Err(())).and_then(|nwrote| {
            if nwrote == data.len() {
                Ok(())
            } else {
                Err(())
            }
        })
    }

    async fn read(
        &self, offset: WALPos, length: usize,
    ) -> Result<Option<WALBytes>, ()> {
        let (res, data) = self
            .aiomgr
            .read(self.fd.as_raw_fd(), offset, length, None)
            .await;
        res.or_else(|_| Err(())).and_then(|nread| {
            Ok(if nread == length { Some(data) } else { None })
        })
    }
}

pub struct WALStoreAIO {
    rootfd: OwnedFd,
    aiomgr: Arc<AIOManager>,
}

unsafe impl Send for WALStoreAIO {}

impl WALStoreAIO {
    pub fn new(
        wal_dir: &str, truncate: bool, rootfd: Option<BorrowedFd>,
        aiomgr: Option<AIOManager>,
    ) -> Result<Self, ()> {
        let aiomgr = Arc::new(aiomgr.ok_or(Err(())).or_else(
            |_: Result<AIOManager, ()>| {
                AIOBuilder::default().build().or(Err(()))
            },
        )?);

        if truncate {
            let _ = std::fs::remove_dir_all(wal_dir);
        }
        let walfd;
        match rootfd {
            None => {
                match mkdir(
                    wal_dir,
                    Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IXUSR,
                ) {
                    Err(e) => {
                        if truncate {
                            panic!("error while creating directory: {}", e)
                        }
                    }
                    Ok(_) => (),
                }
                walfd = match open(
                    wal_dir,
                    OFlag::O_DIRECTORY | OFlag::O_PATH,
                    Mode::empty(),
                ) {
                    Ok(fd) => fd,
                    Err(_) => panic!("error while opening the WAL directory"),
                }
            }
            Some(fd) => {
                let dirstr = std::ffi::CString::new(wal_dir).unwrap();
                let ret = unsafe {
                    libc::mkdirat(
                        fd.as_raw_fd(),
                        dirstr.as_ptr(),
                        libc::S_IRUSR | libc::S_IWUSR | libc::S_IXUSR,
                    )
                };
                if ret != 0 {
                    if truncate {
                        panic!("error while creating directory")
                    }
                }
                walfd = match nix::fcntl::openat(
                    fd.as_raw_fd(),
                    wal_dir,
                    OFlag::O_DIRECTORY | OFlag::O_PATH,
                    Mode::empty(),
                ) {
                    Ok(fd) => fd,
                    Err(_) => panic!("error while opening the WAL directory"),
                }
            }
        }
        Ok(WALStoreAIO {
            rootfd: unsafe { OwnedFd::from_raw_fd(walfd) },
            aiomgr,
        })
    }
}

#[async_trait(?Send)]
impl WALStore for WALStoreAIO {
    type FileNameIter = std::vec::IntoIter<String>;

    async fn open_file(
        &self, filename: &str, _touch: bool,
    ) -> Result<Box<dyn WALFile>, ()> {
        let filename = filename.to_string();
        WALFileAIO::new(self.rootfd.as_fd(), &filename, self.aiomgr.clone())
            .and_then(|f| Ok(Box::new(f) as Box<dyn WALFile>))
    }

    async fn remove_file(&self, filename: String) -> Result<(), ()> {
        unlinkat(
            Some(self.rootfd.as_raw_fd()),
            filename.as_str(),
            UnlinkatFlags::NoRemoveDir,
        )
        .or_else(|_| Err(()))
    }

    fn enumerate_files(&self) -> Result<Self::FileNameIter, ()> {
        let mut logfiles = Vec::new();
        for ent in nix::dir::Dir::openat(
            self.rootfd.as_raw_fd(),
            "./",
            OFlag::empty(),
            Mode::empty(),
        )
        .unwrap()
        .iter()
        {
            logfiles
                .push(ent.unwrap().file_name().to_str().unwrap().to_string())
        }
        Ok(logfiles.into_iter())
    }
}
