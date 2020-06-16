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
    aiomgr: Rc<RefCell<AIOManager<AIOBatchSchedulerIn>>>,
}

impl WALFileAIO {
    pub fn new(
        rootfd: RawFd,
        filename: &str,
        aiomgr: Rc<RefCell<AIOManager<AIOBatchSchedulerIn>>>,
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
            .borrow_mut()
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
        block_on(self.aiomgr.borrow_mut().read(self.fd, offset, length, None))
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
}

impl<F: FnMut(WALBytes, WALRingId) -> Result<(), ()>> WALStoreAIO<F> {
    pub fn new(wal_dir: &str, truncate: bool, recover_func: F) -> Self {
        let recover_func = RefCell::new(recover_func);
        let rootpath = wal_dir.to_string();
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
        WALStoreAIO {
            rootfd,
            rootpath,
            recover_func,
        }
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
        let aiomgr = Rc::new(RefCell::new(
            AIOManager::new(new_batch_scheduler(None), 10, None, None)
                .or(Err(()))?,
        ));
        WALFileAIO::new(self.rootfd, &filename, aiomgr.clone())
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
