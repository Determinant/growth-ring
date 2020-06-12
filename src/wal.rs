use async_trait::async_trait;
use futures::future::{self, FutureExt, TryFutureExt};
use std::cell::{RefCell, UnsafeCell};
use std::collections::BinaryHeap;
use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;

#[repr(u8)]
enum WALRingType {
    #[allow(dead_code)]
    Null = 0x0,
    Full,
    First,
    Middle,
    Last,
}

#[repr(packed)]
struct WALRingBlob {
    crc32: u32,
    rsize: u32,
    rtype: WALRingType,
    // payload follows
}

pub type WALBytes = Box<[u8]>;
pub type WALFileId = u64;
pub type WALPos = u64;

#[derive(Eq, PartialEq, Copy, Clone, Debug, Hash)]
pub struct WALRingId {
    start: WALPos,
    end: WALPos,
}

impl WALRingId {
    pub fn empty_id() -> Self {
        WALRingId { start: 0, end: 0 }
    }
    pub fn get_start(&self) -> WALPos {
        self.start
    }
    pub fn get_end(&self) -> WALPos {
        self.end
    }
}

impl Ord for WALRingId {
    fn cmp(&self, other: &WALRingId) -> std::cmp::Ordering {
        other
            .start
            .cmp(&self.start)
            .then_with(|| other.end.cmp(&self.end))
    }
}

impl PartialOrd for WALRingId {
    fn partial_cmp(&self, other: &WALRingId) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// the state for a WAL writer
struct WALState {
    /// the first file id of WAL
    first_fid: WALFileId,
    /// the next position for a record, addressed in the entire WAL space
    next: WALPos,
    /// number of bits for a file
    file_nbit: u64,
}

#[async_trait(?Send)]
pub trait WALFile {
    /// Initialize the file space in [offset, offset + length) to zero.
    async fn allocate(&self, offset: WALPos, length: usize) -> Result<(), ()>;
    /// Write data with offset. We assume all previous `allocate/truncate` invocations are visible
    /// if ordered earlier (should be guaranteed by most OS).  Additionally, the final write caused
    /// by each invocation of this function should be _atomic_ (the entire single write should be
    /// all or nothing).
    async fn write(&self, offset: WALPos, data: WALBytes) -> Result<(), ()>;
    /// Read data with offset. Return Ok(None) when it reaches EOF.
    fn read(
        &self,
        offset: WALPos,
        length: usize,
    ) -> Result<Option<WALBytes>, ()>;
    /// Truncate a file to a specified length.
    fn truncate(&self, length: usize) -> Result<(), ()>;
}

#[async_trait(?Send)]
pub trait WALStore {
    type FileNameIter: Iterator<Item = String>;

    /// Open a file given the filename, create the file if not exists when `touch` is `true`.
    async fn open_file(
        &self,
        filename: &str,
        touch: bool,
    ) -> Result<Box<dyn WALFile>, ()>;
    /// Unlink a file given the filename.
    fn remove_file(&self, filename: &str) -> Result<(), ()>;
    /// Enumerate all WAL filenames. It should include all WAL files that are previously opened
    /// (created) but not removed. The list could be unordered.
    fn enumerate_files(&self) -> Result<Self::FileNameIter, ()>;
    /// Apply the payload during recovery. An invocation of the callback waits the application for
    /// redoing the given operation to ensure its state is consistent. We assume the necessary
    /// changes by the payload has already been persistent when the callback returns.
    fn apply_payload(
        &self,
        payload: WALBytes,
        ringid: WALRingId,
    ) -> Result<(), ()>;
}

/// The middle layer that manages WAL file handles and invokes public trait functions to actually
/// manipulate files and their contents.
struct WALFilePool<F: WALStore> {
    store: F,
    handles: RefCell<lru::LruCache<WALFileId, Box<dyn WALFile>>>,
    last_write:
        UnsafeCell<MaybeUninit<Pin<Box<dyn Future<Output = Result<(), ()>>>>>>,
    file_nbit: u64,
    file_size: u64,
    block_nbit: u64,
}

impl<F: WALStore> WALFilePool<F> {
    fn new(store: F, file_nbit: u8, block_nbit: u8, cache_size: usize) -> Self {
        let file_nbit = file_nbit as u64;
        let block_nbit = block_nbit as u64;
        WALFilePool {
            store,
            handles: RefCell::new(lru::LruCache::new(cache_size)),
            last_write: UnsafeCell::new(MaybeUninit::new(Box::pin(
                future::ready(Ok(())),
            ))),
            file_nbit,
            file_size: 1 << (file_nbit as u64),
            block_nbit,
        }
    }

    fn get_fname(fid: WALFileId) -> String {
        format!("{:08x}.log", fid)
    }

    fn get_file(
        &self,
        fid: u64,
        touch: bool,
    ) -> impl Future<Output = Result<&dyn WALFile, ()>> {
        async move {
            if let Some(h) = self.handles.borrow_mut().get(&fid) {
                return Ok(unsafe { &*(&**h as *const dyn WALFile) });
            }
            let h = {
                let mut handles = self.handles.borrow_mut();
                handles.put(
                    fid,
                    self.store.open_file(&Self::get_fname(fid), touch).await?,
                );
                &**handles.get(&fid).unwrap() as *const dyn WALFile
            };
            Ok(unsafe { &*(h) })
        }
    }

    fn get_fid(&mut self, fname: &str) -> WALFileId {
        scan_fmt!(fname, "{x}.log", [hex WALFileId]).unwrap()
    }

    // TODO: evict stale handles
    fn write<'a>(
        &'a mut self,
        writes: Vec<(WALPos, WALBytes)>,
    ) -> Vec<Pin<Box<dyn Future<Output = Result<(), ()>> + 'a>>> {
        let file_size = self.file_size;
        let file_nbit = self.file_nbit;
        let meta: Vec<(u64, u64)> = writes
            .iter()
            .map(|(off, w)| ((*off) >> file_nbit, w.len() as u64))
            .collect();
        let mut files: Vec<Pin<Box<dyn Future<Output = _> + 'a>>> = Vec::new();
        for &(fid, _) in meta.iter() {
            files.push(Box::pin(self.get_file(fid, true))
                as Pin<Box<dyn Future<Output = _> + 'a>>)
        }
        let mut fid = writes[0].0 >> file_nbit;
        let mut alloc_start = writes[0].0 & (self.file_size - 1);
        let mut alloc_end = alloc_start + writes[0].1.len() as u64;
        let last_write = unsafe {
            std::mem::replace(
                &mut *self.last_write.get(),
                std::mem::MaybeUninit::uninit(),
            )
            .assume_init()
        };
        // pre-allocate the file space
        let alloc = async move {
            last_write.await?;
            let mut last_h: Option<
                Pin<Box<dyn Future<Output = Result<&'a dyn WALFile, ()>> + 'a>>,
            > = None;
            for ((next_fid, wl), h) in meta.into_iter().zip(files.into_iter()) {
                if let Some(lh) = last_h.take() {
                    if next_fid != fid {
                        lh.await?
                            .allocate(
                                alloc_start,
                                (alloc_end - alloc_start) as usize,
                            )
                            .await?;
                        last_h = Some(h);
                        alloc_start = 0;
                        alloc_end = alloc_start + wl;
                        fid = next_fid;
                    } else {
                        last_h = Some(lh);
                        alloc_end += wl;
                    }
                } else {
                    last_h = Some(h);
                }
            }
            if let Some(lh) = last_h {
                lh.await?
                    .allocate(alloc_start, (alloc_end - alloc_start) as usize)
                    .await?
            }
            Ok(())
        };
        let mut res = Vec::new();
        let mut prev = Box::pin(alloc) as Pin<Box<dyn Future<Output = _> + 'a>>;
        for (off, w) in writes.into_iter() {
            let f = self.get_file(off >> file_nbit, true);
            let w = (async move {
                prev.await?;
                f.await?.write(off & (file_size - 1), w).await
            })
            .shared();
            prev = Box::pin(w.clone());
            res.push(Box::pin(w) as Pin<Box<dyn Future<Output = _> + 'a>>)
        }
        unsafe {
            (*self.last_write.get()) = MaybeUninit::new(std::mem::transmute::<
                Pin<Box<dyn Future<Output = _> + 'a>>,
                Pin<Box<dyn Future<Output = _> + 'static>>,
            >(prev))
        }
        res
    }

    fn remove_file(&mut self, fid: u64) -> Result<(), ()> {
        self.store.remove_file(&Self::get_fname(fid))
    }

    fn reset(&mut self) {
        self.handles.borrow_mut().clear()
    }
}

pub struct WALWriter<F: WALStore> {
    state: WALState,
    file_pool: WALFilePool<F>,
    block_buffer: WALBytes,
    block_size: u32,
    next_complete: WALPos,
    io_complete: BinaryHeap<WALRingId>,
    msize: usize,
}

impl<F: WALStore> WALWriter<F> {
    fn new(state: WALState, file_pool: WALFilePool<F>) -> Self {
        let mut b = Vec::new();
        let block_size = 1 << file_pool.block_nbit as u32;
        let msize = std::mem::size_of::<WALRingBlob>();
        b.resize(block_size as usize, 0);
        WALWriter {
            state,
            file_pool,
            block_buffer: b.into_boxed_slice(),
            block_size,
            next_complete: 0,
            io_complete: BinaryHeap::new(),
            msize,
        }
    }

    /// Submit a sequence of records to WAL; WALStore/WALFile callbacks are invoked before the
    /// function returns.  The caller then has the knowledge of WAL writes so it should defer
    /// actual data writes after WAL writes.
    pub fn grow<'a, T: AsRef<[WALBytes]>>(
        &'a mut self,
        records: T,
    ) -> Vec<impl Future<Output = Result<WALRingId, ()>> + 'a> {
        let mut res = Vec::new();
        let mut writes = Vec::new();
        let msize = self.msize as u32;
        // the global offest of the begining of the block
        // the start of the unwritten data
        let mut bbuff_start = self.state.next as u32 & (self.block_size - 1);
        // the end of the unwritten data
        let mut bbuff_cur = bbuff_start;

        for _rec in records.as_ref() {
            let mut rec = &_rec[..];
            let mut rsize = rec.len() as u32;
            let mut ring_start = None;
            while rsize > 0 {
                let remain = self.block_size - bbuff_cur;
                if remain > msize {
                    let d = remain - msize;
                    let rs0 =
                        self.state.next + (bbuff_cur - bbuff_start) as u64;
                    let blob = unsafe {
                        std::mem::transmute::<*mut u8, &mut WALRingBlob>(
                            (&mut self.block_buffer[bbuff_cur as usize..])
                                .as_mut_ptr(),
                        )
                    };
                    bbuff_cur += msize;
                    if d >= rsize {
                        // the remaining rec fits in the block
                        let payload = rec;
                        blob.crc32 = crc::crc32::checksum_ieee(payload);
                        blob.rsize = rsize;
                        let (rs, rt) = if let Some(rs) = ring_start.take() {
                            (rs, WALRingType::Last)
                        } else {
                            (rs0, WALRingType::Full)
                        };
                        blob.rtype = rt;
                        &mut self.block_buffer[bbuff_cur as usize..
                            bbuff_cur as usize + payload.len()]
                            .copy_from_slice(payload);
                        bbuff_cur += rsize;
                        rsize = 0;
                        let end =
                            self.state.next + (bbuff_cur - bbuff_start) as u64;
                        res.push((WALRingId { start: rs, end }, Vec::new()));
                    } else {
                        // the remaining block can only accommodate partial rec
                        let payload = &rec[..d as usize];
                        blob.crc32 = crc::crc32::checksum_ieee(payload);
                        blob.rsize = d;
                        blob.rtype = if ring_start.is_some() {
                            WALRingType::Middle
                        } else {
                            ring_start = Some(rs0);
                            WALRingType::First
                        };
                        &mut self.block_buffer[bbuff_cur as usize..
                            bbuff_cur as usize + payload.len()]
                            .copy_from_slice(payload);
                        bbuff_cur += d;
                        rsize -= d;
                        rec = &rec[d as usize..];
                    }
                } else {
                    // add padding space by moving the point to the end of the block
                    bbuff_cur = self.block_size;
                }
                if bbuff_cur == self.block_size {
                    writes.push((
                        self.state.next,
                        self.block_buffer[bbuff_start as usize..]
                            .to_vec()
                            .into_boxed_slice(),
                    ));
                    self.state.next += (self.block_size - bbuff_start) as u64;
                    bbuff_start = 0;
                    bbuff_cur = 0;
                }
            }
        }
        if bbuff_cur > bbuff_start {
            writes.push((
                self.state.next,
                self.block_buffer[bbuff_start as usize..bbuff_cur as usize]
                    .to_vec()
                    .into_boxed_slice(),
            ));
            self.state.next += (bbuff_cur - bbuff_start) as u64;
        }

        // mark the block info for each record
        let mut i = 0;
        'outer: for (j, (off, w)) in writes.iter().enumerate() {
            let blk_s = *off;
            let blk_e = blk_s + w.len() as u64;
            while res[i].0.end <= blk_s {
                i += 1;
                if i >= res.len() {
                    break 'outer;
                }
            }
            while res[i].0.start < blk_e {
                res[i].1.push(j);
                if res[i].0.end >= blk_e {
                    break;
                }
                i += 1;
                if i >= res.len() {
                    break 'outer;
                }
            }
        }

        let writes: Vec<future::Shared<_>> = self
            .file_pool
            .write(writes)
            .into_iter()
            .map(move |f| async move { f.await }.shared())
            .collect();
        let res = res
            .into_iter()
            .map(|(ringid, blks)| {
                future::try_join_all(
                    blks.into_iter().map(|idx| writes[idx].clone()),
                )
                .or_else(|_| future::ready(Err(())))
                .and_then(move |_| future::ready(Ok(ringid)))
            })
            .collect();
        res
    }

    /// Inform the WALWriter that data writes (specified by a slice of (offset, length) tuples) are
    /// complete so that it could automatically remove obsolete WAL files.
    pub fn peel<T: AsRef<[WALRingId]>>(
        &mut self,
        records: T,
    ) -> Result<(), ()> {
        let msize = self.msize as u64;
        let block_size = self.block_size as u64;
        for rec in records.as_ref() {
            self.io_complete.push(*rec);
        }
        let orig_fid = self.state.first_fid;
        while let Some(s) = self.io_complete.peek().and_then(|&e| Some(e.start))
        {
            if s != self.next_complete {
                break;
            }
            let mut m = self.io_complete.pop().unwrap();
            let block_remain = block_size - (m.end & (block_size - 1));
            if block_remain <= msize as u64 {
                m.end += block_remain
            }
            self.next_complete = m.end
        }
        let next_fid = self.next_complete >> self.state.file_nbit;
        for fid in orig_fid..next_fid {
            self.file_pool.remove_file(fid)?;
        }
        self.state.first_fid = next_fid;
        Ok(())
    }
}

pub struct WALLoader {
    file_nbit: u8,
    block_nbit: u8,
    cache_size: usize,
    msize: usize,
    filename_fmt: regex::Regex,
}

impl WALLoader {
    pub fn new(file_nbit: u8, block_nbit: u8, cache_size: usize) -> Self {
        let msize = std::mem::size_of::<WALRingBlob>();
        assert!(file_nbit > block_nbit);
        assert!(msize < 1 << block_nbit);
        let filename_fmt = regex::Regex::new(r"[0-9a-f]+\.log").unwrap();
        WALLoader {
            file_nbit,
            block_nbit,
            cache_size,
            msize,
            filename_fmt,
        }
    }

    /// Recover by reading the WAL log files.
    pub fn recover<F: WALStore>(self, store: F) -> Result<WALWriter<F>, ()> {
        let mut file_pool = WALFilePool::new(
            store,
            self.file_nbit,
            self.block_nbit,
            self.cache_size,
        );
        let block_size = 1 << file_pool.block_nbit;
        let msize = self.msize as u32;
        let mut logfiles: Vec<String> = file_pool
            .store
            .enumerate_files()?
            .filter(|f| self.filename_fmt.is_match(f))
            .collect();
        // TODO: check for missing logfiles
        logfiles.sort();
        let mut chunks = None;
        for fname in logfiles.iter() {
            let fid = file_pool.get_fid(fname);
            let f =
                futures::executor::block_on(file_pool.get_file(fid, false))?;
            let mut off = 0;
            while let Some(header_raw) = f.read(off, msize as usize)? {
                let ringid_start = (fid << file_pool.file_nbit) + off;
                off += msize as u64;
                let header = unsafe {
                    std::mem::transmute::<*const u8, &WALRingBlob>(
                        header_raw.as_ptr(),
                    )
                };
                let rsize = header.rsize;
                match header.rtype {
                    WALRingType::Full => {
                        assert!(chunks.is_none());
                        let payload = f.read(off, rsize as usize)?.ok_or(())?;
                        off += rsize as u64;
                        file_pool.store.apply_payload(
                            payload,
                            WALRingId {
                                start: ringid_start,
                                end: (fid << file_pool.file_nbit) + off,
                            },
                        )?;
                    }
                    WALRingType::First => {
                        assert!(chunks.is_none());
                        chunks = Some((
                            vec![f.read(off, rsize as usize)?.ok_or(())?],
                            ringid_start,
                        ));
                        off += rsize as u64;
                    }
                    WALRingType::Middle => {
                        if let Some((chunks, _)) = &mut chunks {
                            chunks
                                .push(f.read(off, rsize as usize)?.ok_or(())?);
                        } // otherwise ignore the leftover
                        off += rsize as u64;
                    }
                    WALRingType::Last => {
                        if let Some((mut chunks, ringid_start)) = chunks.take()
                        {
                            chunks
                                .push(f.read(off, rsize as usize)?.ok_or(())?);
                            off += rsize as u64;
                            let mut payload = Vec::new();
                            payload.resize(
                                chunks.iter().fold(0, |acc, v| acc + v.len()),
                                0,
                            );
                            let mut ps = &mut payload[..];
                            for c in chunks {
                                ps[..c.len()].copy_from_slice(&*c);
                                ps = &mut ps[c.len()..];
                            }
                            file_pool.store.apply_payload(
                                payload.into_boxed_slice(),
                                WALRingId {
                                    start: ringid_start,
                                    end: (fid << file_pool.file_nbit) + off,
                                },
                            )?;
                        }
                        // otherwise ignore the leftover
                        else {
                            off += rsize as u64;
                        }
                    }
                    WALRingType::Null => break,
                }
                let block_remain = block_size - (off & (block_size - 1));
                if block_remain <= msize as u64 {
                    off += block_remain;
                }
            }
            f.truncate(0)?;
            file_pool.remove_file(fid)?;
        }
        file_pool.reset();
        Ok(WALWriter::new(
            WALState {
                first_fid: 0,
                next: 0,
                file_nbit: file_pool.file_nbit,
            },
            file_pool,
        ))
    }
}
