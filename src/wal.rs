use async_trait::async_trait;
use futures::future::{self, FutureExt, TryFutureExt};
use std::cell::{RefCell, UnsafeCell};
use std::collections::{hash_map, BinaryHeap, HashMap};
use std::convert::{TryFrom, TryInto};
use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;

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
    rtype: u8,
    // payload follows
}

impl TryFrom<u8> for WALRingType {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == WALRingType::Null as u8 => Ok(WALRingType::Null),
            x if x == WALRingType::Full as u8 => Ok(WALRingType::Full),
            x if x == WALRingType::First as u8 => Ok(WALRingType::First),
            x if x == WALRingType::Middle as u8 => Ok(WALRingType::Middle),
            x if x == WALRingType::Last as u8 => Ok(WALRingType::Last),
            _ => Err(()),
        }
    }
}

type WALFileId = u64;
pub type WALBytes = Box<[u8]>;
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

pub trait Record {
    fn serialize(&self) -> WALBytes;
}

impl Record for WALBytes {
    fn serialize(&self) -> WALBytes {
        self[..].into()
    }
}

impl Record for String {
    fn serialize(&self) -> WALBytes {
        self.as_bytes().into()
    }
}

impl Record for &str {
    fn serialize(&self) -> WALBytes {
        self.as_bytes().into()
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
    next_complete: WALPos,
    io_complete: BinaryHeap<WALRingId>,
}

#[async_trait(?Send)]
pub trait WALFile {
    /// Initialize the file space in [offset, offset + length) to zero.
    async fn allocate(&self, offset: WALPos, length: usize) -> Result<(), ()>;
    /// Write data with offset. We assume all previous `allocate`/`truncate` invocations are visible
    /// if ordered earlier (should be guaranteed by most OS).  Additionally, the write caused
    /// by each invocation of this function should be _atomic_ (the entire single write should be
    /// all or nothing).
    async fn write(&self, offset: WALPos, data: WALBytes) -> Result<(), ()>;
    /// Read data with offset. Return `Ok(None)` when it reaches EOF.
    async fn read(
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
    async fn remove_file(&self, filename: String) -> Result<(), ()>;
    /// Enumerate all WAL filenames. It should include all WAL files that are previously opened
    /// (created) but not removed. The list could be unordered.
    fn enumerate_files(&self) -> Result<Self::FileNameIter, ()>;
}

struct WALFileHandle<'a, F: WALStore> {
    fid: WALFileId,
    handle: &'a dyn WALFile,
    pool: *const WALFilePool<F>,
}

impl<'a, F: WALStore> std::ops::Deref for WALFileHandle<'a, F> {
    type Target = dyn WALFile + 'a;
    fn deref(&self) -> &Self::Target {
        self.handle
    }
}

impl<'a, F: WALStore> Drop for WALFileHandle<'a, F> {
    fn drop(&mut self) {
        unsafe {
            (&*self.pool).release_file(self.fid);
        }
    }
}

/// The middle layer that manages WAL file handles and invokes public trait functions to actually
/// manipulate files and their contents.
struct WALFilePool<F: WALStore> {
    store: F,
    handle_cache: RefCell<lru::LruCache<WALFileId, Box<dyn WALFile>>>,
    handle_used:
        RefCell<HashMap<WALFileId, UnsafeCell<(Box<dyn WALFile>, usize)>>>,
    last_write:
        UnsafeCell<MaybeUninit<Pin<Box<dyn Future<Output = Result<(), ()>>>>>>,
    last_peel:
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
            handle_cache: RefCell::new(lru::LruCache::new(cache_size)),
            handle_used: RefCell::new(HashMap::new()),
            last_write: UnsafeCell::new(MaybeUninit::new(Box::pin(
                future::ready(Ok(())),
            ))),
            last_peel: UnsafeCell::new(MaybeUninit::new(Box::pin(
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

    fn get_file<'a>(
        &'a self,
        fid: u64,
        touch: bool,
    ) -> impl Future<Output = Result<WALFileHandle<'a, F>, ()>> {
        async move {
            let pool = self as *const WALFilePool<F>;
            if let Some(h) = self.handle_cache.borrow_mut().pop(&fid) {
                let handle = match self.handle_used.borrow_mut().entry(fid) {
                    hash_map::Entry::Vacant(e) => unsafe {
                        &*(*e.insert(UnsafeCell::new((h, 1))).get()).0
                    },
                    _ => unreachable!(),
                };
                Ok(WALFileHandle { fid, handle, pool })
            } else {
                let v = unsafe {
                    &mut *match self.handle_used.borrow_mut().entry(fid) {
                        hash_map::Entry::Occupied(e) => e.into_mut(),
                        hash_map::Entry::Vacant(e) => {
                            e.insert(UnsafeCell::new((
                                self.store
                                    .open_file(&Self::get_fname(fid), touch)
                                    .await?,
                                0,
                            )))
                        }
                    }
                    .get()
                };
                v.1 += 1;
                Ok(WALFileHandle {
                    fid,
                    handle: &*v.0,
                    pool,
                })
            }
        }
    }

    fn release_file(&self, fid: WALFileId) {
        match self.handle_used.borrow_mut().entry(fid) {
            hash_map::Entry::Occupied(e) => {
                let v = unsafe { &mut *e.get().get() };
                v.1 -= 1;
                if v.1 == 0 {
                    self.handle_cache
                        .borrow_mut()
                        .put(fid, e.remove().into_inner().0);
                }
            }
            _ => unreachable!(),
        }
    }

    fn get_fid(&self, fname: &str) -> WALFileId {
        scan_fmt!(fname, "{x}.log", [hex WALFileId]).unwrap()
    }

    fn write<'a>(
        &'a mut self,
        writes: Vec<(WALPos, WALBytes)>,
    ) -> Vec<Pin<Box<dyn Future<Output = Result<(), ()>> + 'a>>> {
        if writes.is_empty() {
            return Vec::new()
        }
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
                Pin<
                    Box<
                        dyn Future<Output = Result<WALFileHandle<'a, F>, ()>>
                            + 'a,
                    >,
                >,
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

    fn remove_files<'a>(
        &'a mut self,
        fid_s: u64,
        fid_e: u64,
    ) -> impl Future<Output = Result<(), ()>> + 'a {
        let last_peel = unsafe {
            std::mem::replace(
                &mut *self.last_peel.get(),
                std::mem::MaybeUninit::uninit(),
            )
            .assume_init()
        };

        let mut removes = Vec::new();
        for fid in fid_s..fid_e {
            removes.push(self.store.remove_file(Self::get_fname(fid))
                as Pin<Box<dyn Future<Output = _> + 'a>>)
        }
        let p = async move {
            last_peel.await?;
            for r in removes.into_iter() {
                r.await?
            }
            Ok(())
        }
        .shared();
        unsafe {
            (*self.last_peel.get()) =
                MaybeUninit::new(std::mem::transmute(Box::pin(p.clone())
                    as Pin<Box<dyn Future<Output = _> + 'a>>))
        }
        p
    }

    fn in_use_len(&self) -> usize {
        self.handle_used.borrow().len()
    }

    fn reset(&mut self) {
        self.handle_cache.borrow_mut().clear();
        self.handle_used.borrow_mut().clear()
    }
}

pub struct WALWriter<F: WALStore> {
    state: WALState,
    file_pool: WALFilePool<F>,
    block_buffer: WALBytes,
    block_size: u32,
    msize: usize,
}

unsafe impl<F> Send for WALWriter<F> where F: WALStore + Send {}

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
            msize,
        }
    }

    /// Submit a sequence of records to WAL. It returns a vector of futures, each of which
    /// corresponds to one record. When a future resolves to `WALRingId`, it is guaranteed the
    /// record is already logged. Then, after finalizing the changes encoded by that record to
    /// the persistent storage, the caller can recycle the WAL files by invoking the given
    /// `peel` with the given `WALRingId`s. Note: each serialized record should contain at least 1
    /// byte (empty record payload will result in assertion failure).
    pub fn grow<'a, R: Record + 'a>(
        &'a mut self,
        records: Vec<R>,
    ) -> Vec<impl Future<Output = Result<(R, WALRingId), ()>> + 'a> {
        let mut res = Vec::new();
        let mut writes = Vec::new();
        let msize = self.msize as u32;
        // the global offest of the begining of the block
        // the start of the unwritten data
        let mut bbuff_start = self.state.next as u32 & (self.block_size - 1);
        // the end of the unwritten data
        let mut bbuff_cur = bbuff_start;

        for rec in records.iter() {
            let bytes = rec.serialize();
            let mut rec = &bytes[..];
            let mut rsize = rec.len() as u32;
            let mut ring_start = None;
            assert!(rsize > 0);
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
                        blob.rtype = rt as u8;
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
                        } as u8;
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
                    break 'outer
                }
            }
            while res[i].0.start < blk_e {
                res[i].1.push(j);
                if res[i].0.end >= blk_e {
                    break
                }
                i += 1;
                if i >= res.len() {
                    break 'outer
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
            .zip(records.into_iter())
            .map(|((ringid, blks), rec)| {
                future::try_join_all(
                    blks.into_iter().map(|idx| writes[idx].clone()),
                )
                .or_else(|_| future::ready(Err(())))
                .and_then(move |_| future::ready(Ok((rec, ringid))))
            })
            .collect();
        res
    }

    /// Inform the `WALWriter` that some data writes are complete so that it could automatically
    /// remove obsolete WAL files. The given list of `WALRingId` does not need to be ordered and
    /// could be of arbitrary length.
    pub fn peel<'a, T: AsRef<[WALRingId]>>(
        &'a mut self,
        records: T,
    ) -> impl Future<Output = Result<(), ()>> + 'a {
        let msize = self.msize as u64;
        let block_size = self.block_size as u64;
        let state = &mut self.state;
        for rec in records.as_ref() {
            state.io_complete.push(*rec);
        }
        let orig_fid = state.first_fid;
        while let Some(s) =
            state.io_complete.peek().and_then(|&e| Some(e.start))
        {
            if s != state.next_complete {
                break
            }
            let mut m = state.io_complete.pop().unwrap();
            let block_remain = block_size - (m.end & (block_size - 1));
            if block_remain <= msize as u64 {
                m.end += block_remain
            }
            state.next_complete = m.end
        }
        let next_fid = state.next_complete >> state.file_nbit;
        state.first_fid = next_fid;
        self.file_pool.remove_files(orig_fid, next_fid)
    }

    pub fn file_pool_in_use(&self) -> usize {
        self.file_pool.in_use_len()
    }
}

#[derive(Copy, Clone)]
pub enum RecoverPolicy {
    /// all checksums must be correct, otherwise recovery fails
    Strict,
    /// stop recovering when hitting the first corrupted record
    BestEffort,
}

pub struct WALLoader {
    file_nbit: u8,
    block_nbit: u8,
    cache_size: usize,
    recover_policy: RecoverPolicy,
}

impl Default for WALLoader {
    fn default() -> Self {
        WALLoader {
            file_nbit: 22,  // 4MB
            block_nbit: 15, // 32KB,
            cache_size: 16,
            recover_policy: RecoverPolicy::Strict,
        }
    }
}

impl WALLoader {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn file_nbit(&mut self, v: u8) -> &mut Self {
        self.file_nbit = v;
        self
    }

    pub fn block_nbit(&mut self, v: u8) -> &mut Self {
        self.block_nbit = v;
        self
    }

    pub fn cache_size(&mut self, v: usize) -> &mut Self {
        self.cache_size = v;
        self
    }

    pub fn recover_policy(&mut self, p: RecoverPolicy) -> &mut Self {
        self.recover_policy = p;
        self
    }

    fn verify_checksum(&self, data: &[u8], checksum: u32) -> Result<bool, ()> {
        if checksum == crc::crc32::checksum_ieee(data) {
            Ok(true)
        } else {
            match self.recover_policy {
                RecoverPolicy::Strict => Err(()),
                RecoverPolicy::BestEffort => Ok(false),
            }
        }
    }

    /// Recover by reading the WAL files.
    pub async fn load<
        S: WALStore,
        F: FnMut(WALBytes, WALRingId) -> Result<(), ()>,
    >(
        &self,
        store: S,
        mut recover_func: F,
    ) -> Result<WALWriter<S>, ()> {
        let msize = std::mem::size_of::<WALRingBlob>();
        assert!(self.file_nbit > self.block_nbit);
        assert!(msize < 1 << self.block_nbit);
        let filename_fmt = regex::Regex::new(r"[0-9a-f]+\.log").unwrap();
        let mut file_pool = WALFilePool::new(
            store,
            self.file_nbit,
            self.block_nbit,
            self.cache_size,
        );
        let block_size = 1 << file_pool.block_nbit;
        let mut logfiles: Vec<String> = file_pool
            .store
            .enumerate_files()?
            .filter(|f| filename_fmt.is_match(f))
            .collect();
        // TODO: check for missing logfiles
        logfiles.sort();
        let mut chunks = None;
        let mut skip = false;
        let mut scanned_files: Vec<(String, WALFileHandle<S>)> = Vec::new();

        macro_rules! remove_scanned_files {
            () => {
                for (fname, f) in scanned_files.drain(..) {
                    f.truncate(0)?;
                    file_pool.store.remove_file(fname.to_string()).await?;
                }
            };
        }

        for fname in logfiles.into_iter() {
            let fid = file_pool.get_fid(&fname);
            let f = file_pool.get_file(fid, false).await?;
            let mut off = 0;
            if skip {
                f.truncate(0)?;
                file_pool.store.remove_file(fname).await?;
                continue
            }
            while let Some(header_raw) = f.read(off, msize as usize).await? {
                let ringid_start = (fid << file_pool.file_nbit) + off;
                off += msize as u64;
                let header = unsafe {
                    std::mem::transmute::<*const u8, &WALRingBlob>(
                        header_raw.as_ptr(),
                    )
                };
                let rsize = header.rsize;
                match header.rtype.try_into() {
                    Ok(WALRingType::Full) => {
                        assert!(chunks.is_none());
                        let payload =
                            f.read(off, rsize as usize).await?.ok_or(())?;
                        // TODO: improve the behavior when CRC32 fails
                        if !self.verify_checksum(&payload, header.crc32)? {
                            skip = true;
                            break
                        }
                        off += rsize as u64;
                        recover_func(
                            payload,
                            WALRingId {
                                start: ringid_start,
                                end: (fid << file_pool.file_nbit) + off,
                            },
                        )?;
                        remove_scanned_files!()
                    }
                    Ok(WALRingType::First) => {
                        assert!(chunks.is_none());
                        let chunk =
                            f.read(off, rsize as usize).await?.ok_or(())?;
                        if !self.verify_checksum(&chunk, header.crc32)? {
                            skip = true;
                            break
                        }
                        chunks = Some((vec![chunk], ringid_start));
                        off += rsize as u64;
                    }
                    Ok(WALRingType::Middle) => {
                        if let Some((chunks, _)) = &mut chunks {
                            let chunk =
                                f.read(off, rsize as usize).await?.ok_or(())?;
                            if !self.verify_checksum(&chunk, header.crc32)? {
                                skip = true;
                                break
                            }
                            chunks.push(chunk);
                        } // otherwise ignore the leftover
                        off += rsize as u64;
                    }
                    Ok(WALRingType::Last) => {
                        if let Some((mut chunks, ringid_start)) = chunks.take()
                        {
                            let chunk =
                                f.read(off, rsize as usize).await?.ok_or(())?;
                            off += rsize as u64;
                            if !self.verify_checksum(&chunk, header.crc32)? {
                                skip = true;
                                break
                            }
                            chunks.push(chunk);
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
                            recover_func(
                                payload.into_boxed_slice(),
                                WALRingId {
                                    start: ringid_start,
                                    end: (fid << file_pool.file_nbit) + off,
                                },
                            )?;
                            remove_scanned_files!()
                        }
                        // otherwise ignore the leftover
                        else {
                            off += rsize as u64;
                        }
                    }
                    Ok(WALRingType::Null) => {
                        remove_scanned_files!();
                        break
                    }
                    Err(_) => match self.recover_policy {
                        RecoverPolicy::Strict => return Err(()),
                        RecoverPolicy::BestEffort => {
                            skip = true;
                            break
                        }
                    },
                }
                let block_remain = block_size - (off & (block_size - 1));
                if block_remain <= msize as u64 {
                    off += block_remain;
                }
            }
            scanned_files.push((fname, f));
        }
        for (fname, f) in scanned_files.into_iter() {
            f.truncate(0)?;
            file_pool.store.remove_file(fname.to_string()).await?;
        }
        file_pool.reset();

        Ok(WALWriter::new(
            WALState {
                first_fid: 0,
                next: 0,
                file_nbit: file_pool.file_nbit,
                next_complete: 0,
                io_complete: BinaryHeap::new(),
            },
            file_pool,
        ))
    }
}
