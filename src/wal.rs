use std::collections::BinaryHeap;

#[repr(u8)]
enum WALRingType {
    #[allow(dead_code)]
    Null = 0x0,
    Full,
    First,
    Middle,
    Last
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

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub struct WALRingId {
    start: WALPos,
    end: WALPos
}

impl Ord for WALRingId {
    fn cmp(&self, other: &WALRingId) -> std::cmp::Ordering {
        other.start.cmp(&self.start).then_with(|| other.end.cmp(&self.end))
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

pub trait WALFile {
    /// Initialize the file space in [offset, offset + length) to zero.
    fn allocate(&self, offset: WALPos, length: usize) -> Result<(), ()>;
    /// Truncate a file to a specified length.
    fn truncate(&self, length: usize) -> Result<(), ()>;
    /// Write data with offset. We assume the actual writes on the storage medium are _strictly
    /// ordered_ the same way as this callback is invoked. We also assume all previous
    /// `allocate/truncate` invocation should be visible if ordered earlier (should be guaranteed
    /// by most OS).  Additionally, the final write caused by each invocation of this function
    /// should be _atomic_ (the entire single write should be all or nothing).
    fn write(&self, offset: WALPos, data: WALBytes);
    /// Read data with offset.
    fn read(&self, offset: WALPos, length: usize) -> WALBytes;
}

pub trait WALStore {
    type FileNameIter: Iterator<Item = String>;

    /// Open a file given the filename, create the file if not exists when `touch` is `true`.
    fn open_file(&self, filename: &str, touch: bool) -> Option<Box<dyn WALFile>>;
    /// Unlink a file given the filename.
    fn remove_file(&self, filename: &str) -> Result<(), ()>;
    /// Enumerate all WAL filenames. It should include all WAL files that are previously opened
    /// (created) but not removed. The list could be unordered.
    fn enumerate_files(&self) -> Self::FileNameIter;
    /// Apply the payload during recovery. This notifies the application should redo the given
    /// operation to ensure its state is consistent (the major goal of having a WAL). We assume
    /// the application applies the payload by the _order_ of this callback invocation.
    fn apply_payload(&self, payload: WALBytes);
}

/// The middle layer that manages WAL file handles and invokes public trait functions to actually
/// manipulate files and their contents.
struct WALFilePool<F: WALStore> {
    store: F,
    handles: lru::LruCache<WALFileId, Box<dyn WALFile>>,
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
            handles: lru::LruCache::new(cache_size),
            file_nbit,
            file_size: 1 << (file_nbit as u64),
            block_nbit,
        }
    }

    fn get_fname(fid: WALFileId) -> String {
        format!("{:08x}.log", fid)
    }

    fn get_file(&mut self, fid: u64, touch: bool) -> &'static dyn WALFile {
        let h = match self.handles.get(&fid) {
            Some(h) => &**h,
            None => {
                self.handles.put(fid, self.store.open_file(&Self::get_fname(fid), touch).unwrap());
                &**self.handles.get(&fid).unwrap()
            }
        };
        unsafe {&*(h as *const dyn WALFile)}
    }

    fn get_fid(&mut self, fname: &str) -> WALFileId {
        scan_fmt!(fname, "{x}.log", [hex WALFileId]).unwrap()
    }

    // TODO: evict stale handles
    fn write(&mut self, writes: Vec<(WALPos, WALBytes)>) {
        // pre-allocate the file space
        let mut fid = writes[0].0 >> self.file_nbit;
        let mut alloc_start = writes[0].0 & (self.file_size - 1);
        let mut alloc_end = alloc_start + writes[0].1.len() as u64;
        let mut h = self.get_file(fid, true);
        for (off, w) in &writes[1..] {
            let next_fid = off >> self.file_nbit;
            if next_fid != fid {
                h.allocate(alloc_start, (alloc_end - alloc_start) as usize).unwrap();
                h = self.get_file(next_fid, true);
                alloc_start = 0;
                alloc_end = alloc_start + w.len() as u64;
                fid = next_fid;
            } else {
                alloc_end += w.len() as u64;
            }
        }
        h.allocate(alloc_start, (alloc_end - alloc_start) as usize).unwrap();
        for (off, w) in writes.into_iter() {
            self.get_file(off >> self.file_nbit, true).write(off & (self.file_size - 1), w);
        }
    }

    fn remove_file(&self, fid: u64) -> Result<(), ()> {
        self.store.remove_file(&Self::get_fname(fid))
    }

    fn reset(&mut self) { self.handles.clear() }
}

pub struct WALWriter<F: WALStore> {
    state: WALState,
    file_pool: WALFilePool<F>,
    block_buffer: WALBytes,
    block_size: u32,
    next_complete: WALPos,
    io_complete: BinaryHeap<WALRingId>
}

impl<F: WALStore> WALWriter<F> {
    fn new(state: WALState, file_pool: WALFilePool<F>) -> Self {
        let mut b = Vec::new();
        let block_size = 1 << file_pool.block_nbit as u32;
        //let block_nbit = state.block_nbit;
        //let block_size = 1 << (block_nbit as u32);
        //let file_nbit = state.file_nbit;
        //let file_size = 1 << (file_nbit as u64);
        b.resize(block_size as usize, 0);
        WALWriter{
            state,
            file_pool,
            block_buffer: b.into_boxed_slice(),
            block_size,
            next_complete: 0,
            io_complete: BinaryHeap::new(),
        }
    }

    /// Submit a sequence of records to WAL; WALStore/WALFile callbacks are invoked before the
    /// function returns.  The caller then has the knowledge of WAL writes so it should defer
    /// actual data writes after WAL writes.
    pub fn grow<T: AsRef<[WALBytes]>>(&mut self, records: T) -> Box<[WALRingId]> {
        let mut res = Vec::new();
        let mut writes = Vec::new();
        let msize = std::mem::size_of::<WALRingBlob>() as u32;
        // the global offest of the begining of the block
        // the start of the unwritten data
        let mut bbuff_start = self.state.next as u32 & (self.block_size - 1);
        // the end of the unwritten data
        let mut bbuff_cur = bbuff_start;

        for _rec in records.as_ref() {
            let mut rec = &_rec[..];
            let mut rsize = rec.len() as u32;
            let mut started = false;
            while rsize > 0 {
                let remain = self.block_size - bbuff_cur;
                if remain > msize {
                    let d = remain - msize;
                    let ring_start = self.state.next + (bbuff_cur - bbuff_start) as u64;
                    let blob = unsafe {std::mem::transmute::<*mut u8, &mut WALRingBlob>(
                        (&mut self.block_buffer[bbuff_cur as usize..]).as_mut_ptr())};
                    bbuff_cur += msize;
                    if d >= rsize {
                        // the remaining rec fits in the block
                        let payload = rec;
                        blob.crc32 = crc::crc32::checksum_ieee(payload);
                        blob.rsize = rsize;
                        blob.rtype = if started {WALRingType::Last} else {WALRingType::Full};
                        &mut self.block_buffer[
                            bbuff_cur as usize..
                            bbuff_cur as usize + payload.len()].copy_from_slice(payload);
                        bbuff_cur += rsize;
                        rsize = 0;
                    } else {
                        // the remaining block can only accommodate partial rec
                        let payload = &rec[..d as usize];
                        blob.crc32 = crc::crc32::checksum_ieee(payload);
                        blob.rsize = d;
                        blob.rtype = if started {WALRingType::Middle} else {
                            started = true;
                            WALRingType::First
                        };
                        &mut self.block_buffer[
                            bbuff_cur as usize..
                            bbuff_cur as usize + payload.len()].copy_from_slice(payload);
                        bbuff_cur += d;
                        rsize -= d;
                        rec = &rec[d as usize..];
                    }
                    let ring_end = self.state.next + (bbuff_cur - bbuff_start) as u64;
                    res.push(WALRingId{start: ring_start, end: ring_end});
                } else {
                    // add padding space by moving the point to the end of the block
                    bbuff_cur = self.block_size;
                }
                if bbuff_cur == self.block_size {
                    writes.push((self.state.next,
                             self.block_buffer[bbuff_start as usize..]
                                .to_vec().into_boxed_slice()));
                    self.state.next += (self.block_size - bbuff_start) as u64;
                    bbuff_start = 0;
                    bbuff_cur = 0;
                }
            }
        }
        if bbuff_cur > bbuff_start {
            writes.push((self.state.next,
                     self.block_buffer[bbuff_start as usize..bbuff_cur as usize]
                        .to_vec().into_boxed_slice()));
            self.state.next += (bbuff_cur - bbuff_start) as u64;
        }
        self.file_pool.write(writes);
        res.into_boxed_slice()
    }

    /// Inform the WALWriter that data writes (specified by a slice of (offset, length) tuples) are
    /// complete so that it could automatically remove obsolete WAL files.
    pub fn peel<T: AsRef<[WALRingId]>>(&mut self, records: T) {
        let msize = std::mem::size_of::<WALRingBlob>() as u64;
        let block_size = self.block_size as u64;
        for rec in records.as_ref() {
            self.io_complete.push(*rec)
        }
        let orig_fid = self.state.first_fid;
        while let Some(s) = self.io_complete.peek().and_then(|&e| Some(e.start)) {
            if s != self.next_complete {
                break
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
            self.file_pool.remove_file(fid).unwrap();
        }
        self.state.first_fid = next_fid;
    }
}

pub struct WALLoader<F: WALStore> {
    file_pool: WALFilePool<F>,
    filename_fmt: regex::Regex
}

impl<F: WALStore> WALLoader<F> {
    pub fn new(store: F, file_nbit: u8, block_nbit: u8, cache_size: usize) -> Self {
        let file_pool = WALFilePool::new(store, file_nbit, block_nbit, cache_size);
        let filename_fmt = regex::Regex::new(r"[0-9a-f]+\.log").unwrap();
        WALLoader{ file_pool, filename_fmt }
    }

    pub fn recover(mut self) -> WALWriter<F> {
        let block_size = 1 << self.file_pool.block_nbit;
        let msize = std::mem::size_of::<WALRingBlob>() as u32;
        let mut logfiles: Vec<String> = self.file_pool.store
            .enumerate_files()
            .filter(|f| self.filename_fmt.is_match(f)).collect();
        // TODO: check for missing logfiles
        logfiles.sort();
        let mut chunks = None;
        for fname in logfiles.iter() {
            let fid = self.file_pool.get_fid(fname);
            let f = self.file_pool.get_file(fid, false);
            let mut off = 0;
            while block_size - (off & (block_size - 1)) > msize as u64 {
                let header_raw = f.read(off, msize as usize);
                off += msize as u64;
                let header = unsafe {
                    std::mem::transmute::<*const u8, &WALRingBlob>(header_raw.as_ptr())};
                let rsize = header.rsize;
                match header.rtype {
                    WALRingType::Full => {
                        assert!(chunks.is_none());
                        let payload = f.read(off, rsize as usize);
                        off += rsize as u64;
                        self.file_pool.store.apply_payload(payload);
                    },
                    WALRingType::First => {
                        assert!(chunks.is_none());
                        chunks = Some(vec![f.read(off, rsize as usize)]);
                        off += rsize as u64;
                    },
                    WALRingType::Middle => {
                        chunks.as_mut().unwrap().push(f.read(off, rsize as usize));
                        off += rsize as u64;
                    },
                    WALRingType::Last => {
                        chunks.as_mut().unwrap().push(f.read(off, rsize as usize));
                        off += rsize as u64;

                        let _chunks = chunks.take().unwrap();
                        let mut payload = Vec::new();
                        payload.resize(_chunks.iter().fold(0, |acc, v| acc + v.len()), 0);
                        let mut ps = &mut payload[..];
                        for c in _chunks {
                            ps[..c.len()].copy_from_slice(&*c);
                            ps = &mut ps[c.len()..];
                        }
                        self.file_pool.store.apply_payload(payload.into_boxed_slice());
                    },
                    WALRingType::Null => break,
                }
            }
            f.truncate(0).unwrap();
            self.file_pool.remove_file(fid).unwrap();
        }
        self.file_pool.reset();
        WALWriter::new(WALState {
            first_fid: 0,
            next: 0,
            file_nbit: self.file_pool.file_nbit,
        }, self.file_pool)
    }
}