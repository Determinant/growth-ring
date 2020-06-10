#[macro_use] extern crate scan_fmt;
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

#[repr(C)]
struct WALRingBlob {
    crc32: u32,
    rsize: u32,
    rtype: WALRingType,
    // payload follows
}

type WALBytes = Box<[u8]>;
type WALFileId = u64;
type WALPos = u64;

#[derive(Eq, PartialEq, Copy, Clone)]
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
    pub first_fid: WALFileId,
    /// the next position for a record, addressed in the entire WAL space
    pub next: WALPos,
    /// number of bits for a block
    pub block_nbit: u64,
    /// number of bits for a file
    pub file_nbit: u64,
}

pub trait WALFile {
    /// Initialize the file space in [offset, offset + length) to zero.
    fn allocate(&self, offset: WALPos, length: usize);
    /// Write data with offset.
    fn write(&self, offset: WALPos, data: WALBytes);
    /// Read data with offset.
    fn read(&self, offset: WALPos, length: usize) -> WALBytes;
}

pub trait WALStore {
    /// Open a file given the filename, create the file if not exists when `touch` is `true`.
    fn open_file(&self, filename: &str, touch: bool) -> Option<Box<dyn WALFile>>;
    /// Unlink a file given the filename.
    fn remove_file(&self, filename: &str) -> bool;
    /// Enumerate all WAL files, ordered by their filenames.
    fn enumerate_files(&self) -> Box<[String]>;
    /// Apply (redo) the payload during recovery.
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
        scan_fmt!(fname, "{:x}.log", [hex WALFileId]).unwrap()
    }

    // TODO: evict stale handles
    fn write(&mut self, writes: Vec<(WALPos, WALBytes)>) {
        // pre-allocate the file space
        let fid = writes[0].0 >> self.file_nbit;
        let mut alloc_start = writes[0].0 & (self.file_size - 1);
        let mut alloc_end = alloc_start + writes[0].1.len() as u64;
        let mut h = self.get_file(fid, true);
        for (off, w) in &writes[1..] {
            let next_fid = off >> self.file_nbit;
            if next_fid != fid {
                h.allocate(alloc_start, (alloc_end - alloc_start) as usize);
                h = self.get_file(next_fid, true);
                alloc_start = 0;
                alloc_end = alloc_start + w.len() as u64;
            } else {
                alloc_end += w.len() as u64;
            }
        }
        h.allocate(alloc_start, (alloc_end - alloc_start) as usize);
        for (off, w) in writes.into_iter() {
            self.get_file(off >> self.file_nbit, true).write(off, w);
        }
    }

    fn remove_file(&self, fid: u64) -> bool {
        self.store.remove_file(&Self::get_fname(fid))
    }
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
    pub fn grow(&mut self, records: &[WALBytes]) -> Box<[WALRingId]> {
        let mut res = Vec::new();
        let mut writes = Vec::new();
        let msize = std::mem::size_of::<WALRingBlob>() as u32;
        // the global offest of the begining of the block
        // the start of the unwritten data
        let mut bbuff_start = self.state.next as u32 & (self.block_size - 1);
        // the end of the unwritten data
        let mut bbuff_cur = bbuff_start;

        for _rec in records {
            let mut rec = &_rec[..];
            let mut rsize = rec.len() as u32;
            let mut started = false;
            while rsize > 0 {
                let remain = self.block_size - bbuff_cur;
                if remain > msize {
                    let d = remain - msize;
                    let blob = unsafe {std::mem::transmute::<*mut u8, &mut WALRingBlob>(
                        (&mut self.block_buffer[bbuff_cur as usize..]).as_mut_ptr())};
                    let ring_start = self.state.next + (bbuff_cur - bbuff_start) as u64;
                    if d >= rsize {
                        // the remaining rec fits in the block
                        let payload = rec;
                        blob.crc32 = crc::crc32::checksum_ieee(payload);
                        blob.rsize = rsize;
                        blob.rtype = if started {WALRingType::Last} else {WALRingType::Full};
                        rsize = 0;
                        &mut self.block_buffer[bbuff_cur as usize..].copy_from_slice(payload);
                        bbuff_cur += rsize;
                    } else {
                        // the remaining block can only accommodate partial rec
                        let payload = &rec[..d as usize];
                        blob.crc32 = crc::crc32::checksum_ieee(payload);
                        blob.rsize = d;
                        blob.rtype = if started {WALRingType::Middle} else {
                            started = true;
                            WALRingType::First
                        };
                        rsize -= d;
                        &mut self.block_buffer[bbuff_cur as usize..].copy_from_slice(payload);
                        bbuff_cur += d;
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
    pub fn peel(&mut self, records: &[WALRingId]) {
        for rec in records {
            self.io_complete.push(*rec)
        }
        let orig_fid = self.state.first_fid;
        while let Some(s) = self.io_complete.peek().and_then(|&e| Some(e.start)) {
            if s != self.next_complete {
                break
            }
            let m = self.io_complete.pop().unwrap();
            self.next_complete = m.end
        }
        let next_fid = self.next_complete >> self.state.file_nbit;
        for fid in orig_fid..next_fid {
            self.file_pool.remove_file(fid);
        }
        self.state.first_fid = next_fid;
    }
}

pub struct WALReader<F: WALStore> {
    file_pool: WALFilePool<F>,
}

impl<F: WALStore> WALReader<F> {
    pub fn new(store: F, file_nbit: u8, block_nbit: u8, cache_size: usize) -> Self {
        let file_pool = WALFilePool::new(store, file_nbit, block_nbit, cache_size);
        WALReader{ file_pool }
    }

    pub fn recover(mut self) -> WALWriter<F> {
        let msize = std::mem::size_of::<WALRingBlob>() as u32;
        let logfiles = self.file_pool.store.enumerate_files();
        for fname in logfiles.iter() {
            let fid = self.file_pool.get_fid(fname);
            let f = self.file_pool.get_file(fid, false);
            let mut off = 0;
            let mut end = false;
            while !end {
                let header_raw = f.read(off, msize as usize);
                let header = unsafe {
                    std::mem::transmute::<*const u8, &WALRingBlob>(header_raw.as_ptr())};
                let rsize = header.rsize;
                off += msize as u64;
                match header.rtype {
                    WALRingType::Full => {
                        let payload = f.read(off, rsize as usize);
                        self.file_pool.store.apply_payload(payload);
                        off += rsize as u64;
                    },
                    WALRingType::First => {
                        let mut chunks = vec![f.read(off, rsize as usize)];
                        off += rsize as u64;
                        loop {
                            let header_raw = f.read(off, msize as usize);
                            let header = unsafe {
                                std::mem::transmute::<*const u8, &WALRingBlob>(header_raw.as_ptr())};
                            if let WALRingType::Null = header.rtype {
                                end = true;
                                break
                            }
                            let rsize = header.rsize;
                            let payload = f.read(off, rsize as usize);
                            off += msize as u64;
                            chunks.push(payload);
                            match header.rtype {
                                WALRingType::Middle => (),
                                WALRingType::Last => break,
                                _ => unreachable!()
                            }
                        }
                        let mut payload = Vec::new();
                        payload.resize(chunks.iter().fold(0, |acc, v| acc + v.len()), 0);
                        let mut ps = &mut payload[..];
                        for c in chunks {
                            ps[..c.len()].copy_from_slice(&*c);
                            ps = &mut ps[c.len()..];
                        }
                        self.file_pool.store.apply_payload(payload.into_boxed_slice());
                    },
                    WALRingType::Null => end = true,
                    _ => unreachable!()
                }
            }
            self.file_pool.remove_file(fid);
        }
        WALWriter::new(WALState {
            first_fid: 0,
            next: 0,
            block_nbit: self.file_pool.block_nbit,
            file_nbit: self.file_pool.file_nbit,
        }, self.file_pool)
    }
}
