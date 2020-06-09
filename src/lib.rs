#[repr(u8)]
enum WALRingType {
    Full,
    First,
    Middle,
    Last
}

struct WALRingBlob {
    crc32: u32,
    rsize: u32,
    rtype: WALRingType,
    // payload follows
}

type WALFileId = u32;
type WALPos = u64;
type WALWrite = (WALPos, Box<[u8]>);

pub struct WALState {
    pub base: WALPos,
    pub last: WALPos,
    pub block_nbit: u8,
    pub file_nbit: u8,
}

pub struct WALWriter {
    state: WALState,
    block_buffer: Box<[u8]>,
    block_size: u32,
    file_size: u64,
}

impl WALWriter {
    pub fn new(state: WALState) -> Self {
        let mut b = Vec::new();
        let block_size = 1 << (state.block_nbit as u32);
        let file_size = 1 << (state.file_nbit as u64);
        b.resize(block_size as usize, 0);
        WALWriter{
            state,
            block_buffer: b.into_boxed_slice(),
            block_size,
            file_size,
        }
    }

    pub fn grow(&mut self, records: &[Box<[u8]>]) -> Vec<WALWrite> {
        let mut res = Vec::new();
        let msize = std::mem::size_of::<WALRingBlob>() as u32;
        // the global offest of the begining of the block
        // the start of the unwritten data
        let mut bbuff_start = self.state.last as u32 & (self.block_size - 1);
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
                        &mut self.block_buffer[bbuff_cur as usize] as *mut u8)};
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
                } else {
                    // add padding space by moving the point to the end of the block
                    bbuff_cur = self.block_size;
                }
                if bbuff_cur == self.block_size {
                    res.push((self.state.last,
                             self.block_buffer[bbuff_start as usize..]
                                .to_vec().into_boxed_slice()));
                    self.state.last += (self.block_size - bbuff_start) as u64;
                    bbuff_start = 0;
                    bbuff_cur = 0;
                }
            }
        }
        if bbuff_cur > bbuff_start {
            res.push((self.state.last,
                     self.block_buffer[bbuff_start as usize..bbuff_cur as usize]
                        .to_vec().into_boxed_slice()));
            self.state.last += (bbuff_cur - bbuff_start) as u64;
        }
        res
    }
}

struct WALReader {
}
