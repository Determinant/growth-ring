#[cfg(test)]

#[allow(dead_code)]
use async_trait::async_trait;
use growthring::wal::{
    WALBytes, WALFile, WALLoader, WALPos, WALRingId, WALStore,
};
use indexmap::{map::Entry, IndexMap};
use rand::Rng;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::collections::{hash_map, HashMap};
use std::convert::TryInto;
use std::rc::Rc;

pub trait FailGen {
    fn next_fail(&self) -> bool;
}

struct FileContentEmul(RefCell<Vec<u8>>);

impl FileContentEmul {
    pub fn new() -> Self {
        FileContentEmul(RefCell::new(Vec::new()))
    }
}

impl std::ops::Deref for FileContentEmul {
    type Target = RefCell<Vec<u8>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Emulate the a virtual file handle.
pub struct WALFileEmul<G: FailGen> {
    file: Rc<FileContentEmul>,
    fgen: Rc<G>,
}

#[async_trait(?Send)]
impl<G: FailGen> WALFile for WALFileEmul<G> {
    async fn allocate(&self, offset: WALPos, length: usize) -> Result<(), ()> {
        if self.fgen.next_fail() {
            return Err(());
        }
        let offset = offset as usize;
        if offset + length > self.file.borrow().len() {
            self.file.borrow_mut().resize(offset + length, 0)
        }
        for v in &mut self.file.borrow_mut()[offset..offset + length] {
            *v = 0
        }
        Ok(())
    }

    fn truncate(&self, length: usize) -> Result<(), ()> {
        if self.fgen.next_fail() {
            return Err(());
        }
        self.file.borrow_mut().resize(length, 0);
        Ok(())
    }

    async fn write(&self, offset: WALPos, data: WALBytes) -> Result<(), ()> {
        if self.fgen.next_fail() {
            return Err(());
        }
        let offset = offset as usize;
        &self.file.borrow_mut()[offset..offset + data.len()]
            .copy_from_slice(&data);
        Ok(())
    }

    fn read(
        &self,
        offset: WALPos,
        length: usize,
    ) -> Result<Option<WALBytes>, ()> {
        if self.fgen.next_fail() {
            return Err(());
        }

        let offset = offset as usize;
        let file = self.file.borrow();
        if offset + length > file.len() {
            Ok(None)
        } else {
            Ok(Some(
                (&file[offset..offset + length]).to_vec().into_boxed_slice(),
            ))
        }
    }
}

pub struct WALStoreEmulState {
    files: HashMap<String, Rc<FileContentEmul>>,
}

impl WALStoreEmulState {
    pub fn new() -> Self {
        WALStoreEmulState {
            files: HashMap::new(),
        }
    }
    pub fn clone(&self) -> Self {
        WALStoreEmulState {
            files: self.files.clone(),
        }
    }
}

/// Emulate the persistent storage state.
pub struct WALStoreEmul<'a, G, F>
where
    G: FailGen,
    F: FnMut(WALBytes, WALRingId),
{
    state: RefCell<&'a mut WALStoreEmulState>,
    fgen: Rc<G>,
    recover: RefCell<F>,
}

impl<'a, G: FailGen, F: FnMut(WALBytes, WALRingId)> WALStoreEmul<'a, G, F> {
    pub fn new(
        state: &'a mut WALStoreEmulState,
        fgen: Rc<G>,
        recover: F,
    ) -> Self {
        let state = RefCell::new(state);
        let recover = RefCell::new(recover);
        WALStoreEmul {
            state,
            fgen,
            recover,
        }
    }
}

#[async_trait(?Send)]
impl<'a, G, F> WALStore for WALStoreEmul<'a, G, F>
where
    G: 'static + FailGen,
    F: FnMut(WALBytes, WALRingId),
{
    type FileNameIter = std::vec::IntoIter<String>;

    async fn open_file(
        &self,
        filename: &str,
        touch: bool,
    ) -> Result<Box<dyn WALFile>, ()> {
        if self.fgen.next_fail() {
            return Err(());
        }
        match self.state.borrow_mut().files.entry(filename.to_string()) {
            hash_map::Entry::Occupied(e) => Ok(Box::new(WALFileEmul {
                file: e.get().clone(),
                fgen: self.fgen.clone(),
            })),
            hash_map::Entry::Vacant(e) => {
                if touch {
                    Ok(Box::new(WALFileEmul {
                        file: e.insert(Rc::new(FileContentEmul::new())).clone(),
                        fgen: self.fgen.clone(),
                    }))
                } else {
                    Err(())
                }
            }
        }
    }

    async fn remove_file(&self, filename: String) -> Result<(), ()> {
        //println!("remove_file(filename={})", filename);
        if self.fgen.next_fail() {
            return Err(());
        }
        self.state
            .borrow_mut()
            .files
            .remove(&filename)
            .ok_or(())
            .and_then(|_| Ok(()))
    }

    fn enumerate_files(&self) -> Result<Self::FileNameIter, ()> {
        if self.fgen.next_fail() {
            return Err(());
        }
        let mut logfiles = Vec::new();
        for (fname, _) in self.state.borrow().files.iter() {
            logfiles.push(fname.clone())
        }
        Ok(logfiles.into_iter())
    }

    fn apply_payload(
        &self,
        payload: WALBytes,
        ringid: WALRingId,
    ) -> Result<(), ()> {
        if self.fgen.next_fail() {
            return Err(());
        }
        /*
        println!("apply_payload(payload=0x{}, ringid={:?})",
                hex::encode(&payload),
                ringid);
        */
        (&mut *self.recover.borrow_mut())(payload, ringid);
        Ok(())
    }
}

pub struct SingleFailGen {
    cnt: std::cell::Cell<usize>,
    fail_point: usize,
}

impl SingleFailGen {
    pub fn new(fail_point: usize) -> Self {
        SingleFailGen {
            cnt: std::cell::Cell::new(0),
            fail_point,
        }
    }
}

impl FailGen for SingleFailGen {
    fn next_fail(&self) -> bool {
        let c = self.cnt.get();
        self.cnt.set(c + 1);
        c == self.fail_point
    }
}

pub struct ZeroFailGen;

impl FailGen for ZeroFailGen {
    fn next_fail(&self) -> bool {
        false
    }
}

pub struct CountFailGen(std::cell::Cell<usize>);

impl CountFailGen {
    pub fn new() -> Self {
        CountFailGen(std::cell::Cell::new(0))
    }
    pub fn get_count(&self) -> usize {
        self.0.get()
    }
}

impl FailGen for CountFailGen {
    fn next_fail(&self) -> bool {
        self.0.set(self.0.get() + 1);
        false
    }
}

/// An ordered list of intervals: `(begin, end, color)*`.
#[derive(Clone)]
pub struct PaintStrokes(Vec<(u32, u32, u32)>);

impl PaintStrokes {
    pub fn new() -> Self {
        PaintStrokes(Vec::new())
    }

    pub fn to_bytes(&self) -> WALBytes {
        let mut res: Vec<u8> = Vec::new();
        let is = std::mem::size_of::<u32>();
        let len = self.0.len() as u32;
        res.resize(is * (1 + 3 * self.0.len()), 0);
        let mut rs = &mut res[..];
        &mut rs[..is].copy_from_slice(&len.to_le_bytes());
        rs = &mut rs[is..];
        for (s, e, c) in self.0.iter() {
            &mut rs[..is].copy_from_slice(&s.to_le_bytes());
            &mut rs[is..is * 2].copy_from_slice(&e.to_le_bytes());
            &mut rs[is * 2..is * 3].copy_from_slice(&c.to_le_bytes());
            rs = &mut rs[is * 3..];
        }
        res.into_boxed_slice()
    }

    pub fn from_bytes(raw: &[u8]) -> Self {
        assert!(raw.len() > 4);
        assert!(raw.len() & 3 == 0);
        let is = std::mem::size_of::<u32>();
        let (len_raw, mut rest) = raw.split_at(is);
        let len = u32::from_le_bytes(len_raw.try_into().unwrap());
        let mut res = Vec::new();
        for _ in 0..len {
            let (s_raw, rest1) = rest.split_at(is);
            let (e_raw, rest2) = rest1.split_at(is);
            let (c_raw, rest3) = rest2.split_at(is);
            res.push((
                u32::from_le_bytes(s_raw.try_into().unwrap()),
                u32::from_le_bytes(e_raw.try_into().unwrap()),
                u32::from_le_bytes(c_raw.try_into().unwrap()),
            ));
            rest = rest3
        }
        PaintStrokes(res)
    }

    pub fn gen_rand<R: rand::Rng>(
        max_pos: u32,
        max_len: u32,
        max_col: u32,
        n: usize,
        rng: &mut R,
    ) -> PaintStrokes {
        assert!(max_pos > 0);
        let mut strokes = Self::new();
        for _ in 0..n {
            let pos = rng.gen_range(0, max_pos);
            let len =
                rng.gen_range(1, std::cmp::min(max_len, max_pos - pos + 1));
            strokes.stroke(pos, pos + len, rng.gen_range(0, max_col))
        }
        strokes
    }

    pub fn stroke(&mut self, start: u32, end: u32, color: u32) {
        self.0.push((start, end, color))
    }

    pub fn into_vec(self) -> Vec<(u32, u32, u32)> {
        self.0
    }
}

impl growthring::wal::Record for PaintStrokes {
    fn serialize(&self) -> WALBytes { self.to_bytes() }
}

#[test]
fn test_paint_strokes() {
    let mut p = PaintStrokes::new();
    for i in 0..3 {
        p.stroke(i, i + 3, i + 10)
    }
    let pr = p.to_bytes();
    for ((s, e, c), i) in PaintStrokes::from_bytes(&pr)
        .into_vec()
        .into_iter()
        .zip(0..)
    {
        assert_eq!(s, i);
        assert_eq!(e, i + 3);
        assert_eq!(c, i + 10);
    }
}

pub struct Canvas {
    waiting: HashMap<WALRingId, usize>,
    queue: IndexMap<u32, VecDeque<(u32, WALRingId)>>,
    canvas: Box<[u32]>,
}

impl Canvas {
    pub fn new(size: usize) -> Self {
        let mut canvas = Vec::new();
        // fill the backgroudn color 0
        canvas.resize(size, 0);
        let canvas = canvas.into_boxed_slice();
        Canvas {
            waiting: HashMap::new(),
            queue: IndexMap::new(),
            canvas,
        }
    }

    pub fn new_reference(&self, ops: &[PaintStrokes]) -> Self {
        let mut res = Self::new(self.canvas.len());
        for op in ops {
            for (s, e, c) in op.0.iter() {
                for i in *s..*e {
                    res.canvas[i as usize] = *c
                }
            }
        }
        res
    }

    fn get_waiting(&mut self, rid: WALRingId) -> &mut usize {
        match self.waiting.entry(rid) {
            hash_map::Entry::Occupied(e) => e.into_mut(),
            hash_map::Entry::Vacant(e) => e.insert(0),
        }
    }

    fn get_queued(&mut self, pos: u32) -> &mut VecDeque<(u32, WALRingId)> {
        match self.queue.entry(pos) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(VecDeque::new()),
        }
    }

    pub fn prepaint(&mut self, strokes: &PaintStrokes, rid: &WALRingId) {
        let rid = *rid;
        let mut nwait = 0;
        for (s, e, c) in strokes.0.iter() {
            for i in *s..*e {
                nwait += 1;
                self.get_queued(i).push_back((*c, rid))
            }
        }
        *self.get_waiting(rid) = nwait
    }

    // TODO: allow customized scheduler
    /// Schedule to paint one position, randomly. It optionally returns a finished batch write
    /// identified by its start position of WALRingId.
    pub fn rand_paint<R: rand::Rng>(
        &mut self,
        rng: &mut R,
    ) -> Option<(Option<WALRingId>, u32)> {
        if self.is_empty() {
            return None;
        }
        let idx = rng.gen_range(0, self.queue.len());
        let (pos, _) = self.queue.get_index(idx).unwrap();
        let pos = *pos;
        Some((self.paint(pos), pos))
    }

    pub fn clear_queued(&mut self) {
        self.queue.clear();
        self.waiting.clear();
    }

    pub fn paint_all(&mut self) {
        for (pos, q) in self.queue.iter() {
            self.canvas[*pos as usize] = q.back().unwrap().0;
        }
        self.clear_queued()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn paint(&mut self, pos: u32) -> Option<WALRingId> {
        let q = self.queue.get_mut(&pos).unwrap();
        let (c, rid) = q.pop_front().unwrap();
        if q.is_empty() {
            self.queue.remove(&pos);
        }
        self.canvas[pos as usize] = c;
        if let Some(cnt) = self.waiting.get_mut(&rid) {
            *cnt -= 1;
            if *cnt == 0 {
                self.waiting.remove(&rid);
                Some(rid)
            } else {
                None
            }
        } else { None }
    }

    pub fn is_same(&self, other: &Canvas) -> bool {
        self.canvas.cmp(&other.canvas) == std::cmp::Ordering::Equal
    }

    pub fn print(&self, max_col: usize) {
        println!("# begin canvas");
        for r in self.canvas.chunks(max_col) {
            for c in r.iter() {
                print!("{:02x} ", c & 0xff);
            }
            println!("");
        }
        println!("# end canvas");
    }
}

#[test]
fn test_canvas() {
    let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(42);
    let mut canvas1 = Canvas::new(100);
    let mut canvas2 = Canvas::new(100);
    let canvas3 = Canvas::new(101);
    let dummy = WALRingId::empty_id();
    let s1 = PaintStrokes::gen_rand(100, 10, 256, 2, &mut rng);
    let s2 = PaintStrokes::gen_rand(100, 10, 256, 2, &mut rng);
    assert!(canvas1.is_same(&canvas2));
    assert!(!canvas2.is_same(&canvas3));
    canvas1.prepaint(&s1, &dummy);
    canvas1.prepaint(&s2, &dummy);
    canvas2.prepaint(&s1, &dummy);
    canvas2.prepaint(&s2, &dummy);
    assert!(canvas1.is_same(&canvas2));
    canvas1.rand_paint(&mut rng);
    assert!(!canvas1.is_same(&canvas2));
    while let Some(_) = canvas1.rand_paint(&mut rng) {}
    while let Some(_) = canvas2.rand_paint(&mut rng) {}
    assert!(canvas1.is_same(&canvas2));
    canvas1.print(10);
}

pub struct PaintingSim {
    pub block_nbit: u8,
    pub file_nbit: u8,
    pub file_cache: usize,
    /// number of PaintStrokes (WriteBatch)
    pub n: usize,
    /// number of strokes per PaintStrokes
    pub m: usize,
    /// number of scheduled ticks per PaintStroke submission
    pub k: usize,
    /// the size of canvas
    pub csize: usize,
    /// max length of a single stroke
    pub stroke_max_len: u32,
    /// max color value
    pub stroke_max_col: u32,
    /// max number of strokes per PaintStroke
    pub stroke_max_n: usize,
    /// random seed
    pub seed: u64,
}

impl PaintingSim {
    pub fn run<G: 'static + FailGen>(
        &self,
        state: &mut WALStoreEmulState,
        canvas: &mut Canvas,
        wal: WALLoader,
        ops: &mut Vec<PaintStrokes>,
        ringid_map: &mut HashMap<WALRingId, usize>,
        fgen: Rc<G>,
    ) -> Result<(), ()> {
        let mut rng =
            <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(self.seed);
        let mut wal =
            wal.recover(WALStoreEmul::new(state, fgen.clone(), |_, _| {}))?;
        for _ in 0..self.n {
            let pss = (0..self.m)
                .map(|_| {
                    PaintStrokes::gen_rand(
                        self.csize as u32,
                        self.stroke_max_len,
                        self.stroke_max_col,
                        rng.gen_range(1, self.stroke_max_n + 1),
                        &mut rng,
                    )
                })
                .collect::<Vec<PaintStrokes>>();
            let pss_ = pss.clone();
            // write ahead
            let rids = wal.grow(pss);
            assert_eq!(rids.len(), self.m);
            let recs = rids
                .into_iter()
                .zip(pss_.into_iter())
                .map(|(r, ps)| -> Result<_, _> {
                    ops.push(ps);
                    let (rec, rid) = futures::executor::block_on(r)?;
                    ringid_map.insert(rid, ops.len() - 1);
                    Ok((rec, rid))
                })
                .collect::<Result<Vec<_>, ()>>()?;
            // finish appending to WAL
            /*
            for rid in rids.iter() {
                println!("got ringid: {:?}", rid);
            }
            */
            // prepare data writes
            for (ps, rid) in recs.into_iter() {
                canvas.prepaint(&ps, &rid);
            }
            // run k ticks of the fine-grained scheduler
            for _ in 0..rng.gen_range(1, self.k) {
                // storage I/O could fail
                if fgen.next_fail() {
                    return Err(());
                }
                if let Some((fin_rid, _)) = canvas.rand_paint(&mut rng) {
                    if let Some(rid) = fin_rid {
                        futures::executor::block_on(wal.peel(&[rid]))?
                    }
                } else {
                    break;
                }
            }
        }
        //canvas.print(40);
        assert_eq!(wal.file_pool_in_use(), 0);
        Ok(())
    }

    pub fn get_walloader(&self) -> WALLoader {
        WALLoader::new(self.file_nbit, self.block_nbit, self.file_cache)
    }

    pub fn get_nticks(&self, state: &mut WALStoreEmulState) -> usize {
        let mut canvas = Canvas::new(self.csize);
        let mut ops: Vec<PaintStrokes> = Vec::new();
        let mut ringid_map = HashMap::new();
        let fgen = Rc::new(CountFailGen::new());
        self.run(
            state,
            &mut canvas,
            self.get_walloader(),
            &mut ops,
            &mut ringid_map,
            fgen.clone(),
        )
        .unwrap();
        fgen.get_count()
    }

    pub fn check(
        &self,
        state: &mut WALStoreEmulState,
        canvas: &mut Canvas,
        wal: WALLoader,
        ops: &Vec<PaintStrokes>,
        ringid_map: &HashMap<WALRingId, usize>,
    ) -> bool {
        if ops.is_empty() {
            return true;
        }
        let mut last_idx = 0;
        let mut napplied = 0;
        canvas.clear_queued();
        wal.recover(WALStoreEmul::new(
            state,
            Rc::new(ZeroFailGen),
            |payload, ringid| {
                let s = PaintStrokes::from_bytes(&payload);
                canvas.prepaint(&s, &ringid);
                last_idx = *ringid_map.get(&ringid).unwrap() + 1;
                napplied += 1;
            },
        ))
        .unwrap();
        println!("last = {}/{}, applied = {}", last_idx, ops.len(), napplied);
        canvas.paint_all();
        // recover complete
        let canvas0 = if last_idx > 0 {
            let canvas0 = canvas.new_reference(&ops[..last_idx]);
            if canvas.is_same(&canvas0) {
                None
            } else {
                Some(canvas0)
            }
        } else {
            let canvas0 = canvas.new_reference(&[]);
            if canvas.is_same(&canvas0) {
                None
            } else {
                let i0 = ops.len() - self.m;
                let mut canvas0 = canvas0.new_reference(&ops[..i0]);
                let mut res = None;
                'outer: loop {
                    if canvas.is_same(&canvas0) {
                        break;
                    }
                    for i in i0..ops.len() {
                        canvas0.prepaint(&ops[i], &WALRingId::empty_id());
                        canvas0.paint_all();
                        if canvas.is_same(&canvas0) {
                            break 'outer;
                        }
                    }
                    res = Some(canvas0);
                    break;
                }
                res
            }
        };
        if let Some(canvas0) = canvas0 {
            canvas.print(40);
            canvas0.print(40);
            false
        } else {
            true
        }
    }

    pub fn new_canvas(&self) -> Canvas {
        Canvas::new(self.csize)
    }
}
