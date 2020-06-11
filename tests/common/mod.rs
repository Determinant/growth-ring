#[cfg(test)]
#[allow(dead_code)]

extern crate growthring;
use growthring::wal::{WALFile, WALStore, WALPos, WALBytes, WALRingId};
use indexmap::{IndexMap, map::Entry};
use rand::Rng;
use std::collections::{HashMap, hash_map};
use std::cell::RefCell;
use std::rc::Rc;
use std::convert::TryInto;
use std::collections::VecDeque;

thread_local! {
    //pub static RNG: RefCell<rand::rngs::StdRng> = RefCell::new(<rand::rngs::StdRng as rand::SeedableRng>::from_seed([0; 32]));
    pub static RNG: RefCell<rand::rngs::ThreadRng> = RefCell::new(rand::thread_rng());
}

/*
pub fn gen_rand_letters(i: usize) -> String {
    RNG.with(|rng| {
        (0..i).map(|_| (rng.borrow_mut().gen_range(0, 26) + 'a' as u8) as char).collect()
    })
}
*/

struct FileContentEmul(RefCell<Vec<u8>>);

impl FileContentEmul {
    pub fn new() -> Self { FileContentEmul(RefCell::new(Vec::new())) }
}

impl std::ops::Deref for FileContentEmul {
    type Target = RefCell<Vec<u8>>;
    fn deref(&self) -> &Self::Target {&self.0}
}

pub trait FailGen {
    fn next_fail(&self) -> bool;
}

/// Emulate the a virtual file handle.
pub struct WALFileEmul<G: FailGen> {
    file: Rc<FileContentEmul>,
    fgen: Rc<G>
}

impl<G: FailGen> WALFile for WALFileEmul<G> {
    fn allocate(&self, offset: WALPos, length: usize) -> Result<(), ()> {
        if self.fgen.next_fail() { return Err(()) }
        let offset = offset as usize;
        if offset + length > self.file.borrow().len() {
            self.file.borrow_mut().resize(offset + length, 0)
        }
        for v in &mut self.file.borrow_mut()[offset..offset + length] { *v = 0 }
        Ok(())
    }

    fn truncate(&self, length: usize) -> Result<(), ()> {
        if self.fgen.next_fail() { return Err(()) }
        self.file.borrow_mut().resize(length, 0);
        Ok(())
    }

    fn write(&self, offset: WALPos, data: WALBytes) -> Result<(), ()> {
        if self.fgen.next_fail() { return Err(()) }
        let offset = offset as usize;
        &self.file.borrow_mut()[offset..offset + data.len()].copy_from_slice(&data);
        Ok(())
    }

    fn read(&self, offset: WALPos, length: usize) -> Result<Option<WALBytes>, ()> {
        if self.fgen.next_fail() { return Err(()) }
        let offset = offset as usize;
        let file = self.file.borrow();
        if offset + length > file.len() { Ok(None) }
        else {
            Ok(Some((&file[offset..offset + length]).to_vec().into_boxed_slice()))
        }
    }
}

pub struct WALStoreEmulState {
    files: HashMap<String, Rc<FileContentEmul>>,
}

impl WALStoreEmulState {
    pub fn new() -> Self { WALStoreEmulState { files: HashMap::new() } }
}

/// Emulate the persistent storage state.
pub struct WALStoreEmul<'a, G, F>
where
    G: FailGen,
    F: Fn(WALBytes, WALPos) {
    state: &'a mut WALStoreEmulState,
    fgen: Rc<G>,
    recover: F
}

impl<'a, G: FailGen, F: Fn(WALBytes, WALPos)> WALStoreEmul<'a, G, F> {
    pub fn new(state: &'a mut WALStoreEmulState, fail_gen: G,
                recover: F) -> Self {
        WALStoreEmul {
            state,
            fgen: Rc::new(fail_gen),
            recover
        }
    }
}

impl<'a, G, F> WALStore for WALStoreEmul<'a, G, F> 
where
    G: 'static + FailGen, F: Fn(WALBytes, WALPos) {
    type FileNameIter = std::vec::IntoIter<String>;

    fn open_file(&mut self, filename: &str, touch: bool) -> Result<Box<dyn WALFile>, ()> {
        if self.fgen.next_fail() { return Err(()) }
        match self.state.files.entry(filename.to_string()) {
            hash_map::Entry::Occupied(e) => Ok(Box::new(WALFileEmul {
                file: e.get().clone(),
                fgen: self.fgen.clone()
            })),
            hash_map::Entry::Vacant(e) => if touch {
                Ok(Box::new(WALFileEmul {
                    file: e.insert(Rc::new(FileContentEmul::new())).clone(),
                    fgen: self.fgen.clone()
                }))
            } else {
                Err(())
            }
        }
    }

    fn remove_file(&mut self, filename: &str) -> Result<(), ()> {
        println!("remove_file(filename={})", filename);
        if self.fgen.next_fail() { return Err(()) }
        self.state.files.remove(filename).ok_or(()).and_then(|_| Ok(()))
    }

    fn enumerate_files(&self) -> Result<Self::FileNameIter, ()> {
        if self.fgen.next_fail() { return Err(()) }
        let mut logfiles = Vec::new();
        for (fname, _) in self.state.files.iter() {
            logfiles.push(fname.clone())
        }
        Ok(logfiles.into_iter())
    }

    fn apply_payload(&mut self, payload: WALBytes, wal_off: WALPos) -> Result<(), ()> {
        if self.fgen.next_fail() { return Err(()) }
        println!("apply_payload(payload=0x{}, wal_off={})",
                hex::encode(&payload),
                wal_off);
        (self.recover)(payload, wal_off);
        Ok(())
    }
}

pub struct SingleFailGen {
    cnt: std::cell::Cell<usize>,
    fail_point: usize
}

impl SingleFailGen {
    pub fn new(fail_point: usize) -> Self {
        SingleFailGen {
            cnt: std::cell::Cell::new(0),
            fail_point
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
    fn next_fail(&self) -> bool { false }
}

/// An ordered list of intervals: `(begin, end, color)*`.
pub struct PaintStrokes(Vec<(u32, u32, u32)>);

impl PaintStrokes {
    pub fn new() -> Self { PaintStrokes(Vec::new()) }
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
            res.push((u32::from_le_bytes(s_raw.try_into().unwrap()),
                    u32::from_le_bytes(e_raw.try_into().unwrap()),
                    u32::from_le_bytes(c_raw.try_into().unwrap())));
            rest = rest3
        }
        PaintStrokes(res)
    }

    pub fn gen_rand<R: rand::Rng>(max_pos: u32, max_len: u32, max_col: u32, n: usize, rng: &mut R) -> PaintStrokes {
        assert!(max_pos > 0);
        let mut strokes = Self::new();
        for _ in 0..n {
            let pos = rng.gen_range(0, max_pos);
            let len = rng.gen_range(1, max_pos - pos + 1);
            strokes.stroke(pos, pos + len, rng.gen_range(0, max_col))
        }
        strokes
    }
    
    pub fn stroke(&mut self, start: u32, end: u32, color: u32) {
        self.0.push((start, end, color))
    }

    pub fn into_vec(self) -> Vec<(u32, u32, u32)> { self.0 }
}

#[test]
fn test_paint_strokes() {
    let mut p = PaintStrokes::new();
    for i in 0..3 {
        p.stroke(i, i + 3, i + 10)
    }
    let pr = p.to_bytes();
    for ((s, e, c), i) in PaintStrokes::from_bytes(&pr)
                            .into_vec().into_iter().zip(0..) {
        assert_eq!(s, i);
        assert_eq!(e, i + 3);
        assert_eq!(c, i + 10);
    }
}

pub struct Canvas {
    waiting: HashMap<WALRingId, usize>,
    queue: IndexMap<u32, VecDeque<(u32, WALRingId)>>,
    canvas: Box<[u32]>
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
            canvas
        }
    }

    fn get_waiting(&mut self, rid: WALRingId) -> &mut usize {
        match self.waiting.entry(rid) {
            hash_map::Entry::Occupied(e) => e.into_mut(),
            hash_map::Entry::Vacant(e) => e.insert(0)
        }
    }

    fn get_queued(&mut self, pos: u32) -> &mut VecDeque<(u32, WALRingId)> {
        match self.queue.entry(pos) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(VecDeque::new())
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
    pub fn rand_paint<R: rand::Rng>(&mut self, rng: &mut R) -> Option<(Option<WALRingId>, u32)> {
        if self.queue.is_empty() { return None }
        let idx = rng.gen_range(0, self.queue.len());
        let (pos, _) = self.queue.get_index_mut(idx).unwrap();
        let pos = *pos;
        Some((self.paint(pos), pos))
    }

    pub fn paint(&mut self, pos: u32) -> Option<WALRingId> {
        let q = self.queue.get_mut(&pos).unwrap();
        let (c, rid) = q.pop_front().unwrap();
        if q.is_empty() { self.queue.remove(&pos); }
        self.canvas[pos as usize] = c;
        let cnt = self.waiting.get_mut(&rid).unwrap();
        *cnt -= 1;
        if *cnt == 0 {
            Some(rid)
        } else { None }
    }

    pub fn is_same(&self, other: &Canvas) -> bool {
        self.canvas.cmp(&other.canvas) == std::cmp::Ordering::Equal
    }

    pub fn print(&self, max_col: usize) {
        for r in self.canvas.chunks(max_col) {
            for c in r.iter() {
                print!("{:02x} ", c & 0xff);
            }
            println!("");
        }
    }
}

#[test]
fn test_canvas() {
    let mut canvas1 = Canvas::new(100);
    let mut canvas2 = Canvas::new(100);
    let canvas3 = Canvas::new(101);
    let dummy = WALRingId::empty_id();
    let (s1, s2) = RNG.with(|rng| {
        let rng = &mut *rng.borrow_mut();
        (PaintStrokes::gen_rand(100, 10, 256, 2, rng),
        PaintStrokes::gen_rand(100, 10, 256, 2, rng))
    });
    assert!(canvas1.is_same(&canvas2));
    assert!(!canvas2.is_same(&canvas3));
    canvas1.prepaint(&s1, &dummy);
    canvas1.prepaint(&s2, &dummy);
    canvas2.prepaint(&s1, &dummy);
    canvas2.prepaint(&s2, &dummy);
    assert!(canvas1.is_same(&canvas2));
    RNG.with(|rng| canvas1.rand_paint(&mut *rng.borrow_mut()));
    assert!(!canvas1.is_same(&canvas2));
    RNG.with(|rng| while let Some(_) = canvas1.rand_paint(&mut *rng.borrow_mut()) {});
    RNG.with(|rng| while let Some(_) = canvas2.rand_paint(&mut *rng.borrow_mut()) {});
    assert!(canvas1.is_same(&canvas2));
    canvas1.print(10);
}
