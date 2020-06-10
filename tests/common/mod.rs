#[cfg(test)]
#[allow(dead_code)]

extern crate growthring;
use growthring::wal::{WALFile, WALStore, WALPos, WALBytes};
use std::collections::{HashMap, hash_map::Entry};
use std::cell::RefCell;
use std::rc::Rc;

/*
thread_local! {
    //pub static RNG: RefCell<rand::rngs::StdRng> = RefCell::new(<rand::rngs::StdRng as rand::SeedableRng>::from_seed([0; 32]));
    pub static RNG: RefCell<rand::rngs::ThreadRng> = RefCell::new(rand::thread_rng());
}

pub fn gen_rand_letters(i: usize) -> String {
    //let mut rng = rand::thread_rng();
    RNG.with(|rng| {
        (0..i).map(|_| (rng.borrow_mut().gen::<u8>() % 26 + 'a' as u8) as char).collect()
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
pub struct WALStoreEmul<'a, G: FailGen> {
    state: &'a mut WALStoreEmulState,
    fgen: Rc<G>
}

impl<'a, G: FailGen> WALStoreEmul<'a, G> {
    pub fn new(state: &'a mut WALStoreEmulState, fail_gen: G) -> Self {
        WALStoreEmul { state, fgen: Rc::new(fail_gen) }
    }
}

impl<'a, G: 'static + FailGen> WALStore for WALStoreEmul<'a, G> {
    type FileNameIter = std::vec::IntoIter<String>;

    fn open_file(&mut self, filename: &str, touch: bool) -> Result<Box<dyn WALFile>, ()> {
        if self.fgen.next_fail() { return Err(()) }
        match self.state.files.entry(filename.to_string()) {
            Entry::Occupied(e) => Ok(Box::new(WALFileEmul {
                file: e.get().clone(),
                fgen: self.fgen.clone()
            })),
            Entry::Vacant(e) => if touch {
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

    fn apply_payload(&mut self, payload: WALBytes) -> Result<(), ()> {
        if self.fgen.next_fail() { return Err(()) }
        println!("apply_payload(payload={})", std::str::from_utf8(&payload).unwrap());
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
