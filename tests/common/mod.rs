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

type FailGen = std::iter::Iterator<Item = bool>;

/// Emulate the a virtual file handle.
pub struct WALFileEmul {
    file: Rc<FileContentEmul>,
}

impl WALFile for WALFileEmul {
    fn allocate(&self, offset: WALPos, length: usize) -> Result<(), ()> {
        let offset = offset as usize;
        if offset + length > self.file.borrow().len() {
            self.file.borrow_mut().resize(offset + length, 0)
        }
        for v in &mut self.file.borrow_mut()[offset..offset + length] { *v = 0 }
        Ok(())
    }

    fn truncate(&self, length: usize) -> Result<(), ()> {
        self.file.borrow_mut().resize(length, 0);
        Ok(())
    }

    fn write(&self, offset: WALPos, data: WALBytes) -> Result<(), ()> {
        let offset = offset as usize;
        &self.file.borrow_mut()[offset..offset + data.len()].copy_from_slice(&data);
        Ok(())
    }

    fn read(&self, offset: WALPos, length: usize) -> Result<Option<WALBytes>, ()> {
        let offset = offset as usize;
        let file = self.file.borrow();
        if offset + length > file.len() { Ok(None) }
        else {
            Ok(Some((&file[offset..offset + length]).to_vec().into_boxed_slice()))
        }
    }
}

pub struct WALStoreEmulState{
    files: HashMap<String, Rc<FileContentEmul>>,
}

impl WALStoreEmulState {
    pub fn new() -> Self { WALStoreEmulState { files: HashMap::new() } }
}

/// Emulate the persistent storage state.
pub struct WALStoreEmul<'a>(&'a mut WALStoreEmulState);

impl<'a> WALStoreEmul<'a> {
    pub fn new(state: &'a mut WALStoreEmulState) -> Self {
        WALStoreEmul(state)
    }
}

impl<'a> WALStore for WALStoreEmul<'a> {
    type FileNameIter = std::vec::IntoIter<String>;

    fn open_file(&mut self, filename: &str, touch: bool) -> Result<Box<dyn WALFile>, ()> {
        match self.0.files.entry(filename.to_string()) {
            Entry::Occupied(e) => Ok(Box::new(WALFileEmul { file: e.get().clone() })),
            Entry::Vacant(e) => if touch {
                Ok(Box::new(
                    WALFileEmul { file: e.insert(Rc::new(FileContentEmul::new())).clone() }))
            } else {
                Err(())
            }
        }
    }

    fn remove_file(&mut self, filename: &str) -> Result<(), ()> {
        self.0.files.remove(filename).ok_or(()).and_then(|_| Ok(()))
    }

    fn enumerate_files(&self) -> Result<Self::FileNameIter, ()> {
        let mut logfiles = Vec::new();
        for (fname, _) in self.0.files.iter() {
            logfiles.push(fname.clone())
        }
        Ok(logfiles.into_iter())
    }

    fn apply_payload(&mut self, payload: WALBytes) -> Result<(), ()> {
        println!("apply_payload(payload={})", std::str::from_utf8(&payload).unwrap());
        Ok(())
    }
}
