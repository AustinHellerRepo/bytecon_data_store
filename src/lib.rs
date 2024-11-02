use std::error::Error;

pub mod implementation;

pub trait DataStore {
    type Item;
    type Key;

    fn insert(&mut self, item: Self::Item) -> Result<Self::Key, Box<dyn Error>>;
    fn get(&self, id: &Self::Key) -> Result<Self::Item, Box<dyn Error>>;
}