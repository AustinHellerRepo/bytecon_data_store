use std::error::Error;

pub mod implementation;

pub trait DataStore {
    type Item;
    type Key;

    async fn initialize(&mut self) -> Result<(), Box<dyn Error>>;
    async fn insert(&mut self, item: Self::Item) -> Result<Self::Key, Box<dyn Error>>;
    async fn get(&self, id: &Self::Key) -> Result<Self::Item, Box<dyn Error>>;
}