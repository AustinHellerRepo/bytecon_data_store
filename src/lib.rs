use std::{error::Error, future::Future};

pub mod implementation;

pub trait DataStore {
    type Item;
    type Key;

    fn initialize(&mut self) -> impl Future<Output = Result<(), Box<dyn Error>>> + Send;
    fn insert(&mut self, item: Self::Item) -> impl Future<Output = Result<Self::Key, Box<dyn Error>>> + Send;
    fn get(&self, id: &Self::Key) -> impl Future<Output = Result<Self::Item, Box<dyn Error>>> + Send;
    fn delete(&self, id: &Self::Key) -> impl Future<Output = Result<(), Box<dyn Error>>> + Send;
}