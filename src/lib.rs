use std::{error::Error, future::Future};

use futures::future::join_all;

pub mod implementation;

// TODO create DistributedRemoteDataStore (server/client) for splitting out data across multiple possible data stores
//      a strategy enum could be provide with variants like "evenly divided" (total bytes used on each is equal), "proportionally divided" (current bytes divided by max bytes is equal), "largest first", "smallest first", "fastest first", "slowest first", etc.

pub trait DataStore {
    type Item;
    type Key;

    fn initialize(&mut self) -> impl Future<Output = Result<(), Box<dyn Error>>> + Send;
    fn insert(&mut self, item: Self::Item) -> impl Future<Output = Result<Self::Key, Box<dyn Error>>> + Send;
    fn get(&self, id: &Self::Key) -> impl Future<Output = Result<Self::Item, Box<dyn Error>>> + Send;
    fn delete(&self, id: &Self::Key) -> impl Future<Output = Result<(), Box<dyn Error>>> + Send;
    fn list(&self, page_index: u64, page_size: u64, row_offset: u64) -> impl Future<Output = Result<Vec<Self::Key>, Box<dyn Error>>> + Send;
    fn bulk_insert(&mut self, items: Vec<Self::Item>) -> impl Future<Output = Result<Vec<Self::Key>, Box<dyn Error>>> + Send;
    fn bulk_get(&self, ids: &Vec<Self::Key>) -> impl Future<Output = Result<Vec<Self::Item>, Box<dyn Error>>> + Send;

}