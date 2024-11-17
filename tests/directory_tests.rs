#[cfg(test)]
#[cfg(feature = "directory")]
mod directory_tests {
    use std::path::PathBuf;
    use data_funnel::{implementation::directory::DirectoryDataStore, DataStore};
    use rand::{seq::SliceRandom, SeedableRng};
    use tempfile::NamedTempFile;

    #[test]
    fn test_k6u9_initialize() {
        let sqlite_tempfile = NamedTempFile::new().unwrap();
        let sqlite_file_path: PathBuf = sqlite_tempfile.path().into();
        let cache_filename_length: usize = 10;
        
        let _directory_data_store = DirectoryDataStore::new(
            sqlite_file_path,
            cache_filename_length,
        ).expect("Failed to create new DirectoryDataStore.");
    }

    #[tokio::test]
    async fn test_w2m7_initialize_and_then_initialize_instance() {
        let sqlite_tempfile = NamedTempFile::new().unwrap();
        let sqlite_file_path: PathBuf = sqlite_tempfile.path().into();
        let cache_filename_length: usize = 10;
        
        let mut directory_data_store = DirectoryDataStore::new(
            sqlite_file_path,
            cache_filename_length,
        ).expect("Failed to create new DirectoryDataStore.");

        directory_data_store.initialize()
            .await
            .expect("Failed to initialize DirectoryDataStore.");
    }

    #[tokio::test]
    async fn test_p4n8_store_bytes_and_retrieve_bytes_and_delete_bytes() {

        // initialize DirectoryDataStore

        let sqlite_tempfile = NamedTempFile::new().unwrap();
        let sqlite_file_path: PathBuf = sqlite_tempfile.path().into();
        let cache_filename_length: usize = 10;
        
        let mut directory_data_store = DirectoryDataStore::new(
            sqlite_file_path,
            cache_filename_length,
        ).expect("Failed to create new DirectoryDataStore.");

        directory_data_store.initialize()
            .await
            .expect("Failed to initialize DirectoryDataStore.");

        let mut ids = Vec::new();

        for j in 0..100 {

            // store file

            let mut bytes = Vec::new();
            bytes.push(1 + j);
            bytes.push(2 + j);
            bytes.push(3 + j);
            bytes.push(4 + j);
            bytes.push(5 + j);
            bytes.push(6 + j);
            bytes.push(7 + j);
            bytes.push(8 + j);
            let id = directory_data_store.insert(bytes.clone())
                .await
                .expect("Failed to insert into DirectoryDataStore.");

            // pull out bytes

            let read_bytes = directory_data_store.get(&id)
                .await
                .expect("Failed to read from DirectoryDataStore.");

            assert_eq!(bytes.len(), read_bytes.len());
            for i in 0..bytes.len() {
                assert_eq!(bytes[i], read_bytes[i]);
            }

            ids.push(id);
        }

        let list = directory_data_store.list(1, 4, 2)
            .await
            .expect("Failed to get list of IDs");

        assert_eq!(4, list.len());
        assert_eq!(7, list[0]);
        assert_eq!(8, list[1]);
        assert_eq!(9, list[2]);
        assert_eq!(10, list[3]);

        let mut random = rand::rngs::StdRng::from_entropy();
        ids.shuffle(&mut random);

        for id in ids {
            directory_data_store.delete(&id)
                .await
                .expect(&format!("Failed to delete for ID {}", id));
        }
    }

    #[tokio::test]
    async fn test_q8b2_bulk_insert_and_bulk_get() {
        let sqlite_tempfile = NamedTempFile::new().unwrap();
        let sqlite_file_path: PathBuf = sqlite_tempfile.path().into();
        let cache_filename_length: usize = 10;
        
        let mut data_store = DirectoryDataStore::new(
            sqlite_file_path,
            cache_filename_length,
        ).expect("Failed to create new DirectoryDataStore.");

        data_store.initialize()
            .await
            .expect("Failed to initialize DirectoryDataStore.");

        let original_bytes_collection = vec![
            vec![1u8],
            vec![2u8, 3u8],
            vec![4u8, 5u8, 6u8],
        ];
        let ids = data_store.bulk_insert(original_bytes_collection.clone())
            .await
            .expect("Failed to perform bulk insert.");

        // try in the original ID order
        {
            let retrieved_bytes_collection = data_store.bulk_get(&ids)
                .await
                .expect("Failed to bulk get by IDs.");

            assert_eq!(original_bytes_collection, retrieved_bytes_collection);
        }

        // try in reverse order
        {
            let mut reversed_ids = Vec::with_capacity(ids.len());
            for i in ids.len() - 1..=0 {
                reversed_ids.push(ids[i]);
            }

            let retrieved_bytes_collection = data_store.bulk_get(&reversed_ids)
                .await
                .expect("Failed to bulk get by IDs.");

            assert_ne!(original_bytes_collection, retrieved_bytes_collection);

            for (top_down, down_up) in (ids.len() - 1..=0).zip(0..ids.len()) {
                assert_eq!(original_bytes_collection[down_up], retrieved_bytes_collection[top_down]);
            }
        }
    }
}