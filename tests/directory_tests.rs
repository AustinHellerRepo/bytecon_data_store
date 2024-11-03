#[cfg(test)]
mod directory_tests {
    use std::path::PathBuf;

    use data_funnel::{implementation::directory::DirectoryDataStore, DataStore};
    use tempfile::NamedTempFile;

    #[test]
    fn initialize() {
        let sqlite_tempfile = NamedTempFile::new().unwrap();
        let sqlite_file_path: PathBuf = sqlite_tempfile.path().into();
        let cache_filename_length: usize = 10;
        
        let _directory_data_store = DirectoryDataStore::new(
            sqlite_file_path,
            cache_filename_length,
        ).expect("Failed to create new DirectoryDataStore.");
    }

    #[tokio::test]
    async fn initialize_and_then_initialize_instance() {
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
    async fn store_bytes_and_retrieve_bytes() {

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
        }
    }
}