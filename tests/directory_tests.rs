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
        
        let directory_data_store = DirectoryDataStore::new(
            sqlite_file_path,
            cache_filename_length,
        ).expect("Failed to create new DirectoryDataStore.");
    }

    #[test]
    fn initialize_and_then_initialize_instance() {
        let sqlite_tempfile = NamedTempFile::new().unwrap();
        let sqlite_file_path: PathBuf = sqlite_tempfile.path().into();
        let cache_filename_length: usize = 10;
        
        let mut directory_data_store = DirectoryDataStore::new(
            sqlite_file_path,
            cache_filename_length,
        ).expect("Failed to create new DirectoryDataStore.");

        directory_data_store.initialize()
            .expect("Failed to initialize DirectoryDataStore.");
    }

    #[test]
    fn store_file() {

        // initialize DirectoryDataStore

        let sqlite_tempfile = NamedTempFile::new().unwrap();
        let sqlite_file_path: PathBuf = sqlite_tempfile.path().into();
        let cache_filename_length: usize = 10;
        
        let mut directory_data_store = DirectoryDataStore::new(
            sqlite_file_path,
            cache_filename_length,
        ).expect("Failed to create new DirectoryDataStore.");

        directory_data_store.initialize()
            .expect("Failed to initialize DirectoryDataStore.");

        // store file

        let mut bytes = Vec::new();
        bytes.push(1);
        bytes.push(2);
        bytes.push(3);
        bytes.push(4);
        bytes.push(5);
        bytes.push(6);
        bytes.push(7);
        bytes.push(8);
        let id = directory_data_store.insert(bytes.clone())
            .expect("Failed to insert into DirectoryDataStore.");

        // pull out bytes

        let read_bytes = directory_data_store.get(&id)
            .expect("Failed to read from DirectoryDataStore.");

        assert_eq!(bytes.len(), read_bytes.len());
        for i in 0..bytes.len() {
            assert_eq!(bytes[i], read_bytes[i]);
        }
    }
}