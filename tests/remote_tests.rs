
#[cfg(test)]
mod remote_tests {
    use std::path::PathBuf;

    use data_funnel::{implementation::{directory::DirectoryDataStore, remote::{RemoteDataStoreClient, RemoteDataStoreServer}}, DataStore};
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn initialize() {
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

        let server_public_key_tempfile = tempfile::NamedTempFile::new().unwrap();
        let mut client = RemoteDataStoreClient::new(
            server_public_key_tempfile.path().into(),
            String::from("localhost"),
            String::from("localhost"),
            8080,
        );
        client.initialize()
            .await
            .expect("Failed to initialize client.");
        let server = RemoteDataStoreServer::new(
            directory_data_store,
            String::from("localhost"),
            8080,
        );
        todo!()
    }
}