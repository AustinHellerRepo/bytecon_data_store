#[cfg(test)]
#[cfg(feature = "remote")]
mod remote_tests {
    use std::{io::Write, path::PathBuf, time::Duration};
    use data_funnel::{implementation::{directory::DirectoryDataStore, remote::{RemoteDataStoreClient, RemoteDataStoreServer}}, DataStore};
    use rand::{seq::SliceRandom, SeedableRng};
    use rcgen::{generate_simple_self_signed, CertifiedKey};
    use tempfile::NamedTempFile;
    use tokio::time::sleep;

    #[tokio::test]
    async fn initialize() {
        let sqlite_tempfile = NamedTempFile::new().unwrap();
        let sqlite_file_path: PathBuf = sqlite_tempfile.path().into();
        let cache_filename_length: usize = 10;

        let mut server_public_key_tempfile = tempfile::NamedTempFile::new().unwrap();
        println!("public key: {:?}", server_public_key_tempfile.path());

        let mut server_private_key_tempfile = tempfile::NamedTempFile::new().unwrap();
        println!("private key: {:?}", server_private_key_tempfile.path());

        // generate self-signed keys
        let (public_key_bytes, private_key_bytes) = {
            let CertifiedKey { cert, key_pair } = generate_simple_self_signed(vec![String::from("localhost")])
                .expect("Failed to generate self-signed cert.");

            let cert_pem = cert.pem();
            let private_key_pem = key_pair.serialize_pem();

            (cert_pem.into_bytes(), private_key_pem.into_bytes())
        };

        server_public_key_tempfile.write_all(&public_key_bytes)
            .expect("Failed to write public key bytes.");
        server_private_key_tempfile.write_all(&private_key_bytes)
            .expect("Failed to write private key bytes.");

        println!("starting server task...");
        let _server_task = {
            let sqlite_file_path = sqlite_file_path.clone();
            let cache_filename_length: usize = cache_filename_length.clone();
            let server_public_key_file_path: PathBuf = server_public_key_tempfile.path().into();
            let server_private_key_file_path: PathBuf = server_private_key_tempfile.path().into();
            tokio::spawn(async move {
                let mut directory_data_store = DirectoryDataStore::new(
                    sqlite_file_path,
                    cache_filename_length,
                ).expect("Failed to create new DirectoryDataStore.");

                directory_data_store.initialize()
                    .await
                    .expect("Failed to initialize DirectoryDataStore.");

                let mut server = RemoteDataStoreServer::new(
                    directory_data_store,
                    server_public_key_file_path,
                    server_private_key_file_path,
                    String::from("localhost"),
                    8080,
                );
                server.start()
                    .await
                    .expect("Failed to start server.");
            })
        };
        println!("started server task");

        println!("sleeping...");

        // wait for the server to start listening
        sleep(Duration::from_millis(100)).await;
        println!("sleeping done");

        let mut client = RemoteDataStoreClient::new(
            server_public_key_tempfile.path().into(),
            String::from("localhost"),
            String::from("localhost"),
            8080,
        );
        client.initialize()
            .await
            .expect("Failed to initialize client.");

        let mut ids = Vec::new();

        for j in 0..100 {
        
            let id = client.insert(vec![
                1 + j,
                2 + j,
                3 + j,
                4 + j,
            ])
                .await
                .expect("Failed to send bytes from client.");

            let bytes = client.get(&id)
                .await
                .expect("Failed to get bytes back via client.");

            assert_eq!(4, bytes.len());
            assert_eq!(1 + j, bytes[0]);
            assert_eq!(2 + j, bytes[1]);
            assert_eq!(3 + j, bytes[2]);
            assert_eq!(4 + j, bytes[3]);

            ids.push(id);
        }

        let mut random = rand::rngs::StdRng::from_entropy();
        ids.shuffle(&mut random);

        for id in ids {
            client.delete(&id)
                .await
                .expect(&format!("Failed to delete ID {}", id));
        }
    }
}