#[cfg(test)]
#[cfg(feature = "remote")]
mod remote_tests {
    use std::{io::Write, path::PathBuf, sync::Arc, time::Duration};
    use data_funnel::{implementation::{directory::DirectoryDataStore, postgres::PostgresDataStore, remote::{RemoteDataStoreClient, RemoteDataStoreServer}}, DataStore};
    use rand::{seq::SliceRandom, SeedableRng};
    use rcgen::{generate_simple_self_signed, CertifiedKey};
    use server_client_bytecon::{ByteConCertificate, ByteConPrivateKey, ByteConPublicKey};
    use tempfile::NamedTempFile;
    use tokio::{sync::Mutex, time::sleep};

    #[tokio::test]
    async fn test_t9w7_initialize_directory_data_store() {
        let sqlite_tempfile = NamedTempFile::new().unwrap();
        let sqlite_file_path: PathBuf = sqlite_tempfile.path().into();
        let cache_filename_length: usize = 10;
        let port = 8082;

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
        let server_task_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let _server_task = {
            let server_task_error = server_task_error.clone();
            let sqlite_file_path = sqlite_file_path.clone();
            let cache_filename_length: usize = cache_filename_length.clone();
            let server_public_key_file_path: PathBuf = server_public_key_tempfile.path().into();
            let server_private_key_file_path: PathBuf = server_private_key_tempfile.path().into();
            tokio::spawn(async move {
                'process_thread: {
                    let data_store_result = DirectoryDataStore::new(
                        sqlite_file_path,
                        cache_filename_length,
                    )
                        .map_err(|error| {
                            format!("Error within server task: {:?}", error)
                        });
                    if let Err(error) = data_store_result {
                        *server_task_error
                            .lock()
                            .await = Some(error);
                        break 'process_thread;
                    }
                    let mut data_store = data_store_result.unwrap();

                    let initialization_result = data_store.initialize()
                        .await
                        .map_err(|error| {
                            format!("Error within server task: {:?}", error)
                        });
                    if let Err(error) = initialization_result {
                        *server_task_error
                            .lock()
                            .await = Some(error);
                        break 'process_thread;
                    }
                    initialization_result.unwrap();

                    let mut server = RemoteDataStoreServer::new(
                        Arc::new(Mutex::new(data_store)),
                        ByteConPublicKey::new(ByteConCertificate::FilePath(server_public_key_file_path)),
                        ByteConPrivateKey::new(ByteConCertificate::FilePath(server_private_key_file_path)),
                        String::from("localhost"),
                        port,
                    );
                    let start_result = server.start()
                        .await
                        .map_err(|error| {
                            format!("Error within server task: {:?}", error)
                        });
                    if let Err(error) = start_result {
                        *server_task_error
                            .lock()
                            .await = Some(error);
                        break 'process_thread;
                    }
                }
            })
        };
        println!("started server task");

        println!("sleeping...");

        // wait for the server to start listening
        sleep(Duration::from_millis(1000)).await;
        println!("sleeping done");

        if let Some(error) = server_task_error.lock().await.as_ref() {
            eprintln!("{}", error);
        }
        assert!(server_task_error.lock().await.is_none());

        let mut client: RemoteDataStoreClient = RemoteDataStoreClient::new(
            ByteConPublicKey::new(ByteConCertificate::FilePath(server_public_key_tempfile.path().into())),
            String::from("localhost"),
            String::from("localhost"),
            port,
        );

        if _server_task.is_finished() {
            eprintln!("Server task has already ended for some reason.");
        }

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

        let list = client.list(1, 4, 2)
            .await
            .expect("Failed to get list of IDs.");

        assert_eq!(4, list.len());
        assert_eq!(7, list[0]);
        assert_eq!(8, list[1]);
        assert_eq!(9, list[2]);
        assert_eq!(10, list[3]);

        let mut random = rand::rngs::StdRng::from_entropy();
        ids.shuffle(&mut random);

        for id in ids {
            client.delete(&id)
                .await
                .expect(&format!("Failed to delete ID {}", id));
        }
    }

    #[tokio::test]
    async fn test_c6b1_directory_data_store_bulk_insert_and_bulk_get() {
        let sqlite_tempfile = NamedTempFile::new().unwrap();
        let sqlite_file_path: PathBuf = sqlite_tempfile.path().into();
        let cache_filename_length: usize = 10;
        let port = 8082;

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
        let server_task_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let _server_task = {
            let server_task_error = server_task_error.clone();
            let sqlite_file_path = sqlite_file_path.clone();
            let cache_filename_length: usize = cache_filename_length.clone();
            let server_public_key_file_path: PathBuf = server_public_key_tempfile.path().into();
            let server_private_key_file_path: PathBuf = server_private_key_tempfile.path().into();
            tokio::spawn(async move {
                'process_thread: {
                    let data_store_result = DirectoryDataStore::new(
                        sqlite_file_path,
                        cache_filename_length,
                    )
                        .map_err(|error| {
                            format!("Error within server task: {:?}", error)
                        });
                    if let Err(error) = data_store_result {
                        *server_task_error
                            .lock()
                            .await = Some(error);
                        break 'process_thread;
                    }
                    let mut data_store = data_store_result.unwrap();

                    let initialization_result = data_store.initialize()
                        .await
                        .map_err(|error| {
                            format!("Error within server task: {:?}", error)
                        });
                    if let Err(error) = initialization_result {
                        *server_task_error
                            .lock()
                            .await = Some(error);
                        break 'process_thread;
                    }
                    initialization_result.unwrap();

                    let mut server = RemoteDataStoreServer::new(
                        Arc::new(Mutex::new(data_store)),
                        ByteConPublicKey::new(ByteConCertificate::FilePath(server_public_key_file_path)),
                        ByteConPrivateKey::new(ByteConCertificate::FilePath(server_private_key_file_path)),
                        String::from("localhost"),
                        port,
                    );
                    let start_result = server.start()
                        .await
                        .map_err(|error| {
                            format!("Error within server task: {:?}", error)
                        });
                    if let Err(error) = start_result {
                        *server_task_error
                            .lock()
                            .await = Some(error);
                        break 'process_thread;
                    }
                }
            })
        };
        println!("started server task");

        println!("sleeping...");

        // wait for the server to start listening
        sleep(Duration::from_millis(1000)).await;
        println!("sleeping done");

        if let Some(error) = server_task_error.lock().await.as_ref() {
            eprintln!("{}", error);
        }
        assert!(server_task_error.lock().await.is_none());

        let mut client: RemoteDataStoreClient = RemoteDataStoreClient::new(
            ByteConPublicKey::new(ByteConCertificate::FilePath(server_public_key_tempfile.path().into())),
            String::from("localhost"),
            String::from("localhost"),
            port,
        );

        if _server_task.is_finished() {
            eprintln!("Server task has already ended for some reason.");
        }

        client.initialize()
            .await
            .expect("Failed to initialize client.");

        let original_bytes_collection = vec![
            vec![1u8],
            vec![2u8, 3u8],
            vec![4u8, 5u8, 6u8],
        ];
        let ids = client.bulk_insert(original_bytes_collection.clone())
            .await
            .expect("Failed to perform bulk insert.");

        // try in the original ID order
        {
            let retrieved_bytes_collection = client.bulk_get(&ids)
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

            let retrieved_bytes_collection = client.bulk_get(&reversed_ids)
                .await
                .expect("Failed to bulk get by IDs.");

            assert_ne!(original_bytes_collection, retrieved_bytes_collection);

            for (top_down, down_up) in (ids.len() - 1..=0).zip(0..ids.len()) {
                assert_eq!(original_bytes_collection[down_up], retrieved_bytes_collection[top_down]);
            }
        }
    }

    #[tokio::test]
    async fn test_n4c6_initialize_postgres_data_store() {
        let port = 8083;
        let postgres_connection_string = String::from("host=localhost user=user password=password dbname=database");

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
        let server_task_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let data_store: Arc<Mutex<PostgresDataStore>> = Arc::new(Mutex::new(PostgresDataStore::new(
            postgres_connection_string,
        )));
        let _server_task = {
            let data_store = data_store.clone();
            let server_task_error = server_task_error.clone();
            let server_public_key_file_path: PathBuf = server_public_key_tempfile.path().into();
            let server_private_key_file_path: PathBuf = server_private_key_tempfile.path().into();
            tokio::spawn(async move {
                'process_thread: {
                    let initialization_result = data_store
                        .lock()
                        .await
                        .initialize()
                        .await
                        .map_err(|error| {
                            format!("Error within server task: {:?}", error)
                        });
                    if let Err(error) = initialization_result {
                        *server_task_error
                            .lock()
                            .await = Some(error);
                        break 'process_thread;
                    }
                    initialization_result.unwrap();

                    let mut server = RemoteDataStoreServer::new(
                        data_store,
                        ByteConPublicKey::new(ByteConCertificate::FilePath(server_public_key_file_path)),
                        ByteConPrivateKey::new(ByteConCertificate::FilePath(server_private_key_file_path)),
                        String::from("localhost"),
                        port,
                    );
                    let start_result = server.start()
                        .await
                        .map_err(|error| {
                            format!("Error within server task when trying to start: {:?}", error)
                        });
                    if let Err(error) = start_result {
                        *server_task_error
                            .lock()
                            .await = Some(error);
                        break 'process_thread;
                    }
                }
            })
        };
        println!("started server task");

        println!("sleeping...");

        // wait for the server to start listening
        sleep(Duration::from_millis(1000)).await;
        println!("sleeping done");

        if let Some(error) = server_task_error.lock().await.as_ref() {
            eprintln!("{}", error);
        }
        assert!(server_task_error.lock().await.is_none());

        let mut client = RemoteDataStoreClient::new(
            ByteConPublicKey::new(ByteConCertificate::FilePath(server_public_key_tempfile.path().into())),
            String::from("localhost"),
            String::from("localhost"),
            port,
        );
        match client.initialize().await {
            Ok(_) => { },
            Err(client_error) => {
                if let Some(server_error) = &*server_task_error
                    .lock()
                    .await {
                    
                    eprintln!("Server error: {}", server_error);
                }
                eprintln!("Client error: {:?}", client_error);
                panic!("Error encountered while initializing client.");
            }
        }

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

        let list = client.list(1, 4, 2)
            .await
            .expect("Failed to get list of IDs.");

        assert_eq!(4, list.len());
        assert_eq!(7, list[0]);
        assert_eq!(8, list[1]);
        assert_eq!(9, list[2]);
        assert_eq!(10, list[3]);

        let mut random = rand::rngs::StdRng::from_entropy();
        ids.shuffle(&mut random);

        for id in ids {
            client.delete(&id)
                .await
                .expect(&format!("Failed to delete ID {}", id));
        }

        data_store
            .lock()
            .await
            .reset()
            .await
            .unwrap();
    }
}