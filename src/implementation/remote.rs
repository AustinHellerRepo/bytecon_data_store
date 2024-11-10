use std::{error::Error, net::SocketAddr, path::PathBuf, sync::Arc};
use bytecon::{ByteConverter, ByteStreamReaderAsync, ByteStreamWriterAsync};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::Mutex};
use tokio_rustls::{rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig, ServerName}, TlsAcceptor, TlsConnector, TlsStream};
use crate::DataStore;
use rand::{rngs::StdRng, Rng, SeedableRng};

//struct TlsStreamWrapper(TlsStream<TcpStream>);
//
//impl TlsStreamWrapper {
//    async fn read_all_bytes(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
//        let mut bytes = Vec::new();
//        let mut chunk = [0u8; 64];
//
//        let mut initial_packet = [0u8; 8];
//        let _ = self.0.read_exact(&mut initial_packet)
//            .await?;
//
//        let expected_bytes_length: u64 = u64::from_le_bytes(initial_packet);
//        while (bytes.len() as u64) < expected_bytes_length {
//            let read_bytes_length = self.0.read(&mut chunk)
//                .await?;
//
//            if read_bytes_length != 0 {
//                bytes.extend_from_slice(&chunk[..read_bytes_length]);
//            }
//        }
//
//        Ok(bytes)
//    }
//    async fn write_all_bytes(&mut self, bytes: Vec<u8>) -> Result<(), Box<dyn Error>> {
//        let bytes_length: u64 = bytes.len() as u64;
//
//        let bytes_length_bytes = bytes_length.to_le_bytes();
//
//        self.0.write(&bytes_length_bytes)
//            .await?;
//
//        self.0.write(&bytes)
//            .await?;
//
//        Ok(())
//    }
//}

pub struct RemoteDataStoreClient {
    server_public_key_file_path: PathBuf,
    server_domain: String,
    server_address: String,
    server_port: u16,
    nonce_generator: StdRng,
}

impl RemoteDataStoreClient {
    pub fn new(
        server_public_key_file_path: PathBuf,
        server_domain: String,
        server_address: String,
        server_port: u16,
    ) -> Self {
        Self {
            server_public_key_file_path,
            server_domain,
            server_address,
            server_port,
            nonce_generator: StdRng::from_entropy(),
        }
    }
    async fn connect(&self) -> Result<TlsStream<TcpStream>, Box<dyn Error>> {
        // load TLS client configuration
        let config = {
            let cert_file = std::fs::File::open(&self.server_public_key_file_path)?;
            let mut reader = std::io::BufReader::new(cert_file);
            let certs = rustls_pemfile::certs(&mut reader)?;

            let mut root_cert_store = RootCertStore::empty();
            for cert in certs {
                root_cert_store.add(&Certificate(cert))?;
            }

            ClientConfig::builder()
                .with_safe_defaults()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth()
        };

        let connector = TlsConnector::from(Arc::new(config));

        // connect to the server over TCP
        let connecting_address = format!("{}:{}", self.server_address, self.server_port);
        println!("RemoteDataStoreClient connecting to address {}", connecting_address);
        let tcp_stream = TcpStream::connect(connecting_address.clone()).await?;
        println!("RemoteDataStoreClient connected to address {}", connecting_address);
        let server_name = ServerName::try_from(self.server_domain.as_str())?;
        println!("RemoteDataStoreClient connecting to TLS with server name {:?}", server_name);
        let tls_stream = connector.connect(server_name.clone(), tcp_stream).await?;
        println!("RemoteDataStoreClient connected to TLS with server name {:?}", server_name);

        Ok(TlsStream::Client(tls_stream))
    }
    async fn send_request(&self, server_request: &ServerRequest) -> Result<ServerResponse, Box<dyn Error>> {
        let mut tls_stream = self.connect()
            .await?;
        tls_stream.write_from_byte_converter(server_request)
            .await?;
        let server_response = tls_stream.read_to_byte_converter::<ServerResponse>()
            .await?;
        
        return Ok(server_response);
    }
}

impl DataStore for RemoteDataStoreClient {
    type Item = Vec<u8>;
    type Key = i64;

    async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let request_nonce: u128 = self.nonce_generator.gen();
        let health_check_request = ServerRequest::HealthCheck {
            nonce: request_nonce,
        };
        let health_check_response = self.send_request(&health_check_request)
            .await
            .map_err(|error| {
                format!("Error trying to send request: {:?}", error)
            })?;
        match health_check_response {
            ServerResponse::HealthCheck { nonce: response_nonce } => {
                if request_nonce != response_nonce {
                    Err(RemoteDataStoreError::NonceMismatch {
                        response_nonce,
                        request_nonce,
                    }.into())
                }
                else {
                    Ok(())
                }
            },
            _ => {
                Err(RemoteDataStoreError::UnexpectedResponseForRequest {
                    request: health_check_request,
                    response: health_check_response,
                }.into())
            }
        }
    }
    async fn insert(&mut self, item: Self::Item) -> Result<Self::Key, Box<dyn std::error::Error>> {
        let server_request = ServerRequest::SendBytes {
            bytes: item,
        };
        let server_response = self.send_request(&server_request)
            .await?;
        if let ServerResponse::SentBytes { id } = server_response {
            Ok(id)
        }
        else {
            Err(RemoteDataStoreError::UnexpectedResponseForRequest {
                response: server_response,
                request: server_request,
            }.into())
        }
    }
    async fn get(&self, id: &Self::Key) -> Result<Self::Item, Box<dyn std::error::Error>> {
        let server_request = ServerRequest::GetBytes {
            id: *id,
        };
        let server_response = self.send_request(&server_request)
            .await?;
        if let ServerResponse::ReceivedBytes { bytes } = server_response {
            Ok(bytes)
        }
        else {
            Err(RemoteDataStoreError::UnexpectedResponseForRequest {
                response: server_response,
                request: server_request,
            }.into())
        }
    }
    async fn delete(&self, id: &Self::Key) -> Result<(), Box<dyn std::error::Error>> {
        
        // copy key into owned object
        let id = *id;

        let server_request = ServerRequest::Delete {
            id: id,
        };
        let server_response = self.send_request(&server_request)
            .await?;
        if let ServerResponse::Deleted { id: response_file_record_id } = server_response {
            if id != response_file_record_id {
                Err(RemoteDataStoreError::DeleteMismatch {
                    request_file_record_id: id,
                    response_file_record_id,
                }.into())
            }
            else {
                Ok(())
            }
        }
        else {
            Err(RemoteDataStoreError::UnexpectedResponseForRequest {
                response: server_response,
                request: server_request,
            }.into())
        }
    }
    async fn list(&self, page_index: u64, page_size: u64, row_offset: u64) -> Result<Vec<Self::Key>, Box<dyn std::error::Error>> {
        let server_request = ServerRequest::ListIds {
            page_index,
            page_size,
            row_offset,
        };
        let server_response = self.send_request(&server_request)
            .await?;
        if let ServerResponse::ReceivedIdList { ids } = server_response {
            Ok(ids)
        }
        else {
            Err(RemoteDataStoreError::UnexpectedResponseForRequest {
                response: server_response,
                request: server_request,
            }.into())
        }
    }
}

pub struct RemoteDataStoreServer<TDataStore: DataStore> {
    data_store: Arc<Mutex<TDataStore>>,
    public_key_file_path: PathBuf,
    private_key_file_path: PathBuf,
    bind_address: String,
    bind_port: u16,
}

impl<TDataStore: DataStore<Item = Vec<u8>, Key = i64> + Send + Sync + 'static> RemoteDataStoreServer<TDataStore> {
    pub fn new(
        data_store: Arc<Mutex<TDataStore>>,
        public_key_file_path: PathBuf,
        private_key_file_path: PathBuf,
        bind_address: String,
        bind_port: u16
    ) -> Self {
        Self {
            data_store,
            public_key_file_path,
            private_key_file_path,
            bind_address,
            bind_port,
        }
    }
    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        // load certs
        let certs = {
            let cert_file = std::fs::File::open(&self.public_key_file_path)?;
            let mut reader = std::io::BufReader::new(cert_file);
            let certs = rustls_pemfile::certs(&mut reader)?
                .into_iter()
                .map(Certificate)
                .collect();
            certs
        };

        let key = {
            let key_file = std::fs::File::open(&self.private_key_file_path)?;
            let mut reader = std::io::BufReader::new(key_file);
            let keys = rustls_pemfile::pkcs8_private_keys(&mut reader)?;
            PrivateKey(keys[0].clone())
        };

        // configure the TLS server
        let tls_config = ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;
        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));

        // bind TCP listener
        let listening_address = format!("{}:{}", self.bind_address, self.bind_port);
        println!("Server binding to address {}", listening_address);
        let listener = TcpListener::bind(&listening_address).await?;

        loop {
            let (tcp_stream, client_address) = listener.accept().await?;
            let tls_acceptor = tls_acceptor.clone();
            let data_store = self.data_store.clone();

            println!("{}: received connection from {}", chrono::Utc::now(), client_address);

            let _process_task = tokio::spawn(async move {

                async fn process_function<TDataStore: DataStore<Item = Vec<u8>, Key = i64> + Send + Sync + 'static>(client_address: SocketAddr, data_store: Arc<Mutex<TDataStore>>, tcp_stream: TcpStream, tls_acceptor: TlsAcceptor) -> Result<(), Box<dyn Error>> {
                    match tls_acceptor.accept(tcp_stream).await {
                        Ok(stream) => {

                            let mut tls_stream = TlsStream::Server(stream);
                            let server_request = tls_stream.read_to_byte_converter::<ServerRequest>()
                                .await?;

                            let server_response = match server_request {
                                ServerRequest::HealthCheck { nonce } => {
                                    ServerResponse::HealthCheck {
                                        nonce,
                                    }
                                },
                                ServerRequest::SendBytes { bytes } => {
                                    let key = data_store
                                        .lock()
                                        .await
                                        .insert(bytes)
                                        .await?;

                                    ServerResponse::SentBytes {
                                        id: key,
                                    }
                                },
                                ServerRequest::GetBytes { id } => {
                                    let bytes = data_store
                                        .lock()
                                        .await
                                        .get(&id)
                                        .await?;

                                    ServerResponse::ReceivedBytes {
                                        bytes,
                                    }
                                },
                                ServerRequest::Delete { id } => {
                                    data_store
                                        .lock()
                                        .await
                                        .delete(&id)
                                        .await?;

                                    ServerResponse::Deleted {
                                        id,
                                    }
                                },
                                ServerRequest::ListIds { page_index, page_size, row_offset } => {
                                    let ids = data_store
                                        .lock()
                                        .await
                                        .list(page_index, page_size, row_offset)
                                        .await?;

                                    ServerResponse::ReceivedIdList {
                                        ids,
                                    }
                                }
                            };
                            tls_stream.write_from_byte_converter(&server_response)
                                .await?;
                            Ok(())
                        },
                        Err(e) => {
                            eprintln!("{}: failed to accept TLS connection from client {} with error {:?}.", chrono::Utc::now(), client_address, e);
                            Err(e.into())
                        }
                    }
                }

                match process_function(client_address, data_store, tcp_stream, tls_acceptor).await {
                    Ok(_) => {
                        println!("{}: processed request from client {}.", chrono::Utc::now(), client_address);
                    },
                    Err(error) => {
                        eprintln!("{}: failed to process request from client {} with error {:?}.", chrono::Utc::now(), client_address, error);
                    }
                }
            });
        }
    }
}

#[derive(Clone, Debug)]
enum ServerRequest {
    HealthCheck {
        nonce: u128,
    },
    SendBytes {
        bytes: Vec<u8>,
    },
    GetBytes {
        id: i64,
    },
    Delete {
        id: i64,
    },
    ListIds {
        page_index: u64,
        page_size: u64,
        row_offset: u64,
    },
}

impl ByteConverter for ServerRequest {
    fn append_to_bytes(&self, bytes: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
        match self {
            ServerRequest::HealthCheck { nonce }=> {
                // byte
                bytes.push(0);
                // u128
                nonce.append_to_bytes(bytes)?;
            },
            ServerRequest::SendBytes { bytes: send_bytes } => {
                // byte
                bytes.push(1);
                // vec<u8>
                send_bytes.append_to_bytes(bytes)?;
            },
            ServerRequest::GetBytes { id } => {
                // byte
                bytes.push(2);
                // i64
                id.append_to_bytes(bytes)?;
            },
            ServerRequest::Delete { id } => {
                // byte
                bytes.push(3);
                // i64
                id.append_to_bytes(bytes)?;
            },
            ServerRequest::ListIds { page_index, page_size, row_offset } => {
                // byte
                bytes.push(4);
                // u64
                page_index.append_to_bytes(bytes)?;
                // u64
                page_size.append_to_bytes(bytes)?;
                // u64
                row_offset.append_to_bytes(bytes)?;
            },
        }
        Ok(())
    }
    fn extract_from_bytes(bytes: &Vec<u8>, index: &mut usize) -> Result<Self, Box<dyn Error>> where Self: Sized {
        // byte
        let enum_variant_byte = u8::extract_from_bytes(bytes, index)?;

        match enum_variant_byte {
            0 => {
                Ok(Self::HealthCheck {
                    nonce: u128::extract_from_bytes(bytes, index)?,
                })
            },
            1 => {
                Ok(Self::SendBytes {
                    bytes: Vec::<u8>::extract_from_bytes(bytes, index)?,
                })
            },
            2 => {
                Ok(Self::GetBytes {
                    id: i64::extract_from_bytes(bytes, index)?,
                })
            },
            3 => {
                Ok(Self::Delete {
                    id: i64::extract_from_bytes(bytes, index)?,
                })
            },
            4 => {
                Ok(Self::ListIds {
                    page_index: u64::extract_from_bytes(bytes, index)?,
                    page_size: u64::extract_from_bytes(bytes, index)?,
                    row_offset: u64::extract_from_bytes(bytes, index)?,
                })
            },
            _ => {
                Err(RemoteDataStoreError::UnexpectedEnumVariantByte {
                    enum_variant_byte,
                    enum_variant_name: String::from(std::any::type_name::<Self>()),
                }.into())
            }
        }
    }
}

#[derive(Debug)]
enum ServerResponse {
    HealthCheck {
        nonce: u128,
    },
    SentBytes {
        id: i64,
    },
    ReceivedBytes {
        bytes: Vec<u8>,
    },
    Deleted {
        id: i64,
    },
    ReceivedIdList {
        ids: Vec<i64>,
    },
}

impl ByteConverter for ServerResponse {
    fn append_to_bytes(&self, bytes: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
        match self {
            ServerResponse::HealthCheck { nonce } => {
                // byte
                bytes.push(0);
                // u128
                nonce.append_to_bytes(bytes)?;
            },
            ServerResponse::ReceivedBytes { bytes: received_bytes } => {
                // byte
                bytes.push(1);
                // vec<u8>
                received_bytes.append_to_bytes(bytes)?;
            },
            ServerResponse::SentBytes { id } => {
                // byte
                bytes.push(2);
                // i64
                id.append_to_bytes(bytes)?;
            },
            ServerResponse::Deleted { id } => {
                // byte
                bytes.push(3);
                // i64
                id.append_to_bytes(bytes)?;
            },
            ServerResponse::ReceivedIdList { ids } => {
                // byte
                bytes.push(4);
                // vec<i64>
                ids.append_to_bytes(bytes)?;
            },
        }

        Ok(())
    }
    fn extract_from_bytes(bytes: &Vec<u8>, index: &mut usize) -> Result<Self, Box<dyn Error>> where Self: Sized {
        // enum variant byte
        let enum_variant_byte = u8::extract_from_bytes(bytes, index)?;

        match enum_variant_byte {
            0 => {
                Ok(Self::HealthCheck {
                    nonce: u128::extract_from_bytes(bytes, index)?,
                })
            },
            1 => {
                Ok(Self::ReceivedBytes {
                    bytes: Vec::<u8>::extract_from_bytes(bytes, index)?,
                })
            },
            2 => {
                Ok(Self::SentBytes {
                    id: i64::extract_from_bytes(bytes, index)?,
                })
            },
            3 => {
                Ok(Self::Deleted {
                    id: i64::extract_from_bytes(bytes, index)?,
                })
            },
            4 => {
                Ok(Self::ReceivedIdList {
                    ids: Vec::<i64>::extract_from_bytes(bytes, index)?,
                })
            },
            _ => {
                Err(RemoteDataStoreError::UnexpectedEnumVariantByte {
                    enum_variant_byte,
                    enum_variant_name: String::from(std::any::type_name::<Self>()),
                }.into())
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum RemoteDataStoreError {
    #[error("Unexpected response {response:?} based on request {request:?}.")]
    UnexpectedResponseForRequest {
        response: ServerResponse,
        request: ServerRequest,
    },
    #[error("Unexpected response nonce {response_nonce} that failed to match request nonce {request_nonce}")]
    NonceMismatch {
        response_nonce: u128,
        request_nonce: u128,
    },
    #[error("Unexpected deleted ID mismatch from request {request_file_record_id} and response {response_file_record_id}.")]
    DeleteMismatch {
        request_file_record_id: i64,
        response_file_record_id: i64,
    },
    #[error("Unexpected enum variant byte {enum_variant_byte} when trying to construct {enum_variant_name}.")]
    UnexpectedEnumVariantByte {
        enum_variant_byte: u8,
        enum_variant_name: String,
    },
}