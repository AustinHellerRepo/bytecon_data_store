use std::{error::Error, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::Mutex};
use tokio_rustls::{rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig, ServerName}, TlsAcceptor, TlsConnector, TlsStream};
use crate::DataStore;
use rand::{rngs::StdRng, Rng, SeedableRng};

struct TlsStreamWrapper(TlsStream<TcpStream>);

impl TlsStreamWrapper {
    async fn read_all_bytes(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 64];

        let mut initial_packet = [0u8; 8];
        let _ = self.0.read_exact(&mut initial_packet)
            .await?;

        let expected_bytes_length: u64 = u64::from_le_bytes(initial_packet);
        while (bytes.len() as u64) < expected_bytes_length {
            let read_bytes_length = self.0.read(&mut chunk)
                .await?;

            if read_bytes_length != 0 {
                bytes.extend_from_slice(&chunk[..read_bytes_length]);
            }
        }

        Ok(bytes)
    }
    async fn write_all_bytes(&mut self, bytes: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let bytes_length: u64 = bytes.len() as u64;

        let bytes_length_bytes = bytes_length.to_le_bytes();

        self.0.write(&bytes_length_bytes)
            .await?;

        self.0.write(&bytes)
            .await?;

        Ok(())
    }
}

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
    async fn connect(&self) -> Result<TlsStreamWrapper, Box<dyn Error>> {
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
        let tcp_stream = TcpStream::connect(connecting_address).await?;
        let server_name = ServerName::try_from(self.server_domain.as_str())?;
        let tls_stream = connector.connect(server_name, tcp_stream).await?;

        Ok(TlsStreamWrapper(TlsStream::Client(tls_stream)))
    }
    async fn send_request(&self, server_request: ServerRequest) -> Result<ServerResponse, Box<dyn Error>> {
        let mut tls_stream_wrapper = self.connect()
            .await?;
        server_request.write_to_stream(&mut tls_stream_wrapper)
            .await?;
        let server_response = ServerResponse::read_from_stream(&mut tls_stream_wrapper)
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
        let health_check_response = self.send_request(health_check_request.clone())
            .await?;
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
        let server_response = self.send_request(server_request.clone())
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
        let server_response = self.send_request(server_request.clone())
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
        let server_response = self.send_request(server_request.clone())
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
        data_store: TDataStore,
        public_key_file_path: PathBuf,
        private_key_file_path: PathBuf,
        bind_address: String,
        bind_port: u16
    ) -> Self {
        Self {
            data_store: Arc::new(Mutex::new(data_store)),
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

                            let mut tls_stream_wrapper = TlsStreamWrapper(TlsStream::Server(stream));
                            let server_request = ServerRequest::read_from_stream(&mut tls_stream_wrapper)
                                .await?;

                            match server_request {
                                ServerRequest::HealthCheck { nonce } => {
                                    let server_response = ServerResponse::HealthCheck {
                                        nonce,
                                    };
                                    server_response.write_to_stream(&mut tls_stream_wrapper)
                                        .await?;
                                    Ok(())
                                },
                                ServerRequest::SendBytes { bytes } => {
                                    let key = data_store
                                        .lock()
                                        .await
                                        .insert(bytes)
                                        .await?;

                                    let server_response = ServerResponse::SentBytes {
                                        id: key,
                                    };
                                    server_response.write_to_stream(&mut tls_stream_wrapper)
                                        .await?;
                                    Ok(())
                                },
                                ServerRequest::GetBytes { id } => {
                                    let bytes = data_store
                                        .lock()
                                        .await
                                        .get(&id)
                                        .await?;

                                    let server_response = ServerResponse::ReceivedBytes {
                                        bytes,
                                    };
                                    server_response.write_to_stream(&mut tls_stream_wrapper)
                                        .await?;
                                    Ok(())
                                },
                                ServerRequest::Delete { id } => {
                                    data_store
                                        .lock()
                                        .await
                                        .delete(&id)
                                        .await?;

                                    let server_response = ServerResponse::Deleted {
                                        id,
                                    };
                                    server_response.write_to_stream(&mut tls_stream_wrapper)
                                        .await?;
                                    Ok(())
                                }
                            }
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

trait StreamReaderWriter {
    async fn write_to_stream(&self, tls_stream: &mut TlsStreamWrapper) -> Result<(), Box<dyn Error>>;
    async fn read_from_stream(tls_stream: &mut TlsStreamWrapper) -> Result<Self, Box<dyn Error>> where Self: Sized;
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
}

impl StreamReaderWriter for ServerRequest {
    async fn write_to_stream(&self, tls_stream_wrapper: &mut TlsStreamWrapper) -> Result<(), Box<dyn Error>> {
        let mut sending_bytes: Vec<u8> = Vec::new();
        
        match self {
            ServerRequest::HealthCheck { nonce }=> {
                sending_bytes.push(0);
                sending_bytes.extend_from_slice(&nonce.to_le_bytes());
            },
            ServerRequest::SendBytes { bytes } => {
                sending_bytes.push(1);
                sending_bytes.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                sending_bytes.extend_from_slice(&bytes);
            },
            ServerRequest::GetBytes { id } => {
                sending_bytes.push(2);
                sending_bytes.extend_from_slice(&id.to_le_bytes());
            },
            ServerRequest::Delete { id } => {
                sending_bytes.push(3);
                sending_bytes.extend_from_slice(&id.to_le_bytes());
            },
        }
        tls_stream_wrapper.write_all_bytes(sending_bytes).await?;

        Ok(())
    }
    async fn read_from_stream(tls_stream_wrapper: &mut TlsStreamWrapper) -> Result<ServerRequest, Box<dyn Error>> {

        // read all bytes from the TlsStreamWrapper
        let bytes = tls_stream_wrapper.read_all_bytes()
            .await?;
        let bytes_length = bytes.len();

        // we are done reading, so begin parsing outcome
        let mut index = 0;
        let enum_variant_id = bytes[index];
        index += 1;

        match enum_variant_id {
            0 => {
                if index + 16 > bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index + 16,
                    }.into());
                }

                let nonce = u128::from_le_bytes(bytes[index..index + 16].try_into()?);
                index += 16;

                if index != bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index,
                    }.into());
                }

                return Ok(ServerRequest::HealthCheck {
                    nonce,
                });
            },
            1 => {
                if index + 4 > bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index + 4,
                    }.into());
                }

                let server_request_bytes_length = usize::try_from(u32::from_le_bytes(bytes[index..index + 4].try_into()?))?;
                index += 4;

                if index + server_request_bytes_length > bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index + server_request_bytes_length,
                    }.into());
                }

                let mut server_request_bytes = Vec::with_capacity(server_request_bytes_length);
                server_request_bytes.extend_from_slice(&bytes[index..index + server_request_bytes_length]);
                index += server_request_bytes_length;

                if index != bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index,
                    }.into());
                }

                return Ok(ServerRequest::SendBytes {
                    bytes: server_request_bytes,
                });
            },
            2 => {
                if index + 8 > bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index + 8,
                    }.into());
                }

                let id = i64::from_le_bytes(bytes[index..index + 8].try_into()?);
                index += 8;

                if index != bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index,
                    }.into());
                }

                return Ok(ServerRequest::GetBytes {
                    id,
                });
            },
            3 => {
                if index + 8 > bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index + 8,
                    }.into());
                }

                let id = i64::from_le_bytes(bytes[index..index + 8].try_into()?);
                index += 8;

                if index != bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index,
                    }.into());
                }

                return Ok(ServerRequest::Delete {
                    id,
                });
            },
            _ => {
                return Err(RemoteDataStoreError::UnexpectedEnumVariantByte {
                    variant_byte: enum_variant_id,
                }.into());
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
}

impl StreamReaderWriter for ServerResponse {
    async fn write_to_stream(&self, tls_stream_wrapper: &mut TlsStreamWrapper) -> Result<(), Box<dyn Error>> {
        let mut sending_bytes: Vec<u8> = Vec::new();
        
        match self {
            ServerResponse::HealthCheck { nonce } => {
                sending_bytes.push(0);
                sending_bytes.extend_from_slice(&nonce.to_le_bytes());
            },
            ServerResponse::ReceivedBytes { bytes } => {
                sending_bytes.push(1);
                sending_bytes.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                sending_bytes.extend_from_slice(&bytes);
            },
            ServerResponse::SentBytes { id } => {
                sending_bytes.push(2);
                sending_bytes.extend_from_slice(&id.to_le_bytes());
            },
            ServerResponse::Deleted { id } => {
                sending_bytes.push(3);
                sending_bytes.extend_from_slice(&id.to_le_bytes());
            },
        }
        tls_stream_wrapper.write_all_bytes(sending_bytes).await?;

        Ok(())
    }
    async fn read_from_stream(tls_stream_wrapper: &mut TlsStreamWrapper) -> Result<ServerResponse, Box<dyn Error>> {
        let bytes = tls_stream_wrapper.read_all_bytes()
            .await?;
        let bytes_length = bytes.len();

        // we are done reading, so begin parsing outcome
        let mut index = 0;
        let enum_variant_id = bytes[index];
        index += 1;

        match enum_variant_id {
            0 => {
                if index + 16 > bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index + 16,
                    }.into());
                }

                let nonce = u128::from_le_bytes(bytes[index..index + 16].try_into()?);
                index += 16;

                if index != bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index,
                    }.into());
                }

                return Ok(ServerResponse::HealthCheck {
                    nonce,
                });
            },
            1 => {
                if index + 4 > bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index + 4,
                    }.into());
                }

                let server_response_bytes_length = usize::try_from(u32::from_le_bytes(bytes[index..index + 4].try_into()?))?;
                index += 4;

                if index + server_response_bytes_length > bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index + server_response_bytes_length,
                    }.into());
                }

                let mut server_response_bytes = Vec::with_capacity(server_response_bytes_length);
                server_response_bytes.extend_from_slice(&bytes[index..index + server_response_bytes_length]);
                index += server_response_bytes_length;

                if index != bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index,
                    }.into());
                }

                return Ok(ServerResponse::ReceivedBytes {
                    bytes: server_response_bytes,
                });
            },
            2 => {
                if index + 8 > bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index + 8,
                    }.into());
                }

                let id = i64::from_le_bytes(bytes[index..index + 8].try_into()?);
                index += 8;

                if index != bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index,
                    }.into());
                }

                return Ok(ServerResponse::SentBytes {
                    id,
                });
            },
            3 => {
                if index + 8 > bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index + 8,
                    }.into());
                }

                let id = i64::from_le_bytes(bytes[index..index + 8].try_into()?);
                index += 8;

                if index != bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index,
                    }.into());
                }

                return Ok(ServerResponse::Deleted {
                    id,
                });
            },
            _ => {
                return Err(RemoteDataStoreError::UnexpectedEnumVariantByte {
                    variant_byte: enum_variant_id,
                }.into());
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
    #[error("Unexpected number of bytes received {received_bytes_length} after parsing the enum variant ID {enum_variant_id} based on {parsed_bytes_length} bytes.")]
    UnexpectedNumberOfBytesParsed {
        received_bytes_length: usize,
        enum_variant_id: u8,
        parsed_bytes_length: usize,
    },
    #[error("Unexpected enum variant byte {variant_byte}.")]
    UnexpectedEnumVariantByte {
        variant_byte: u8,
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
}