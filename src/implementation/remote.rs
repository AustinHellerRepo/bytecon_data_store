use std::{error::Error, path::PathBuf, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use tokio_rustls::{rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig, ServerName}, TlsAcceptor, TlsConnector, TlsStream};

use crate::DataStore;

pub struct RemoteDataStoreClient {
    server_public_key_file_path: PathBuf,
    server_domain: String,
    server_address: String,
    server_port: u16,
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
        let tcp_stream = TcpStream::connect(connecting_address).await?;
        let server_name = ServerName::try_from(self.server_domain.as_str())?;
        let tls_stream = connector.connect(server_name, tcp_stream).await?;

        Ok(TlsStream::Client(tls_stream))
    }
    async fn send_request(&self, server_request: ServerRequest) -> Result<ServerResponse, Box<dyn Error>> {
        let mut tls_stream = self.connect()
            .await?;
        server_request.write_to_stream(&mut tls_stream)
            .await?;
        let server_response = ServerResponse::read_from_stream(&mut tls_stream)
            .await?;
        return Ok(server_response);
    }
}

impl DataStore for RemoteDataStoreClient {
    type Item = Vec<u8>;
    type Key = i64;

    async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let health_check_response = self.send_request(ServerRequest::HealthCheck)
            .await?;
        // getting back a response is good enough
        Ok(())
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
}

pub struct RemoteDataStoreServer<TDataStore: DataStore> {
    data_store: TDataStore,
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
            data_store: data_store,
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

        println!("Server listening on {listening_address}...");

        loop {
            let (tcp_stream, client_address) = listener.accept().await?;
            let tls_acceptor = tls_acceptor.clone();

            match tls_acceptor.accept(tcp_stream).await {
                Ok(stream) => {
                    let mut tls_stream = TlsStream::Server(stream);
                    let server_request = ServerRequest::read_from_stream(&mut tls_stream)
                        .await?;
                    match server_request {
                        ServerRequest::HealthCheck => {
                            let server_response = ServerResponse::HealthCheck;
                            server_response.write_to_stream(&mut tls_stream)
                                .await?;
                        },
                        ServerRequest::SendBytes { bytes } => {
                            let key = self.data_store
                                .insert(bytes)
                                .await
                                .expect("Failed to insert into internal DataStore.");
                            let server_response = ServerResponse::SentBytes {
                                id: key,
                            };
                            server_response.write_to_stream(&mut tls_stream)
                                .await?;
                        },
                        ServerRequest::GetBytes { id } => {
                            let bytes = self.data_store
                                .get(&id)
                                .await?;
                            let server_response = ServerResponse::ReceivedBytes {
                                bytes,
                            };
                            server_response.write_to_stream(&mut tls_stream)
                                .await?;
                        },
                    }
                },
                Err(e) => {
                    eprintln!("Failed to accept TLS connection: {:?}", e);
                }
            }
        }
    }
}

trait StreamReaderWriter {
    async fn write_to_stream(&self, tls_stream: &mut TlsStream<TcpStream>) -> Result<(), Box<dyn Error>>;
    async fn read_from_stream(tls_stream: &mut TlsStream<TcpStream>) -> Result<Self, Box<dyn Error>> where Self: Sized;
}

#[derive(Clone, Debug)]
enum ServerRequest {
    HealthCheck,
    SendBytes {
        bytes: Vec<u8>,
    },
    GetBytes {
        id: i64,
    },
}

impl StreamReaderWriter for ServerRequest {
    async fn write_to_stream(&self, tls_stream: &mut TlsStream<TcpStream>) -> Result<(), Box<dyn Error>> {
        let mut sending_bytes: Vec<u8> = Vec::new();
        
        match self {
            ServerRequest::HealthCheck => {
                sending_bytes.push(0);
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
        }
        tls_stream.write_all(&sending_bytes).await?;

        Ok(())
    }
    async fn read_from_stream(tls_stream: &mut TlsStream<TcpStream>) -> Result<ServerRequest, Box<dyn Error>> {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 1024];

        loop {
            let bytes_read_length = tls_stream.read(&mut chunk).await?;

            if bytes_read_length == 0 {
                // we are done reading, so begin parsing outcome
                let mut index = 0;
                let enum_variant_id = buffer[index];
                index += 1;

                match enum_variant_id {
                    0 => {
                        if index != bytes_read_length {
                            return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                                received_bytes_length: bytes_read_length,
                                enum_variant_id,
                                parsed_bytes_length: index,
                            }.into());
                        }

                        return Ok(ServerRequest::HealthCheck);
                    },
                    1 => {
                        if index + 4 > bytes_read_length {
                            return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                                received_bytes_length: bytes_read_length,
                                enum_variant_id,
                                parsed_bytes_length: index + 4,
                            }.into());
                        }

                        let bytes_length = usize::try_from(u32::from_le_bytes(buffer[index..index + 4].try_into()?))?;
                        index += 4;

                        if index + bytes_length > bytes_read_length {
                            return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                                received_bytes_length: bytes_read_length,
                                enum_variant_id,
                                parsed_bytes_length: index + bytes_length,
                            }.into());
                        }

                        let mut bytes = Vec::with_capacity(bytes_length);
                        bytes.extend_from_slice(&buffer[index..index + bytes_length]);
                        index += bytes_length;

                        if index != bytes_read_length {
                            return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                                received_bytes_length: bytes_read_length,
                                enum_variant_id,
                                parsed_bytes_length: index,
                            }.into());
                        }

                        return Ok(ServerRequest::SendBytes {
                            bytes,
                        });
                    },
                    2 => {
                        if index + 8 > bytes_read_length {
                            return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                                received_bytes_length: bytes_read_length,
                                enum_variant_id,
                                parsed_bytes_length: index + 8,
                            }.into());
                        }

                        let id = i64::from_le_bytes(buffer[..8].try_into()?);

                        return Ok(ServerRequest::GetBytes {
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

            buffer.extend_from_slice(&chunk[..bytes_read_length]);
        }
    }
}

#[derive(Debug)]
enum ServerResponse {
    HealthCheck,
    SentBytes {
        id: i64,
    },
    ReceivedBytes {
        bytes: Vec<u8>,
    },
}

impl StreamReaderWriter for ServerResponse {
    async fn write_to_stream(&self, tls_stream: &mut TlsStream<TcpStream>) -> Result<(), Box<dyn Error>> {
        let mut sending_bytes: Vec<u8> = Vec::new();
        
        match self {
            ServerResponse::HealthCheck => {
                sending_bytes.push(0);
            },
            ServerResponse::ReceivedBytes { bytes } => {
                sending_bytes.push(1);
                sending_bytes.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                sending_bytes.extend_from_slice(&bytes);
            },
            ServerResponse::SentBytes { id } => {
                sending_bytes.push(2);
                sending_bytes.extend_from_slice(&id.to_le_bytes());
            }
        }
        tls_stream.write_all(&sending_bytes).await?;

        Ok(())
    }
    async fn read_from_stream(tls_stream: &mut TlsStream<TcpStream>) -> Result<ServerResponse, Box<dyn Error>> {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 1024];

        loop {
            let bytes_read_length = tls_stream.read(&mut chunk).await?;

            if bytes_read_length == 0 {
                // we are done reading, so begin parsing outcome
                let mut index = 0;
                let enum_variant_id = buffer[index];
                index += 1;

                match enum_variant_id {
                    0 => {
                        if index != bytes_read_length {
                            return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                                received_bytes_length: bytes_read_length,
                                enum_variant_id,
                                parsed_bytes_length: index,
                            }.into());
                        }

                        return Ok(ServerResponse::HealthCheck);
                    },
                    1 => {
                        if index + 4 > bytes_read_length {
                            return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                                received_bytes_length: bytes_read_length,
                                enum_variant_id,
                                parsed_bytes_length: index + 4,
                            }.into());
                        }

                        let bytes_length = usize::try_from(u32::from_le_bytes(buffer[index..index + 4].try_into()?))?;
                        index += 4;

                        if index + bytes_length > bytes_read_length {
                            return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                                received_bytes_length: bytes_read_length,
                                enum_variant_id,
                                parsed_bytes_length: index + bytes_length,
                            }.into());
                        }

                        let mut bytes = Vec::with_capacity(bytes_length);
                        bytes.extend_from_slice(&buffer[index..index + bytes_length]);
                        index += bytes_length;

                        if index != bytes_read_length {
                            return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                                received_bytes_length: bytes_read_length,
                                enum_variant_id,
                                parsed_bytes_length: index,
                            }.into());
                        }

                        return Ok(ServerResponse::ReceivedBytes {
                            bytes,
                        });
                    },
                    2 => {
                        if index + 8 > bytes_read_length {
                            return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                                received_bytes_length: bytes_read_length,
                                enum_variant_id,
                                parsed_bytes_length: index + 8,
                            }.into());
                        }

                        let id = i64::from_le_bytes(buffer[..8].try_into()?);

                        return Ok(ServerResponse::SentBytes {
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

            buffer.extend_from_slice(&chunk[..bytes_read_length]);
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
}