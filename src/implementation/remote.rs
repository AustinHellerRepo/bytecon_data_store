use std::{error::Error, path::PathBuf, sync::Arc};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}};
use tokio_rustls::{rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig, ServerName}, TlsAcceptor, TlsConnector, TlsStream};
use crate::DataStore;

struct TlsStreamWrapper(TlsStream<TcpStream>);

impl TlsStreamWrapper {
    async fn read_all_bytes(&mut self) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut bytes = Vec::new();
        let mut chunk = [0u8; 64];

        let mut initial_packet = [0u8; 8];
        let mut read_bytes_length = self.0.read_exact(&mut initial_packet)
            .await?;

        println!("Read exact initial packet: {:?}", initial_packet);

        let expected_bytes_length: u64 = u64::from_le_bytes(initial_packet);

        println!("Expected bytes over TlsStream: {}", expected_bytes_length);
        
        while (bytes.len() as u64) < expected_bytes_length {
            read_bytes_length = self.0.read(&mut chunk)
                .await?;

            println!("Read chunk packet: {:?}", chunk);

            if read_bytes_length != 0 {
                bytes.extend_from_slice(&chunk[..read_bytes_length]);
            }
        }

        Ok(bytes)
    }
    async fn write_all_bytes(&mut self, bytes: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let bytes_length: u64 = bytes.len() as u64;

        let bytes_length_bytes = bytes_length.to_le_bytes();

        println!("Write exact initial packet: {:?}", bytes_length_bytes);

        self.0.write(&bytes_length_bytes)
            .await?;

        println!("Write chunked packets: {:?}", bytes);

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
        
        println!("Read server response: {:?}", server_response);

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

                    println!("Received connection on server from client.");

                    let mut tls_stream_wrapper = TlsStreamWrapper(TlsStream::Server(stream));
                    let server_request = ServerRequest::read_from_stream(&mut tls_stream_wrapper)
                        .await?;

                    println!("Read server request on server from client: {:?}", server_request);

                    match server_request {
                        ServerRequest::HealthCheck => {
                            let server_response = ServerResponse::HealthCheck;
                            server_response.write_to_stream(&mut tls_stream_wrapper)
                                .await?;
                        },
                        ServerRequest::SendBytes { bytes } => {
                            let key = self.data_store
                                .insert(bytes)
                                .await
                                .expect("Failed to insert into internal DataStore.");

                            println!("Sending to client key {}", key);

                            let server_response = ServerResponse::SentBytes {
                                id: key,
                            };
                            server_response.write_to_stream(&mut tls_stream_wrapper)
                                .await?;
                        },
                        ServerRequest::GetBytes { id } => {
                            let bytes = self.data_store
                                .get(&id)
                                .await?;

                            println!("Sending to client bytes {:?}", bytes);

                            let server_response = ServerResponse::ReceivedBytes {
                                bytes,
                            };
                            server_response.write_to_stream(&mut tls_stream_wrapper)
                                .await?;
                        },
                    }
                },
                Err(e) => {
                    eprintln!("Failed to accept TLS connection: {:?}", e);
                }
            }
        }

        panic!("Unexpected break from loop.");
    }
}

trait StreamReaderWriter {
    async fn write_to_stream(&self, tls_stream: &mut TlsStreamWrapper) -> Result<(), Box<dyn Error>>;
    async fn read_from_stream(tls_stream: &mut TlsStreamWrapper) -> Result<Self, Box<dyn Error>> where Self: Sized;
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
    async fn write_to_stream(&self, tls_stream_wrapper: &mut TlsStreamWrapper) -> Result<(), Box<dyn Error>> {
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
        tls_stream_wrapper.write_all_bytes(sending_bytes).await?;

        Ok(())
    }
    async fn read_from_stream(tls_stream_wrapper: &mut TlsStreamWrapper) -> Result<ServerRequest, Box<dyn Error>> {

        // read all bytes from the TlsStreamWrapper
        let bytes = tls_stream_wrapper.read_all_bytes()
            .await?;
        let bytes_length = bytes.len();

        println!("Read {} bytes from client.", bytes_length);

        // we are done reading, so begin parsing outcome
        let mut index = 0;
        let enum_variant_id = bytes[index];
        index += 1;

        match enum_variant_id {
            0 => {
                if index != bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index,
                    }.into());
                }

                return Ok(ServerRequest::HealthCheck);
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
    HealthCheck,
    SentBytes {
        id: i64,
    },
    ReceivedBytes {
        bytes: Vec<u8>,
    },
}

impl StreamReaderWriter for ServerResponse {
    async fn write_to_stream(&self, tls_stream_wrapper: &mut TlsStreamWrapper) -> Result<(), Box<dyn Error>> {
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
                if index != bytes_length {
                    return Err(RemoteDataStoreError::UnexpectedNumberOfBytesParsed {
                        received_bytes_length: bytes_length,
                        enum_variant_id,
                        parsed_bytes_length: index,
                    }.into());
                }

                return Ok(ServerResponse::HealthCheck);
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
}