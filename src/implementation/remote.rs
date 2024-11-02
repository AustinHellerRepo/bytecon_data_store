use std::{error::Error, path::PathBuf, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use tokio_rustls::{client::TlsStream, rustls::{Certificate, ClientConfig, RootCertStore, ServerName}, TlsConnector};

use crate::DataStore;

pub struct RemoteDataStoreClient {
    server_public_key_file_path: PathBuf,
    server_domain: String,
    server_address: String,
    server_port: u8,
}

impl RemoteDataStoreClient {
    pub fn new(
        server_public_key_file_path: PathBuf,
        server_domain: String,
        server_address: String,
        server_port: u8
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

        Ok(tls_stream)
    }
    fn send_request(&self, server_request: ServerRequest) -> Result<ServerResponse, Box<dyn Error>> {
        todo!()
    }
}

impl DataStore for RemoteDataStoreClient {
    type Item = Vec<u8>;
    type Key = i64;

    async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let health_check_response = self.send_request(ServerRequest::HealthCheck)?;
        // getting back a response is good enough
        Ok(())
    }
    async fn insert(&mut self, item: Self::Item) -> Result<Self::Key, Box<dyn std::error::Error>> {
        let server_request = ServerRequest::SendBytes {
            bytes: item,
        };
        let server_response = self.send_request(server_request.clone())?;
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
        let server_response = self.send_request(server_request.clone())?;
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
    bind_address: String,
    bind_port: u8,
}

impl<TDataStore: DataStore> RemoteDataStoreServer<TDataStore> {
    pub fn new(data_store: TDataStore, bind_address: String, bind_port: u8) -> Self {
        Self {
            data_store,
            bind_address,
            bind_port,
        }
    }
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

impl ServerRequest {
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
                        todo!()
                    },
                    2 => {
                        todo!()
                    },
                    _ => {
                        todo!()
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
}