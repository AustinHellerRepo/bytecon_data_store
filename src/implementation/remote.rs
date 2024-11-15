use std::{error::Error, path::PathBuf, sync::Arc};
use bytecon::ByteConverter;
use server_client_bytecon::{ByteConClient, ByteConServer, MessageProcessor};
use tokio::sync::Mutex;
use crate::DataStore;

pub struct RemoteDataStoreClient {
    client: ByteConClient<ServerRequest, ServerResponse>,
}

impl RemoteDataStoreClient {
    pub fn new(
        server_public_key_file_path: PathBuf,
        server_domain: String,
        server_address: String,
        server_port: u16,
    ) -> Self {
        Self {
            client: ByteConClient::new(
                server_address,
                server_port,
                server_public_key_file_path,
                server_domain,
            ),
        }
    }
    async fn send_request(&self, server_request: &ServerRequest) -> Result<ServerResponse, Box<dyn Error>> {
        return Ok(self.client.send_message(server_request).await?);
    }
}

impl DataStore for RemoteDataStoreClient {
    type Item = Vec<u8>;
    type Key = i64;

    async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let health_check_request = ServerRequest::HealthCheck;
        let health_check_response = self.send_request(&health_check_request)
            .await
            .map_err(|error| {
                format!("Error trying to send request: {:?}", error)
            })?;
        match health_check_response {
            ServerResponse::HealthCheck => {
                Ok(())
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

struct RemoteDataStoreMessageProcessor<TDataStore>
where
    TDataStore: DataStore<Item = Vec<u8>, Key = i64> + Send + Sync + 'static,
{
    data_store: Arc<Mutex<TDataStore>>
}

impl<TDataStore> MessageProcessor for RemoteDataStoreMessageProcessor<TDataStore>
where
    TDataStore: DataStore<Item = Vec<u8>, Key = i64> + Send + Sync + 'static,
{
    type TInput = ServerRequest;
    type TOutput = ServerResponse;

    async fn process_message(&self, message: &Self::TInput) -> Result<Self::TOutput, Box<dyn Error>> {
        let server_response = match message {
            ServerRequest::HealthCheck => {
                ServerResponse::HealthCheck
            },
            ServerRequest::SendBytes { bytes } => {
                let key = self.data_store
                    .lock()
                    .await
                    .insert(bytes.to_vec())
                    .await?;

                ServerResponse::SentBytes {
                    id: key,
                }
            },
            ServerRequest::GetBytes { id } => {
                let bytes = self.data_store
                    .lock()
                    .await
                    .get(&id)
                    .await?;

                ServerResponse::ReceivedBytes {
                    bytes,
                }
            },
            ServerRequest::Delete { id } => {
                self.data_store
                    .lock()
                    .await
                    .delete(&id)
                    .await?;

                ServerResponse::Deleted {
                    id: *id,
                }
            },
            ServerRequest::ListIds { page_index, page_size, row_offset } => {
                let ids = self.data_store
                    .lock()
                    .await
                    .list(*page_index, *page_size, *row_offset)
                    .await?;

                ServerResponse::ReceivedIdList {
                    ids,
                }
            }
        };
        Ok(server_response)
    }
}

pub struct RemoteDataStoreServer<TDataStore>
where
    TDataStore: DataStore<Item = Vec<u8>, Key = i64> + Send + Sync + 'static,
{
    server: ByteConServer<RemoteDataStoreMessageProcessor<TDataStore>>,
}

impl<TDataStore: DataStore<Item = Vec<u8>, Key = i64> + Send + Sync + 'static> RemoteDataStoreServer<TDataStore> {
    pub fn new(
        data_store: Arc<Mutex<TDataStore>>,
        public_key_file_path: PathBuf,
        private_key_file_path: PathBuf,
        bind_address: String,
        bind_port: u16,
    ) -> Self {
        Self {
            server: ByteConServer::<RemoteDataStoreMessageProcessor<TDataStore>>::new(
                bind_address,
                bind_port,
                public_key_file_path,
                private_key_file_path,
                Arc::new(RemoteDataStoreMessageProcessor {
                    data_store,
                }),
            )
        }
    }
    pub async fn start(&mut self) -> Result<(), Box<dyn Error>> {
        self.server.start()
            .await
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
            ServerRequest::HealthCheck => {
                // byte
                bytes.push(0);
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
                Ok(Self::HealthCheck)
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
    HealthCheck,
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
            ServerResponse::HealthCheck => {
                // byte
                bytes.push(0);
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
                Ok(Self::HealthCheck)
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