use std::{error::Error, sync::Arc};
use bytecon::ByteConverter;
use cloneless_cow::ClonelessCow;
use server_client_bytecon::{ByteConClient, ByteConPrivateKey, ByteConPublicKey, ByteConServer, MessageProcessor};
use tokio::sync::Mutex;
use crate::DataStore;

pub struct RemoteDataStoreClient {
    client: ByteConClient<ServerRequest<'static>, ServerResponse>,
}

impl RemoteDataStoreClient {
    pub fn new(
        server_public_key: ByteConPublicKey,
        server_domain: String,
        server_address: String,
        server_port: u16,
    ) -> Self {
        Self {
            client: ByteConClient::new(
                server_address,
                server_port,
                server_public_key,
                server_domain,
            ),
        }
    }
    async fn send_request<'a>(&self, server_request: &ServerRequest<'a>) -> Result<ServerResponse, Box<dyn Error>> {
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
                    request: String::from(format!("{:?}", health_check_request)),
                    response: String::from(format!("{:?}", health_check_response)),
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
                response: String::from(format!("{:?}", server_response)),
                request: String::from(format!("{:?}", server_request)),
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
                response: String::from(format!("{:?}", server_response)),
                request: String::from(format!("{:?}", server_request)),
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
                response: String::from(format!("{:?}", server_response)),
                request: String::from(format!("{:?}", server_request)),
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
                response: String::from(format!("{:?}", server_response)),
                request: String::from(format!("{:?}", server_request)),
            }.into())
        }
    }
    async fn bulk_insert(&mut self, items: Vec<Self::Item>) -> Result<Vec<Self::Key>, Box<dyn Error>> {
        let server_request = ServerRequest::BulkSendBytes {
            bytes_collection: items,
        };
        let server_response = self.send_request(&server_request)
            .await?;
        if let ServerResponse::BulkSentBytes { ids } = server_response {
            Ok(ids)
        }
        else {
            Err(RemoteDataStoreError::UnexpectedResponseForRequest {
                response: String::from(format!("{:?}", server_response)),
                request: String::from(format!("{:?}", server_request)),
            }.into())
        }
    }
    async fn bulk_get(&self, ids: &Vec<Self::Key>) -> Result<Vec<Self::Item>, Box<dyn Error>> {
        let server_request = ServerRequest::BulkGetBytes {
            ids: ClonelessCow::Borrowed(ids),
        };
        let server_response = self.send_request(&server_request)
            .await?;
        if let ServerResponse::BulkReceivedBytes { bytes_collection } = server_response {
            Ok(bytes_collection)
        }
        else {
            Err(RemoteDataStoreError::UnexpectedResponseForRequest {
                response: String::from(format!("{:?}", server_response)),
                request: String::from(format!("{:?}", server_request)),
            }.into())
        }
    }
}

impl ByteConverter for RemoteDataStoreClient {
    fn append_to_bytes(&self, bytes: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.client.append_to_bytes(bytes)?;
        Ok(())
    }
    fn extract_from_bytes(bytes: &Vec<u8>, index: &mut usize) -> Result<Self, Box<dyn Error>> where Self: Sized {
        Ok(Self {
            client: ByteConClient::<ServerRequest, ServerResponse>::extract_from_bytes(bytes, index)?,
        })
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
    type TInput = ServerRequest<'static>;
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
            },
            ServerRequest::BulkSendBytes { bytes_collection } => {
                let ids = self.data_store
                    .lock()
                    .await
                    .bulk_insert(bytes_collection.to_vec())
                    .await?;

                ServerResponse::BulkSentBytes {
                    ids,
                }
            },
            ServerRequest::BulkGetBytes { ids } => {
                let bytes_collection = self.data_store
                    .lock()
                    .await
                    .bulk_get(ids.as_ref())
                    .await?;

                ServerResponse::BulkReceivedBytes {
                    bytes_collection,
                }
            },
        };
        Ok(server_response)
    }
}

impl<TDataStore> ByteConverter for RemoteDataStoreMessageProcessor<TDataStore>
where
    TDataStore: DataStore<Item = Vec<u8>, Key = i64> + Send + Sync + 'static + ByteConverter,
{
    fn append_to_bytes(&self, bytes: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.data_store
            .blocking_lock()
            .append_to_bytes(bytes)?;
        Ok(())
    }
    fn extract_from_bytes(bytes: &Vec<u8>, index: &mut usize) -> Result<Self, Box<dyn Error>> where Self: Sized {
        Ok(Self {
            data_store: Arc::new(Mutex::new(TDataStore::extract_from_bytes(bytes, index)?)),
        })
    }
}


pub struct RemoteDataStoreServer<TDataStore>
where
    TDataStore: DataStore<Item = Vec<u8>, Key = i64> + Send + Sync + 'static,
{
    server: ByteConServer<RemoteDataStoreMessageProcessor<TDataStore>>,
}

impl<TDataStore> RemoteDataStoreServer<TDataStore>
where
    TDataStore: DataStore<Item = Vec<u8>, Key = i64> + Send + Sync + 'static,
{
    pub fn new(
        data_store: Arc<Mutex<TDataStore>>,
        public_key: ByteConPublicKey,
        private_key: ByteConPrivateKey,
        bind_address: String,
        bind_port: u16,
    ) -> Self {
        Self {
            server: ByteConServer::<RemoteDataStoreMessageProcessor<TDataStore>>::new(
                bind_address,
                bind_port,
                public_key,
                private_key,
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

impl<TDataStore> ByteConverter for RemoteDataStoreServer<TDataStore>
where
    TDataStore: DataStore<Item = Vec<u8>, Key = i64> + Send + Sync + 'static + ByteConverter,
{
    fn append_to_bytes(&self, bytes: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.server.append_to_bytes(bytes)?;
        Ok(())
    }
    fn extract_from_bytes(bytes: &Vec<u8>, index: &mut usize) -> Result<Self, Box<dyn Error>> where Self: Sized {
        Ok(Self {
            server: ByteConServer::<RemoteDataStoreMessageProcessor<TDataStore>>::extract_from_bytes(bytes, index)?,
        })
    }
}

#[derive(Debug)]
enum ServerRequest<'a> {
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
    BulkSendBytes {
        bytes_collection: Vec<Vec<u8>>,
    },
    BulkGetBytes {
        ids: ClonelessCow<'a, Vec<i64>>,
    },
}

impl ByteConverter for ServerRequest<'_> {
    fn append_to_bytes(&self, bytes: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
        match self {
            ServerRequest::HealthCheck => {
                // byte
                0u8.append_to_bytes(bytes)?;
            },
            ServerRequest::SendBytes { bytes: send_bytes } => {
                // byte
                1u8.append_to_bytes(bytes)?;
                // vec<u8>
                send_bytes.append_to_bytes(bytes)?;
            },
            ServerRequest::GetBytes { id } => {
                // byte
                2u8.append_to_bytes(bytes)?;
                // i64
                id.append_to_bytes(bytes)?;
            },
            ServerRequest::Delete { id } => {
                // byte
                3u8.append_to_bytes(bytes)?;
                // i64
                id.append_to_bytes(bytes)?;
            },
            ServerRequest::ListIds { page_index, page_size, row_offset } => {
                // byte
                4u8.append_to_bytes(bytes)?;
                // u64
                page_index.append_to_bytes(bytes)?;
                // u64
                page_size.append_to_bytes(bytes)?;
                // u64
                row_offset.append_to_bytes(bytes)?;
            },
            ServerRequest::BulkSendBytes { bytes_collection } => {
                // byte
                5u8.append_to_bytes(bytes)?;
                // Vec<Vec<u8>>
                bytes_collection.append_to_bytes(bytes)?;
            },
            ServerRequest::BulkGetBytes { ids } => {
                // byte
                6u8.append_to_bytes(bytes)?;
                // Vec<i64>
                ids.as_ref().append_to_bytes(bytes)?;
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
            5 => {
                Ok(Self::BulkSendBytes {
                    bytes_collection: Vec::<Vec<u8>>::extract_from_bytes(bytes, index)?,
                })
            },
            6 => {
                Ok(Self::BulkGetBytes {
                    ids: ClonelessCow::Owned(Vec::<i64>::extract_from_bytes(bytes, index)?),
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
    BulkSentBytes {
        ids: Vec<i64>,
    },
    BulkReceivedBytes {
        bytes_collection: Vec<Vec<u8>>,
    },
}

impl ByteConverter for ServerResponse {
    fn append_to_bytes(&self, bytes: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
        match self {
            ServerResponse::HealthCheck => {
                // byte
                0u8.append_to_bytes(bytes)?;
            },
            ServerResponse::ReceivedBytes { bytes: received_bytes } => {
                // byte
                1u8.append_to_bytes(bytes)?;
                // Vec<u8>
                received_bytes.append_to_bytes(bytes)?;
            },
            ServerResponse::SentBytes { id } => {
                // byte
                2u8.append_to_bytes(bytes)?;
                // i64
                id.append_to_bytes(bytes)?;
            },
            ServerResponse::Deleted { id } => {
                // byte
                3u8.append_to_bytes(bytes)?;
                // i64
                id.append_to_bytes(bytes)?;
            },
            ServerResponse::ReceivedIdList { ids } => {
                // byte
                4u8.append_to_bytes(bytes)?;
                // Vec<i64>
                ids.append_to_bytes(bytes)?;
            },
            ServerResponse::BulkSentBytes { ids } => {
                // byte
                5u8.append_to_bytes(bytes)?;
                // Vec<i64>
                ids.append_to_bytes(bytes)?;
            },
            ServerResponse::BulkReceivedBytes { bytes_collection } => {
                // byte
                6u8.append_to_bytes(bytes)?;
                // Vec<Vec<u8>>
                bytes_collection.append_to_bytes(bytes)?;
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
            5 => {
                Ok(Self::BulkSentBytes {
                    ids: Vec::<i64>::extract_from_bytes(bytes, index)?,
                })
            },
            6 => {
                Ok(Self::BulkReceivedBytes {
                    bytes_collection: Vec::<Vec<u8>>::extract_from_bytes(bytes, index)?,
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
    #[error("Unexpected response {response} based on request {request}.")]
    UnexpectedResponseForRequest {
        response: String,
        request: String,
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
