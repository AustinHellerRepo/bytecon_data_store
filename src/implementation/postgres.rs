use std::error::Error;
use bytecon::ByteConverter;
use deadpool_postgres::{Client, Manager, Pool, PoolConfig};
use tokio_postgres::{Config, NoTls};
use crate::DataStore;

pub struct PostgresDataStore {
    connection_string: String,
    pool: Pool<NoTls>,
}

impl PostgresDataStore {
    pub fn new(connection_string: String) -> Self {
        let config = connection_string.as_str().parse::<Config>().expect("Failed to parse connection string into tokio-postgres Config.");
        let manager_config = deadpool_postgres::ManagerConfig { recycling_method: deadpool_postgres::RecyclingMethod::Fast };
        let manager = Manager::from_config(config, NoTls, manager_config);
        let pool = Pool::from_config(manager, PoolConfig::new(100));
        Self {
            connection_string,
            pool,
        }
    }
    async fn connect(&self) -> Result<Client<NoTls>, Box<dyn Error>> {
        let client: Client<NoTls> = self.pool.get()
            .await
            .map_err(|error| {
                PostgresDataStoreError::FailedToConnectToPostgresDatabase {
                    error: Box::new(error),
                }
            })
            .unwrap();

        Ok(client)
    }
    pub async fn reset(&self) -> Result<(), Box<dyn Error>> {
        let client = self.connect()
            .await?;

        client.execute("
            TRUNCATE TABLE file_record;
        ", &[])
            .await?;

        client.execute("
            ALTER SEQUENCE file_record_file_record_id_seq RESTART WITH 1;
        ", &[])
            .await?;

        Ok(())
    }
}

impl DataStore for PostgresDataStore {
    type Item = Vec<u8>;
    type Key = i64;

    async fn initialize(&mut self) -> Result<(), Box<dyn Error>> {
        let client = self.connect()
            .await?;

        client.execute("
            CREATE TABLE IF NOT EXISTS file_record
            (
                file_record_id BIGSERIAL PRIMARY KEY
                , bytes BYTEA NOT NULL
            );
        ", &[])
            .await?;

        Ok(())
    }
    async fn insert(&mut self, item: Self::Item) -> Result<Self::Key, Box<dyn Error>> {
        let client = self.connect()
            .await?;

        let row = client.query_one("
            INSERT INTO file_record
            (
                bytes
            )
            VALUES
            (
                $1
            )
            RETURNING
                file_record_id;
        ", &[
            &item,
        ])
            .await?;

        let key: i64 = row.get(0);
        Ok(key)
    }
    async fn get(&self, id: &Self::Key) -> Result<Self::Item, Box<dyn Error>> {
        let client = self.connect()
            .await?;

        let row = client.query_one("
            SELECT
                bytes
            FROM file_record
            WHERE
                file_record_id = $1;
        ", &[
            id,
        ])
            .await?;

        let bytes: Vec<u8> = row.get(0);
        Ok(bytes)
    }
    async fn delete(&self, id: &Self::Key) -> Result<(), Box<dyn Error>> {
        let client = self.connect()
            .await?;

        let rows_affected_total = client.execute("
            DELETE FROM file_record
            WHERE
                file_record_id = $1;
        ", &[
            id,
        ])
            .await?;

        if rows_affected_total == 0 {
            Err(PostgresDataStoreError::FailedToDeleteFileRecord {
                id: *id,
            }.into())
        }
        else {
            Ok(())
        }
    }
    async fn list(&self, page_index: u64, page_size: u64, row_offset: u64) -> Result<Vec<Self::Key>, Box<dyn Error>> {
        let client = self.connect()
            .await?;

        let offset = page_index * page_size + row_offset;

        let ids = client.query("
            SELECT
                file_record_id
            FROM file_record
            ORDER BY
                file_record_id
            LIMIT $1
            OFFSET $2;
        ", &[
            &(page_size as i64),
            &(offset as i64),
        ])
            .await?
            .into_iter()
            .map(|row| {
                let id: i64 = row.get(0);
                id
            })
            .collect();
            
        Ok(ids)
    }
}

impl ByteConverter for PostgresDataStore {
    fn append_to_bytes(&self, bytes: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.connection_string.append_to_bytes(bytes)?;
        Ok(())
    }
    fn extract_from_bytes(bytes: &Vec<u8>, index: &mut usize) -> Result<Self, Box<dyn Error>> where Self: Sized {
        Ok(Self::new(String::extract_from_bytes(bytes, index)?))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PostgresDataStoreError {
    #[error("Failed to find file record to delete with ID {id}")]
    FailedToDeleteFileRecord {
        id: i64,
    },
    #[error("Failed to connect to Postgres database with error {error}.")]
    FailedToConnectToPostgresDatabase {
        error: Box<dyn Error>,
    },
}