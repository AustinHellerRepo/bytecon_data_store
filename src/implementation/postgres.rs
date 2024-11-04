use std::{error::Error, time::Duration};
use tokio::time::sleep;
use tokio_postgres::NoTls;

use crate::DataStore;

pub struct PostgresDataStore {
    connection_string: String,
}

impl PostgresDataStore {
    pub fn new(connection_string: String) -> Self {
        Self {
            connection_string,
        }
    }
}

impl DataStore for PostgresDataStore {
    type Item = Vec<u8>;
    type Key = i64;

    async fn initialize(&mut self) -> Result<(), Box<dyn Error>> {
        println!("PostgresDataStore initializing...");
        let (client, connection) = tokio_postgres::connect(
            &self.connection_string,
            NoTls,
        )
            .await
            .map_err(|error| {
                PostgresDataStoreError::FailedToConnectToPostgresDatabase {
                    error,
                }
            })?;
        println!("PostgresDataStore initialized");

        tokio::spawn(async move {
            connection.await
                .map_err(|error| {
                    PostgresDataStoreError::FailedToConnectToPostgresDatabase {
                        error,
                    }
                })
                .unwrap();
        });

        sleep(Duration::from_millis(100))
            .await;

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
        let (client, connection) = tokio_postgres::connect(
            &self.connection_string,
            NoTls,
        )
            .await?;

        connection.await?;

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
        let (client, connection) = tokio_postgres::connect(
            &self.connection_string,
            NoTls,
        )
            .await?;

        connection.await?;

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
        let (client, connection) = tokio_postgres::connect(
            &self.connection_string,
            NoTls,
        )
            .await?;

        connection.await?;

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
        let (client, connection) = tokio_postgres::connect(
            &self.connection_string,
            NoTls,
        )
            .await?;

        connection.await?;

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
            &(page_index as i64),
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

#[derive(thiserror::Error, Debug)]
pub enum PostgresDataStoreError {
    #[error("Failed to find file record to delete with ID {id}")]
    FailedToDeleteFileRecord {
        id: i64,
    },
    #[error("Failed to connect to Postgres database with error {error}.")]
    FailedToConnectToPostgresDatabase {
        error: tokio_postgres::Error,
    },
}