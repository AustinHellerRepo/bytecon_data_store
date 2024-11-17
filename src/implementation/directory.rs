use std::{error::Error, fs::File, io::{Read, Write}, path::{Path, PathBuf}, sync::{Arc, Mutex}};
use crate::DataStore;
use bytecon::ByteConverter;
use futures::future::join_all;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rusqlite::{named_params, Connection};

pub struct DirectoryDataStore {
    sqlite_file_path: PathBuf,
    storage_directory_path: PathBuf,
    random: Arc<Mutex<ChaCha8Rng>>,
    cache_filename_length: usize,
}

impl DirectoryDataStore {
    pub fn new(sqlite_file_path: PathBuf, cache_filename_length: usize) -> Result<Self, DirectoryDataStoreError> {

        let storage_directory_path = {
            sqlite_file_path.parent()
                .ok_or_else(|| {
                    DirectoryDataStoreError::UnableToConstructStorageDirectoryPath {
                        sqlite_file_path: sqlite_file_path.clone(),
                    }
                })?
                .append("cache")
        };

        Ok(Self {
            sqlite_file_path,
            storage_directory_path,
            random: Arc::new(Mutex::new(ChaCha8Rng::from_entropy())),
            cache_filename_length,
        })
    }
    fn generate_random_filename(&self) -> Result<String, Box<dyn Error>> {
        let locked_random_result = self.random.lock();
        let mut locked_random = locked_random_result
            .map_err(|_| {
                DirectoryDataStoreError::FailedToLockMutex
            })?;
        Ok(locked_random.gen_filename(self.cache_filename_length))
    }
    fn generate_random_value<T>(&self) -> Result<T, DirectoryDataStoreError>
    where rand::distributions::Standard: rand::prelude::Distribution<T>
    {
        let locked_random_result = self.random.lock();
        let mut locked_random = locked_random_result
            .map_err(|_| {
                DirectoryDataStoreError::FailedToLockMutex
            })?;
        Ok(locked_random.gen())
    }
}

impl DataStore for DirectoryDataStore {
    type Item = Vec<u8>;
    type Key = i64;

    async fn initialize(&mut self) -> Result<(), Box<dyn Error>> {
        let connection = Connection::open(&self.sqlite_file_path)
            .map_err(|error| {
                DirectoryDataStoreError::UnableToConnectToSqlitePath {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        connection.execute("
            CREATE TABLE IF NOT EXISTS file_record
            (
                file_record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT,
                bytes_length INTEGER
            );
        ", [])
            .map_err(|error| {
                DirectoryDataStoreError::UnableToCreateTablesWhenConstructingFreshStart {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        connection.close()
            .map_err(|(_, error)| {
                DirectoryDataStoreError::FailedToCloseSqliteConnection {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;
        
        if !self.storage_directory_path.exists() {
            std::fs::create_dir_all(&self.storage_directory_path)
                .map_err(|error| {
                    DirectoryDataStoreError::FailedToCreateCacheDirectory {
                        cache_directory_path: self.storage_directory_path.clone(),
                        error,
                    }
                })?;
        }

        Ok(())
    }
    async fn insert(&mut self, item: Self::Item) -> Result<Self::Key, Box<dyn Error>> {
        let random_file_name = self.generate_random_filename()?;
        let random_file_path = self.storage_directory_path.append(random_file_name);

        if random_file_path.exists() {
            return Err(Box::new(DirectoryDataStoreError::RandomFilePathAlreadyExists {
                random_file_path: random_file_path.clone(),
                sqlite_file_path: self.sqlite_file_path.clone(),
            }));
        }
        
        let mut random_file = File::create(random_file_path.clone())
            .map_err(|error| {
                DirectoryDataStoreError::FailedToCreateRandomFile {
                    random_file_path: random_file_path.clone(),
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        let connection = Connection::open(&self.sqlite_file_path)
            .map_err(|error| {
                DirectoryDataStoreError::UnableToConnectToSqlitePath {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        connection.execute("
            INSERT INTO file_record
            (
                file_path,
                bytes_length
            )
            VALUES
            (
                :file_path
                , :bytes_length
            );
        ", named_params! {
            ":file_path": random_file_path.as_os_str().to_str(),
            ":bytes_length": item.len(),
        })
            .map_err(|error| {
                DirectoryDataStoreError::FailedToInsertFileRecord {
                    random_file_path: random_file_path.clone(),
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        let file_record_id = connection.last_insert_rowid();

        connection.close()
            .map_err(|(_, error)| {
                DirectoryDataStoreError::FailedToCloseSqliteConnection {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        random_file.write_all(&item)
            .map_err(|error| {
                DirectoryDataStoreError::FailedToWriteBytesToFile {
                    bytes_length: item.len(),
                    random_file_path: random_file_path.clone(),
                    error,
                }
            })?;

        Ok(file_record_id)
    }
    async fn get(&self, id: &Self::Key) -> Result<Self::Item, Box<dyn Error>> {
        
        let connection = Connection::open(&self.sqlite_file_path)
            .map_err(|error| {
                DirectoryDataStoreError::UnableToConnectToSqlitePath {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        let mut statement = connection.prepare("
            SELECT
                file_path
                , bytes_length
            FROM file_record
            WHERE
                file_record_id = :file_record_id;
        ")
            .map_err(|error| {
                DirectoryDataStoreError::FailedToConstructStatement {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        let (file_path, bytes_length) = statement.query_row(named_params! {
            ":file_record_id": *id,
        }, |row| {
            let file_path: String = row.get(0)?;
            let bytes_length: usize = row.get(1)?;
            Ok((
                file_path,
                bytes_length,
            ))
        })
            .map_err(|error| {
                DirectoryDataStoreError::FailedToPullBackFileRecord {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    id: *id,
                    error,
                }
            })?;
        
        let bytes = {
            let mut bytes: Vec<u8> = Vec::with_capacity(bytes_length);
            let mut file = File::open(&file_path)
                .map_err(|error| {
                    DirectoryDataStoreError::FailedToOpenCachedFileRecord {
                        cached_file_path: file_path.clone(),
                        error,
                    }
                })?;
            file.read_to_end(&mut bytes)
                .map_err(|error| {
                    DirectoryDataStoreError::FailedToReadFromCachedFile {
                        cached_file_path: file_path.clone(),
                        error,
                    }
                })?;
            bytes
        };

        Ok(bytes)
    }
    async fn delete(&self, id: &Self::Key) -> Result<(), Box<dyn Error>> {
        
        let mut connection = Connection::open(&self.sqlite_file_path)
            .map_err(|error| {
                DirectoryDataStoreError::UnableToConnectToSqlitePath {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        let transaction = connection.transaction()
            .map_err(|error| {
                DirectoryDataStoreError::UnableToCreateTransaction {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        let mut statement = transaction.prepare("
            SELECT
                file_path
            FROM file_record
            WHERE
                file_record_id = :file_record_id;
        ")
            .map_err(|error| {
                DirectoryDataStoreError::FailedToConstructStatement {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        let file_path = statement.query_row(named_params! {
            ":file_record_id": *id,
        }, |row| {
            let file_path: String = row.get(0)?;
            Ok(file_path)
        })
            .map_err(|error| {
                DirectoryDataStoreError::FailedToPullBackFileRecord {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    id: *id,
                    error,
                }
            })?;
        
        let path = Path::new(&file_path);
        if path.exists() {
            std::fs::remove_file(path)
                .map_err(|error| {
                    DirectoryDataStoreError::FailedToDeleteFileAtPath {
                        file_path: file_path.into(),
                        error,
                    }
                })?;
        }

        transaction.execute("
            DELETE FROM file_record
            WHERE
                file_record_id = :file_record_id;
        ", named_params! {
            ":file_record_id": *id,
        })
            .map_err(|error| {
                DirectoryDataStoreError::FailedToDeleteFileRecord {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    id: *id,
                    error,
                }
            })?;

        Ok(())
    }
    async fn list(&self, page_index: u64, page_size: u64, row_offset: u64) -> Result<Vec<Self::Key>, Box<dyn Error>> {
        
        let connection = Connection::open(&self.sqlite_file_path)
            .map_err(|error| {
                DirectoryDataStoreError::UnableToConnectToSqlitePath {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        let mut statement = connection.prepare("
            SELECT
                file_record_id
            FROM file_record
            ORDER BY
                file_record_id
            LIMIT :limit
            OFFSET :offset;
        ")
            .map_err(|error| {
                DirectoryDataStoreError::FailedToConstructStatement {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;

        let offset = page_index * page_size + row_offset;
        let file_record_id_results: Vec<Result<i64, rusqlite::Error>> = statement.query_map(named_params! {
            ":limit": page_size,
            ":offset": offset,
        }, |row| {
            let file_record_id: i64 = row.get(0)?;
            Ok(file_record_id)
        })
            .map_err(|error| {
                DirectoryDataStoreError::FailedToPullBackFileRecordList {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    page_size,
                    page_index,
                    row_offset,
                    error,
                }
            })?
            .collect();

        let mut file_record_ids = Vec::with_capacity(file_record_id_results.len());
        for file_record_id_result in file_record_id_results {
            let file_record_id = file_record_id_result?;
            file_record_ids.push(file_record_id);
        }
        
        Ok(file_record_ids)
    }
    async fn bulk_insert(&mut self, items: Vec<Self::Item>) -> Result<Vec<Self::Key>, Box<dyn Error>> {
        let mut file_paths = Vec::with_capacity(items.len());
        for _ in 0..items.len() {
            let random_file_name = self.generate_random_filename()?;
            let random_file_path = self.storage_directory_path.append(random_file_name);

            if random_file_path.exists() {
                return Err(Box::new(DirectoryDataStoreError::RandomFilePathAlreadyExists {
                    random_file_path: random_file_path.clone(),
                    sqlite_file_path: self.sqlite_file_path.clone(),
                }));
            }

            file_paths.push(random_file_path);
        }

        let mut files = Vec::with_capacity(file_paths.len());
        for file_path in file_paths.iter() {
            let mut random_file = File::create(file_path.clone())
                .map_err(|error| {
                    DirectoryDataStoreError::FailedToCreateRandomFile {
                        random_file_path: file_path.clone(),
                        sqlite_file_path: self.sqlite_file_path.clone(),
                        error,
                    }
                })?;
            files.push(random_file);
        }

        let mut connection = Connection::open(&self.sqlite_file_path)
            .map_err(|error| {
                DirectoryDataStoreError::UnableToConnectToSqlitePath {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;
        let transaction = connection.transaction()?;

        let file_record_ids = {
            let mut statement = transaction.prepare("
                INSERT INTO file_record
                (
                    file_path,
                    bytes_length
                )
                VALUES
                (
                    :file_path
                    , :bytes_length
                );
            ")?;

            let mut file_record_ids = Vec::with_capacity(file_paths.len());
            for (file_path, item_length) in file_paths.iter().zip(items.iter().map(|item| item.len())) {
                statement.execute(named_params! {
                    ":file_path": file_path.as_os_str().to_str(),
                    ":bytes_length": item_length,
                })
                    .map_err(|error| {
                        DirectoryDataStoreError::FailedToInsertFileRecord {
                            random_file_path: file_path.clone(),
                            sqlite_file_path: self.sqlite_file_path.clone(),
                            error,
                        }
                    })?;

                let file_record_id = transaction.last_insert_rowid();
                file_record_ids.push(file_record_id);
            }
            file_record_ids
        };

        transaction.commit()?;

        for (mut file, (file_path, item)) in files.into_iter().zip(file_paths.into_iter().zip(items.into_iter())) {
            file.write_all(&item)
                .map_err(|error| {
                    DirectoryDataStoreError::FailedToWriteBytesToFile {
                        bytes_length: item.len(),
                        random_file_path: file_path.clone(),
                        error,
                    }
                })?;
        }

        Ok(file_record_ids)
    }
    async fn bulk_get(&self, ids: &Vec<Self::Key>) -> Result<Vec<Self::Item>, Box<dyn Error>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        // perform database interactions
        let items = {
            let mut connection = Connection::open(&self.sqlite_file_path)
                .map_err(|error| {
                    DirectoryDataStoreError::UnableToConnectToSqlitePath {
                        sqlite_file_path: self.sqlite_file_path.clone(),
                        error,
                    }
                })?;

            let transaction = connection.transaction()?;

            // create temp table
            let temp_table_name = {
                let temp_table_name = {
                    let random_number: u128 = self.generate_random_value()?;
                    String::from(format!("temp_ids_{}", random_number))
                };
                transaction.execute(&format!("
                    CREATE TEMP TABLE {}
                    (
                        id INTEGER
                    );
                ", temp_table_name), [])?;
                temp_table_name
            };

            // insert ids into temp table
            {
                let mut statement = transaction.prepare("
                    INSERT INTO temp_ids
                    (
                        id
                    )
                    VALUES
                    (
                        :id
                    );
                ")?;
                for id in ids {
                    statement.execute(named_params! {
                        ":id": id,
                    })?;
                }
            }

            // select from primary table
            let items = {
                let mut statement = transaction.prepare("
                    SELECT
                        file_path
                        , bytes_length
                    FROM file_record fr
                    JOIN temp_ids ti
                    ON
                        ti.id = fr.id
                ")?;
                let items = statement.query_map([], |row| {
                    let file_path: String = row.get(0)?;
                    let bytes_length: usize = row.get(1)?;
                    Ok((
                        file_path,
                        bytes_length,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
                items
            };

            // drop the temp table
            {
                transaction.execute(&format!("
                    DROP TABLE {};
                ", temp_table_name), [])?;
            }

            items
        };

        // read the bytes from the files
        let bytes_collections: Vec<Vec<u8>> = {
            let futures = items.into_iter()
                .map(|(file_path, bytes_length)| {
                    async move {
                        let mut bytes: Vec<u8> = Vec::with_capacity(bytes_length);
                        let mut file = File::open(&file_path)
                            .map_err(|error| {
                                DirectoryDataStoreError::FailedToOpenCachedFileRecord {
                                    cached_file_path: file_path.clone(),
                                    error,
                                }
                            })?;
                        file.read_to_end(&mut bytes)
                            .map_err(|error| {
                                DirectoryDataStoreError::FailedToReadFromCachedFile {
                                    cached_file_path: file_path.clone(),
                                    error,
                                }
                            })?;
                        Ok(bytes)
                    }
                })
                .collect::<Vec<_>>();

            let joined_futures: Vec<Result<Vec<u8>, DirectoryDataStoreError>> = join_all(futures)
                .await;
            let mut bytes_collections = Vec::with_capacity(joined_futures.len());
            for joined_future in joined_futures {
                let bytes = joined_future?;
                bytes_collections.push(bytes);
            }
            bytes_collections
        };

        Ok(bytes_collections)
    }
}

impl ByteConverter for DirectoryDataStore {
    fn append_to_bytes(&self, bytes: &mut Vec<u8>) -> Result<(), Box<dyn Error>> {
        self.sqlite_file_path.append_to_bytes(bytes)?;
        self.storage_directory_path.append_to_bytes(bytes)?;
        self.random.lock()
            .map_err(|_| {
                DirectoryDataStoreError::FailedToLockMutex
            })?
            .append_to_bytes(bytes)?;
        self.cache_filename_length.append_to_bytes(bytes)?;
        Ok(())
    }
    fn extract_from_bytes(bytes: &Vec<u8>, index: &mut usize) -> Result<Self, Box<dyn Error>> where Self: Sized {
        Ok(Self {
            sqlite_file_path: PathBuf::extract_from_bytes(bytes, index)?,
            storage_directory_path: PathBuf::extract_from_bytes(bytes, index)?,
            random: Arc::new(Mutex::new(ChaCha8Rng::extract_from_bytes(bytes, index)?)),
            cache_filename_length: usize::extract_from_bytes(bytes, index)?,
        })
    }
}

trait Appendable<T: AsRef<Path>> {
    fn append(&self, appended: T) -> PathBuf;
}

impl<T: AsRef<Path>> Appendable<T> for &std::path::Path {
    fn append(&self, appended: T) -> PathBuf {
        self.join(appended)
    }
}

impl<T: AsRef<Path>> Appendable<T> for PathBuf {
    fn append(&self, appended: T) -> PathBuf {
        self.join(appended)
    }
}

impl<T: AsRef<Path>> Appendable<T> for &PathBuf {
    fn append(&self, appended: T) -> PathBuf {
        self.join(appended)
    }
}

trait RandomFilenameGenerator {
    fn gen_filename(&mut self, length: usize) -> String;
}

impl RandomFilenameGenerator for rand::rngs::StdRng {
    fn gen_filename(&mut self, length: usize) -> String {
        self.sample_iter(&rand::distributions::Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }
}

impl RandomFilenameGenerator for ChaCha8Rng {
    fn gen_filename(&mut self, length: usize) -> String {
        self.sample_iter(&rand::distributions::Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DirectoryDataStoreError {
    #[error("Unable to create connection to Sqlite path at {sqlite_file_path} with error {error}.")]
    UnableToConnectToSqlitePath {
        sqlite_file_path: PathBuf,
        error: rusqlite::Error,
    },
    #[error("Unable to create tables when constructing fresh start at {sqlite_file_path} with error {error}.")]
    UnableToCreateTablesWhenConstructingFreshStart {
        sqlite_file_path: PathBuf,
        error: rusqlite::Error,
    },
    #[error("Unable to construct storage directory path from Sqlite path {sqlite_file_path}.")]
    UnableToConstructStorageDirectoryPath {
        sqlite_file_path: PathBuf,
    },
    #[error("Random file path already exists at {random_file_path} with Sqlite path at {sqlite_file_path}.")]
    RandomFilePathAlreadyExists {
        random_file_path: PathBuf,
        sqlite_file_path: PathBuf,
    },
    #[error("Failed to create random file at {random_file_path} with Sqlite path at {sqlite_file_path} with error {error}.")]
    FailedToCreateRandomFile {
        random_file_path: PathBuf,
        sqlite_file_path: PathBuf,
        error: std::io::Error,
    },
    #[error("Failed to insert file record at {random_file_path} with Sqlite path at {sqlite_file_path} with error {error}.")]
    FailedToInsertFileRecord {
        random_file_path: PathBuf,
        sqlite_file_path: PathBuf,
        error: rusqlite::Error,
    },
    #[error("Failed to close Sqlite connection to {sqlite_file_path} with error {error}")]
    FailedToCloseSqliteConnection {
        sqlite_file_path: PathBuf,
        error: rusqlite::Error,
    },
    #[error("Failed to write {bytes_length} bytes to file {random_file_path}.")]
    FailedToWriteBytesToFile {
        bytes_length: usize,
        random_file_path: PathBuf,
        error: std::io::Error,
    },
    #[error("Failed to construct rusqlite Statement instance for {sqlite_file_path} with error {error}.")]
    FailedToConstructStatement {
        sqlite_file_path: PathBuf,
        error: rusqlite::Error,
    },
    #[error("Failed to pull back file_record row from Statement for {sqlite_file_path} for ID {id} with error {error}.")]
    FailedToPullBackFileRecord {
        sqlite_file_path: PathBuf,
        id: i64,
        error: rusqlite::Error,
    },
    #[error("Failed to open cached file at {cached_file_path} with error {error}.")]
    FailedToOpenCachedFileRecord {
        cached_file_path: String,
        error: std::io::Error,
    },
    #[error("Failed to read from cached file at {cached_file_path} with error {error}.")]
    FailedToReadFromCachedFile {
        cached_file_path: String,
        error: std::io::Error,
    },
    #[error("Failed to create cache directory at {cache_directory_path} with error {error}.")]
    FailedToCreateCacheDirectory {
        cache_directory_path: PathBuf,
        error: std::io::Error,
    },
    #[error("Failed to delete file_record row for {sqlite_file_path} with ID {id} with error {error}.")]
    FailedToDeleteFileRecord {
        sqlite_file_path: PathBuf,
        id: i64,
        error: rusqlite::Error,
    },
    #[error("Unable to create transaction from Sqlite connection for {sqlite_file_path} with error {error}.")]
    UnableToCreateTransaction {
        sqlite_file_path: PathBuf,
        error: rusqlite::Error,
    },
    #[error("Failed to delete file based on file_record path {file_path} with error {error}.")]
    FailedToDeleteFileAtPath {
        file_path: PathBuf,
        error: std::io::Error,
    },
    #[error("Failed to pull back file_record rows from list Statement for {sqlite_file_path} for page size {page_size}, page index {page_index}, and row offset {row_offset} with error {error}.")]
    FailedToPullBackFileRecordList {
        sqlite_file_path: PathBuf,
        page_size: u64,
        page_index: u64,
        row_offset: u64,
        error: rusqlite::Error,
    },
    #[error("Failed to lock mutex.")]
    FailedToLockMutex,
}