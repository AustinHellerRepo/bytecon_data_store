use std::{error::Error, ffi::OsString, fs::File, io::{Read, Write}, path::{Path, PathBuf}};
use rand::Rng;
use rusqlite::{params, Connection};
use crate::DataStore;

pub struct DirectoryDataStore {
    sqlite_file_path: PathBuf,
    storage_directory_path: PathBuf,
    random: rand::rngs::ThreadRng,
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
            random: rand::thread_rng(),
            cache_filename_length,
        })
    }
}

impl DataStore for DirectoryDataStore {
    type Item = Vec<u8>;
    type Key = i64;

    fn initialize(&mut self) -> Result<(), Box<dyn Error>> {
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
                file_id SERIAL INTEGER PRIMARY KEY,
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
    fn insert(&mut self, item: Self::Item) -> Result<Self::Key, Box<dyn Error>> {
        let random_file_name = self.random.gen_filename(self.cache_filename_length);
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

        let connection = Connection::open(&random_file_path)
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
                ?
                , ?
            );
        ", (
            random_file_path.as_os_str().to_str(),
            item.len(),
        ))
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

    fn get(&self, id: &Self::Key) -> Result<Self::Item, Box<dyn Error>> {
        
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
                file_record = ?
        ")
            .map_err(|error| {
                DirectoryDataStoreError::FailedToConstructStatement {
                    sqlite_file_path: self.sqlite_file_path.clone(),
                    error,
                }
            })?;
        
        let (file_path, bytes_length) = statement.query_row(params![
            id
        ], |row| {
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
    #[error("Failed to pull back file_record row from Statement for {sqlite_file_path} with error {error}.")]
    FailedToPullBackFileRecord {
        sqlite_file_path: PathBuf,
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

impl RandomFilenameGenerator for rand::rngs::ThreadRng {
    fn gen_filename(&mut self, length: usize) -> String {
        self.sample_iter(&rand::distributions::Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }
}