#[cfg(test)]
mod postgres_tests {
    use data_funnel::{implementation::postgres::PostgresDataStore, DataStore};

    #[tokio::test]
    async fn initialize() {
        let postgres_connection_string = String::from("host=localhost user=user password=password dbname=database");
        let mut data_store = PostgresDataStore::new(
            postgres_connection_string,
        );

        data_store.initialize()
            .await
            .unwrap();

        let bytes: Vec<u8> = vec![
            1,
            2,
            3,
            4,
        ];
        let id = data_store.insert(bytes.clone())
            .await
            .unwrap();

        let actual_bytes = data_store.get(&id)
            .await
            .unwrap();

        assert_eq!(4, actual_bytes.len());
        assert_eq!(bytes[0], actual_bytes[0]);
        assert_eq!(bytes[1], actual_bytes[1]);
        assert_eq!(bytes[2], actual_bytes[2]);
        assert_eq!(bytes[3], actual_bytes[3]);
    }
}