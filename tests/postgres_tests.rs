#[cfg(test)]
mod postgres_tests {
    use data_funnel::{implementation::postgres::PostgresDataStore, DataStore};

    #[tokio::test]
    async fn test_p7a1_initialize() {
        let postgres_connection_string = String::from("host=localhost user=user password=password dbname=database");
        let mut data_store = PostgresDataStore::new(
            postgres_connection_string,
        );

        data_store.initialize()
            .await
            .unwrap();

        for j in 0..100 {
            let bytes: Vec<u8> = vec![
                1 + j,
                2 + j,
                3 + j,
                4 + j,
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

        let list = data_store.list(1, 4, 2)
            .await
            .unwrap();

        assert_eq!(4, list.len());
        assert_eq!(7, list[0]);
        assert_eq!(8, list[1]);
        assert_eq!(9, list[2]);
        assert_eq!(10, list[3]);

        for j in 1..101 {
            data_store.delete(&j)
                .await
                .unwrap();
        }

        let empty_list = data_store.list(0, 1, 0)
            .await
            .unwrap();

        assert_eq!(0, empty_list.len());

        data_store.reset()
            .await
            .unwrap();
    }
}