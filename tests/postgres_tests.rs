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

        data_store.reset()
            .await
            .unwrap();

        let empty_list = data_store.list(0, 1, 0)
            .await
            .unwrap();

        if !empty_list.is_empty() {
            println!("non-empty list: {:?}", empty_list);
        }
        assert_eq!(0, empty_list.len());

        let ids_length: u8 = 100;
        let mut ids = Vec::with_capacity(ids_length as usize);
        for j in 0..ids_length {
            let bytes: Vec<u8> = vec![
                1 + j,
                2 + j,
                3 + j,
                4 + j,
            ];
            let id = data_store.insert(bytes.clone())
                .await
                .unwrap();

            ids.push(id);

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

        for id in ids {
            data_store.delete(&id)
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