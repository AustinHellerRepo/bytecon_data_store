#[cfg(test)]
mod postgres_tests {
    use bytecon_data_store::{implementation::postgres::PostgresDataStore, DataStore};

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

    #[tokio::test]
    async fn test_i5m0_bulk_insert_and_bulk_get() {
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

        let original_bytes_collection = vec![
            vec![1u8],
            vec![2u8, 3u8],
            vec![4u8, 5u8, 6u8],
        ];
        let ids = data_store.bulk_insert(original_bytes_collection.clone())
            .await
            .expect("Failed to perform bulk insert.");

        // try in the original ID order
        {
            let retrieved_bytes_collection = data_store.bulk_get(&ids)
                .await
                .expect("Failed to bulk get by IDs.");

            assert_eq!(original_bytes_collection, retrieved_bytes_collection);
        }

        // try in reverse order
        {
            let mut reversed_ids = Vec::with_capacity(ids.len());
            for i in ids.len() - 1..=0 {
                reversed_ids.push(ids[i]);
            }

            let retrieved_bytes_collection = data_store.bulk_get(&reversed_ids)
                .await
                .expect("Failed to bulk get by IDs.");

            assert_ne!(original_bytes_collection, retrieved_bytes_collection);

            for (top_down, down_up) in (ids.len() - 1..=0).zip(0..ids.len()) {
                assert_eq!(original_bytes_collection[down_up], retrieved_bytes_collection[top_down]);
            }
        }
    }
}