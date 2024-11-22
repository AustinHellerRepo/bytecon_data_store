# bytecon_data_store
This library contains a `DataStore` trait for interacting with data storage generally along with a few different specific implementations.

## Features
- The ability to send anything that implements `ByteConverter` to storage while also being easily retrievable.
- Postgres support via the `"postgres"` feature.
- Direct-to-file support via the `"directory"` feature.
- Client/server support via the `"remote"` feature.
- Convenient `ByteConDataStore` for sending `ByteConverter` instances into any `DataStore` that sends `Vec<u8>`.

## Usage
You will want to convert your `ByteConverter` instances to `Vec<u8>` to utilize the existing implementations. This design choice was made to allow for your `ByteConverter`-implementing structs to not need to implement Send or Sync.