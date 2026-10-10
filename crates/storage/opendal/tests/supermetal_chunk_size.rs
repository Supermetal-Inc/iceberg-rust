// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! `feat(storage-opendal): make the write chunk size configurable`

#[cfg(feature = "opendal-memory")]
mod tests {
    use bytes::Bytes;
    use iceberg::io::{IO_CHUNK_SIZE, StorageConfig, StorageFactory};
    use iceberg_storage_opendal::OpenDalStorageFactory;

    #[test]
    fn chunk_size_must_be_a_positive_integer() {
        let build = |value: &str| {
            OpenDalStorageFactory::Memory
                .build(&StorageConfig::new().with_prop(IO_CHUNK_SIZE, value))
        };

        assert!(build("67108864").is_ok());
        for invalid in ["0", "-1", "abc", ""] {
            let err = build(invalid).err().unwrap().to_string();
            assert!(err.contains(IO_CHUNK_SIZE), "{invalid}: {err}");
        }
    }

    #[tokio::test]
    async fn chunked_writer_writes_all_bytes() {
        let storage = OpenDalStorageFactory::Memory
            .build(&StorageConfig::new().with_prop(IO_CHUNK_SIZE, "4"))
            .unwrap();
        let path = "memory:/chunked";

        let mut writer = storage.writer(path).await.unwrap();
        for part in [&b"abc"[..], b"defgh", b"ij"] {
            writer.write(Bytes::copy_from_slice(part)).await.unwrap();
        }
        writer.close().await.unwrap();

        assert_eq!(
            storage.read(path).await.unwrap(),
            Bytes::from_static(b"abcdefghij")
        );
    }
}
