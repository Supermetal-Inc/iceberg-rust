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

//! Test helpers for the fork's transaction actions, kept out of upstream's test module.

use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use crate::io::FileIO;
use crate::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, Snapshot,
    Struct, TableMetadata, TableMetadataBuilder,
};
use crate::table::Table;
use crate::test_utils::test_runtime;
use crate::{TableIdent, TableUpdate};

pub(crate) fn data_file(path: &str, table: &Table) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(10)
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .partition(Struct::from_iter([Some(Literal::long(100))]))
        .build()
        .unwrap()
}

pub(crate) fn pos_delete_v2(path: &str, table: &Table) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::PositionDeletes)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(50)
        .record_count(5)
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .partition(Struct::from_iter([Some(Literal::long(100))]))
        .build()
        .unwrap()
}

pub(crate) fn first_snapshot(updates: Vec<TableUpdate>) -> Snapshot {
    updates
        .into_iter()
        .find_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
            _ => None,
        })
        .expect("commit did not add a snapshot")
}

pub(crate) fn table_with_snapshot(base: &Table, snapshot: Snapshot) -> Table {
    let updated =
        TableMetadataBuilder::new_from_metadata(base.metadata_ref().as_ref().clone(), None)
            .set_branch_snapshot(snapshot, MAIN_BRANCH)
            .unwrap()
            .build()
            .unwrap()
            .metadata;

    base.clone().with_metadata(Arc::new(updated))
}

pub(crate) fn make_v3_minimal_table() -> Table {
    let file = File::open(format!(
        "{}/testdata/table_metadata/{}",
        env!("CARGO_MANIFEST_DIR"),
        "TableMetadataV3ValidMinimal.json"
    ))
    .unwrap();
    let reader = BufReader::new(file);
    let resp = serde_json::from_reader::<_, TableMetadata>(reader).unwrap();

    Table::builder()
        .metadata(resp)
        .metadata_location("s3://bucket/test/location/metadata/v1.json")
        .identifier(TableIdent::from_strs(["ns1", "test1"]).unwrap())
        .file_io(FileIO::new_with_memory())
        .runtime(test_runtime())
        .build()
        .unwrap()
}
