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

//! Supermetal's calls into APIs that only this fork has, written the way its `targets/iceberg`
//! code makes them. Nothing runs this function. Compiling it is the test, so a rebase that
//! changes one of these APIs fails here before it fails in Supermetal. Each patch's own tests
//! cover behavior.

use std::collections::HashMap;

use iceberg::delete_vector::DeleteVector;
use iceberg::io::IO_CHUNK_SIZE;
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::deletion_vector_writer::{
    DeletionVectorWriterBuilder, PositionDeleteInput,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, Result};
use roaring::RoaringTreemap;

#[allow(dead_code)]
async fn supermetal_calls(
    catalog: &dyn Catalog,
    table: &Table,
    files: Vec<DataFile>,
    replaced_deletion_vectors: Vec<DataFile>,
    snapshot_properties: HashMap<String, String>,
    deleted_rows: Vec<PositionDeleteInput>,
) -> Result<()> {
    // A merge on read commit in `executor.rs`.
    let tx = Transaction::new(table);
    let tx = tx
        .row_delta()
        .with_check_duplicate(true)
        .set_snapshot_properties(snapshot_properties.clone())
        .add_data_files(files.clone())
        .add_delete_files(files.clone())
        .remove_delete_files(replaced_deletion_vectors)
        .apply(tx)?;
    tx.commit_once(catalog).await?;

    // A truncate in `executor.rs`.
    let tx = Transaction::new(table);
    let tx = tx
        .overwrite_files()
        .full_table_overwrite()
        .set_snapshot_properties(snapshot_properties)
        .set_starting_snapshot_id(0)
        .delete_data_files(files.clone())?
        .apply(tx)?;
    tx.commit_once(catalog).await?;

    // Bounds too large for a manifest are dropped before a file is committed, in `writer.rs`.
    for mut file in files {
        file.lower_bounds
            .retain(|_, bound| bound.to_bytes().map_or(true, |bytes| bytes.len() <= 1024));
        file.upper_bounds
            .retain(|_, bound| bound.to_bytes().map_or(true, |bytes| bytes.len() <= 1024));
    }

    // Deletion vectors in `writer.rs`.
    let mut writer = DeletionVectorWriterBuilder::new(
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata())?,
        DefaultFileNameGenerator::new("dv".to_string(), None, DataFileFormat::Puffin),
    )
    .build(None)
    .await?;
    writer.write(deleted_rows).await?;
    writer.write_bitmap(
        "s3://bucket/data.parquet".to_string(),
        DeleteVector::new(RoaringTreemap::new()),
    )?;
    writer.close().await?;

    // The property that bounds multipart upload parts, in `catalog.rs`.
    let _: &str = IO_CHUNK_SIZE;
    Ok(())
}
