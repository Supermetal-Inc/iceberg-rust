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

//! Deletion vector writer for Iceberg V3.
//!
//! Collects deleted row positions per data file and writes each data file's positions as a
//! `deletion-vector-v1` blob in its own Puffin file.

use std::collections::HashMap;
use std::sync::Arc;

use crate::delete_vector::{
    DELETION_VECTOR_PROPERTY_CARDINALITY, DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE,
    DeleteVector,
};
use crate::io::FileIO;
use crate::puffin::{CREATED_BY_PROPERTY, CompressionCodec, PuffinWriter};
use crate::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, PartitionKey, Struct,
};
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::{IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Position delete input.
#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub struct PositionDeleteInput {
    /// The path of the file.
    pub path: Arc<str>,
    /// The row number in data file
    pub pos: i64,
}

impl PositionDeleteInput {
    /// Create a new `PositionDeleteInput`.
    pub fn new(path: Arc<str>, row: i64) -> Self {
        Self { path, pos: row }
    }
}

/// File properties of the Puffin files this writer produces. Java sets `created-by` too.
fn puffin_properties() -> HashMap<String, String> {
    HashMap::from([(
        CREATED_BY_PROPERTY.to_string(),
        format!("iceberg-rust {}", env!("CARGO_PKG_VERSION")),
    )])
}

/// Builder for `DeletionVectorWriter`.
#[derive(Clone)]
pub struct DeletionVectorWriterBuilder<L: LocationGenerator, F: FileNameGenerator> {
    file_io: FileIO,
    location_generator: L,
    file_name_generator: F,
}

impl<L: LocationGenerator, F: FileNameGenerator> DeletionVectorWriterBuilder<L, F> {
    /// Create a new `DeletionVectorWriterBuilder`.
    pub fn new(file_io: FileIO, location_generator: L, file_name_generator: F) -> Self {
        Self {
            file_io,
            location_generator,
            file_name_generator,
        }
    }
}

#[async_trait::async_trait]
impl<L, F> IcebergWriterBuilder<Vec<PositionDeleteInput>> for DeletionVectorWriterBuilder<L, F>
where
    L: LocationGenerator,
    F: FileNameGenerator,
{
    type R = DeletionVectorWriter<L, F>;

    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Ok(DeletionVectorWriter {
            file_io: self.file_io.clone(),
            location_generator: self.location_generator.clone(),
            file_name_generator: self.file_name_generator.clone(),
            partition_key,
            delete_vectors: HashMap::new(),
        })
    }
}

/// Writer that collects position deletes and emits deletion vectors as puffin files.
pub struct DeletionVectorWriter<L: LocationGenerator, F: FileNameGenerator> {
    file_io: FileIO,
    location_generator: L,
    file_name_generator: F,
    partition_key: Option<PartitionKey>,
    delete_vectors: HashMap<String, DeleteVector>,
}

impl<L: LocationGenerator, F: FileNameGenerator> DeletionVectorWriter<L, F> {
    /// Register a fully merged `DeleteVector` for `referenced_file`.
    /// Rejects duplicates so a `write_bitmap` call cannot silently
    /// overwrite a bitmap built up by prior `write` calls.
    pub fn write_bitmap(&mut self, referenced_file: String, vector: DeleteVector) -> Result<()> {
        if self.delete_vectors.contains_key(&referenced_file) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("delete vector already registered for {referenced_file}"),
            ));
        }
        self.delete_vectors.insert(referenced_file, vector);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<L, F> IcebergWriter<Vec<PositionDeleteInput>> for DeletionVectorWriter<L, F>
where
    L: LocationGenerator,
    F: FileNameGenerator,
{
    async fn write(&mut self, input: Vec<PositionDeleteInput>) -> Result<()> {
        for delete in input {
            let pos = u64::try_from(delete.pos).map_err(|_| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "deletion vector position must be non-negative".to_string(),
                )
            })?;
            let entry = self
                .delete_vectors
                .entry(delete.path.as_ref().to_string())
                .or_default();
            entry.insert(pos);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        if self.delete_vectors.is_empty() {
            return Ok(Vec::new());
        }

        let partition_key = self.partition_key.as_ref();
        let delete_vectors = std::mem::take(&mut self.delete_vectors);
        let mut data_files = Vec::with_capacity(delete_vectors.len());

        for (data_file_path, delete_vector) in delete_vectors {
            if delete_vector.is_empty() {
                continue;
            }
            let file_name = self.file_name_generator.generate_file_name();
            let location = self
                .location_generator
                .generate_location(partition_key, &file_name);
            let output_file = self.file_io.new_output(&location)?;
            let mut writer = PuffinWriter::new(&output_file, puffin_properties(), false).await?;

            let cardinality = delete_vector.len();
            let properties = HashMap::from([
                (
                    DELETION_VECTOR_PROPERTY_CARDINALITY.to_string(),
                    cardinality.to_string(),
                ),
                (
                    DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_string(),
                    data_file_path.clone(),
                ),
            ]);
            let blob = delete_vector.to_puffin_blob(properties)?;
            writer.add(blob, CompressionCodec::None).await?;

            let blob_metadata = writer
                .blobs_metadata()
                .first()
                .cloned()
                .ok_or_else(|| Error::new(ErrorKind::Unexpected, "puffin metadata is empty"))?;
            let file_metadata = writer.close().await?;

            let mut builder = DataFileBuilder::default();
            builder
                .content(DataContentType::PositionDeletes)
                .file_path(location)
                .file_format(DataFileFormat::Puffin)
                .partition(partition_key.map_or_else(Struct::empty, |pk| pk.data().clone()))
                .partition_spec_id(partition_key.map_or(0, |pk| pk.spec().spec_id()))
                .record_count(cardinality)
                .file_size_in_bytes(file_metadata.size)
                .referenced_data_file(Some(data_file_path))
                .content_offset(Some(blob_metadata.offset() as i64))
                .content_size_in_bytes(Some(blob_metadata.length() as i64));

            data_files.push(builder.build().map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Failed to build deletion vector file: {err}"),
                )
            })?);
        }

        Ok(data_files)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use roaring::RoaringTreemap;
    use tempfile::TempDir;

    use super::*;
    use crate::puffin::{DELETION_VECTOR_V1, PuffinReader};
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };

    fn writer_builder(
        temp_dir: &TempDir,
    ) -> DeletionVectorWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator> {
        DeletionVectorWriterBuilder::new(
            FileIO::new_with_fs(),
            DefaultLocationGenerator::with_data_location(
                temp_dir.path().to_str().unwrap().to_string(),
            ),
            DefaultFileNameGenerator::new("dv".to_string(), None, DataFileFormat::Puffin),
        )
    }

    #[tokio::test]
    async fn test_deletion_vector_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let mut writer = writer_builder(&temp_dir).build(None).await?;

        let a: Arc<str> = Arc::from("s3://bucket/data/a.parquet");
        let b: Arc<str> = Arc::from("s3://bucket/data/b.parquet");
        let c = "s3://bucket/data/c.parquet".to_string();
        writer
            .write(vec![
                PositionDeleteInput::new(a.clone(), 3),
                PositionDeleteInput::new(b.clone(), 1),
                PositionDeleteInput::new(a.clone(), 1),
                PositionDeleteInput::new(a.clone(), 3),
            ])
            .await?;
        // Positions past u32::MAX land in a second roaring bitmap of the treemap.
        writer.write_bitmap(
            c.clone(),
            DeleteVector::new(RoaringTreemap::from_iter([7, 1 << 33])),
        )?;

        let mut files = writer.close().await?;
        files.sort_by_key(|f| f.referenced_data_file());
        let expected = [
            (a.to_string(), vec![1, 3]),
            (b.to_string(), vec![1]),
            (c, vec![7, 1 << 33]),
        ];
        assert_eq!(files.len(), expected.len());

        for (file, (path, positions)) in files.iter().zip(expected) {
            assert_eq!(file.content_type(), DataContentType::PositionDeletes);
            assert_eq!(file.file_format(), DataFileFormat::Puffin);
            assert_eq!(file.referenced_data_file(), Some(path.clone()));
            assert_eq!(file.record_count(), positions.len() as u64);
            assert_eq!(file.partition_spec_id, 0);

            // The content offset and size locate the blob, which decodes to the positions.
            let bytes = file_io.new_input(file.file_path())?.read().await?;
            assert_eq!(bytes.len() as u64, file.file_size_in_bytes());
            let start = file.content_offset().unwrap() as usize;
            let end = start + file.content_size_in_bytes().unwrap() as usize;
            let decoded = DeleteVector::deserialize(&bytes[start..end])?;
            assert_eq!(decoded.iter().collect::<Vec<_>>(), positions);

            let reader = PuffinReader::new(file_io.new_input(file.file_path())?).await?;
            let file_metadata = reader.file_metadata().await?;
            assert!(
                file_metadata
                    .properties()
                    .get(CREATED_BY_PROPERTY)
                    .is_some_and(|value| value.starts_with("iceberg-rust "))
            );
            let blobs = file_metadata.blobs();
            assert_eq!(blobs.len(), 1);
            let blob = &blobs[0];
            assert_eq!(blob.blob_type(), DELETION_VECTOR_V1);
            assert_eq!(blob.fields(), &[
                crate::metadata_columns::RESERVED_FIELD_ID_POS
            ]);
            assert_eq!(blob.snapshot_id(), -1);
            assert_eq!(blob.sequence_number(), -1);
            assert_eq!(blob.offset(), start as u64);
            assert_eq!(
                blob.properties(),
                &HashMap::from([
                    (
                        DELETION_VECTOR_PROPERTY_CARDINALITY.to_string(),
                        positions.len().to_string()
                    ),
                    (
                        DELETION_VECTOR_PROPERTY_REFERENCED_DATA_FILE.to_string(),
                        path
                    ),
                ])
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_deletion_vector_writer_rejects_invalid_input() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = writer_builder(&temp_dir).build(None).await?;

        let err = writer
            .write(vec![PositionDeleteInput::new(Arc::from("a.parquet"), -1)])
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);

        writer
            .write(vec![PositionDeleteInput::new(Arc::from("a.parquet"), 0)])
            .await?;
        let err = writer
            .write_bitmap("a.parquet".to_string(), DeleteVector::default())
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DataInvalid);

        Ok(())
    }

    #[tokio::test]
    async fn test_deletion_vector_writer_skips_empty_vectors() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut writer = writer_builder(&temp_dir).build(None).await?;
        assert!(writer.close().await?.is_empty());

        let mut writer = writer_builder(&temp_dir).build(None).await?;
        writer.write_bitmap("a.parquet".to_string(), DeleteVector::default())?;
        assert!(writer.close().await?.is_empty());

        Ok(())
    }

    /// Deletion vectors written by `DeletionVectorWriter` are applied by `ArrowReader`.
    #[tokio::test]
    async fn test_deletion_vector_applied_by_reader() -> Result<()> {
        use std::fs::File;

        use arrow_array::cast::AsArray;
        use arrow_array::types::Int32Type;
        use arrow_array::{Int32Array, RecordBatch};
        use futures::TryStreamExt;
        use parquet::arrow::ArrowWriter;

        use crate::Runtime;
        use crate::arrow::{ArrowReaderBuilder, schema_to_arrow_schema};
        use crate::scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream};
        use crate::spec::{NestedField, PrimitiveType, Schema, Type};

        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIO::new_with_fs();
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()?,
        );
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);

        // Data file: ids 1..=5 at positions 0..=4.
        let data_file_path = format!("{}/data.parquet", temp_dir.path().to_str().unwrap());
        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(
            Int32Array::from_iter_values(1..=5),
        )])?;
        let mut parquet_writer =
            ArrowWriter::try_new(File::create(&data_file_path)?, arrow_schema, None)?;
        parquet_writer.write(&batch)?;
        parquet_writer.close()?;

        // Delete positions 1 and 3 (ids 2 and 4).
        let mut writer = writer_builder(&temp_dir).build(None).await?;
        let path: Arc<str> = Arc::from(data_file_path.as_str());
        writer
            .write(vec![
                PositionDeleteInput::new(path.clone(), 3),
                PositionDeleteInput::new(path, 1),
            ])
            .await?;
        let dv = writer.close().await?.pop().unwrap();

        let task = FileScanTask::builder()
            .with_file_size_in_bytes(std::fs::metadata(&data_file_path)?.len())
            .with_start(0)
            .with_length(0)
            .with_record_count(Some(5))
            .with_data_file_path(data_file_path.clone())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![1])
            .with_deletes(vec![
                FileScanTaskDeleteFile::builder()
                    .with_file_path(dv.file_path().to_string())
                    .with_file_size_in_bytes(dv.file_size_in_bytes())
                    .with_file_type(dv.content_type())
                    .with_file_format(dv.file_format())
                    .with_partition_spec_id(dv.partition_spec_id)
                    .with_referenced_data_file(dv.referenced_data_file())
                    .with_content_offset(dv.content_offset().map(|o| o as u64))
                    .with_content_size_in_bytes(dv.content_size_in_bytes().map(|s| s as u64))
                    .with_record_count(Some(dv.record_count()))
                    .build()?,
            ])
            .with_case_sensitive(false)
            .build()?;

        let reader = ArrowReaderBuilder::new(file_io, Runtime::current()).build();
        let tasks = Box::pin(futures::stream::iter(vec![Ok(task)])) as FileScanTaskStream;
        let batches: Vec<RecordBatch> = reader.read(tasks)?.stream().try_collect().await?;
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| b.column(0).as_primitive::<Int32Type>().values().to_vec())
            .collect();
        assert_eq!(ids, vec![1, 3, 5]);

        Ok(())
    }
}
