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

//! Position delete file writer.
//!
//! This writer assumes that incoming delete records are ordered by the tuple `(file_path, pos)`
//! as required by the Iceberg specification. Ordering and deduplication must be handled by the
//! caller (e.g. by using a sorting writer) before passing records to this writer.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::builder::{PrimitiveBuilder, StringBuilder};
use arrow_array::types::Int64Type;
use once_cell::sync::Lazy;

use super::position_delete_writer as upstream;
use crate::spec::{DataFile, PartitionKey, Schema};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use crate::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Canonical schema for position delete files.
pub static POSITION_DELETE_SCHEMA: Lazy<Schema> =
    Lazy::new(|| upstream::position_delete_schema().as_ref().clone());

/// A row position to delete from a data file.
#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub struct PositionDeleteInput {
    /// Data file path.
    pub path: Arc<str>,
    /// Row position within the file.
    pub pos: i64,
}
impl PositionDeleteInput {
    /// Creates a position delete.
    pub fn new(path: Arc<str>, row: i64) -> Self {
        Self { path, pos: row }
    }
}

/// Builds a position-delete writer accepting row positions.
#[derive(Clone, Debug)]
pub struct PositionDeleteFileWriterBuilder<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: upstream::PositionDeleteFileWriterBuilder<B, L, F>,
}
impl<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator>
    PositionDeleteFileWriterBuilder<B, L, F>
{
    /// Wraps a rolling Parquet writer.
    pub fn new(inner: RollingFileWriterBuilder<B, L, F>) -> Self {
        Self {
            inner: upstream::PositionDeleteFileWriterBuilder::new(inner),
        }
    }
}
#[async_trait::async_trait]
impl<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator>
    IcebergWriterBuilder<Vec<PositionDeleteInput>> for PositionDeleteFileWriterBuilder<B, L, F>
{
    type R = PositionDeleteFileWriter<B, L, F>;
    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Ok(PositionDeleteFileWriter {
            inner: self.inner.build(partition_key).await?,
        })
    }
}

/// Adapts row positions to the canonical position-delete writer.
pub struct PositionDeleteFileWriter<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner: upstream::PositionDeleteFileWriter<B, L, F>,
}
#[async_trait::async_trait]
impl<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator>
    IcebergWriter<Vec<PositionDeleteInput>> for PositionDeleteFileWriter<B, L, F>
{
    async fn write(&mut self, input: Vec<PositionDeleteInput>) -> Result<()> {
        if input.is_empty() {
            return Ok(());
        }
        self.inner.write(build_position_delete_batch(input)?).await
    }
    async fn close(&mut self) -> Result<Vec<DataFile>> {
        self.inner.close().await
    }
}
impl<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator> CurrentFileStatus
    for PositionDeleteFileWriter<B, L, F>
{
    fn current_file_path(&self) -> String {
        self.inner.current_file_path()
    }
    fn current_row_num(&self) -> usize {
        self.inner.current_row_num()
    }
    fn current_written_size(&self) -> usize {
        self.inner.current_written_size()
    }
}

fn build_position_delete_batch(input: Vec<PositionDeleteInput>) -> Result<RecordBatch> {
    let mut path_column_builder = StringBuilder::new();
    let mut offset_column_builder = PrimitiveBuilder::<Int64Type>::new();

    for pd_input in input {
        path_column_builder.append_value(pd_input.path.as_ref());
        offset_column_builder.append_value(pd_input.pos);
    }

    RecordBatch::try_new(upstream::position_delete_arrow_schema(), vec![
        Arc::new(path_column_builder.finish()),
        Arc::new(offset_column_builder.finish()),
    ])
    .map_err(|e| Error::new(ErrorKind::DataInvalid, e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIO;
    use crate::metadata_columns::{
        RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
    };
    use crate::spec::{
        DataContentType, DataFileFormat, Literal, NestedField, PartitionKey, PartitionSpec,
        PrimitiveType, Schema, Struct, Type,
    };
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::tests::check_parquet_data_file;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder};

    #[tokio::test]
    async fn test_position_delete_writer_unpartitioned() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("pos_del".to_string(), None, DataFileFormat::Parquet);

        let schema = Arc::new(POSITION_DELETE_SCHEMA.clone());
        let parquet_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_builder)
            .build(None)
            .await?;

        let deletes = vec![
            PositionDeleteInput::new(Arc::from("s3://bucket/data/file-1.parquet"), 1),
            PositionDeleteInput::new(Arc::from("s3://bucket/data/file-1.parquet"), 2),
            PositionDeleteInput::new(Arc::from("s3://bucket/data/file-2.parquet"), 5),
        ];

        let expected_batch = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        RESERVED_FIELD_ID_DELETE_FILE_PATH.to_string(),
                    )])),
                arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false)
                    .with_metadata(HashMap::from([(
                        PARQUET_FIELD_ID_META_KEY.to_string(),
                        RESERVED_FIELD_ID_DELETE_FILE_POS.to_string(),
                    )])),
            ])),
            vec![
                Arc::new(StringArray::from(vec![
                    "s3://bucket/data/file-1.parquet",
                    "s3://bucket/data/file-1.parquet",
                    "s3://bucket/data/file-2.parquet",
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 5])),
            ],
        )?;

        writer.write(deletes).await?;
        let data_files = writer.close().await?;

        assert_eq!(data_files.len(), 1);
        assert_eq!(
            data_files[0].content_type(),
            DataContentType::PositionDeletes
        );
        assert_eq!(data_files[0].partition(), &Struct::empty());

        check_parquet_data_file(&file_io, &data_files[0], &expected_batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_position_delete_writer_partitioned() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIO::new_with_fs();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen = DefaultFileNameGenerator::new(
            "pos_del_part".to_string(),
            None,
            DataFileFormat::Parquet,
        );

        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(10, "region", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;
        let schema_ref = Arc::new(schema);

        let parquet_builder = ParquetWriterBuilder::new(
            WriterProperties::builder().build(),
            Arc::new(POSITION_DELETE_SCHEMA.clone()),
        );

        let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let partition_value = Struct::from_iter([Some(Literal::string("us"))]);
        let partition_key = PartitionKey::new(
            PartitionSpec::builder(schema_ref.clone()).build()?,
            schema_ref,
            partition_value.clone(),
        );

        let mut writer = PositionDeleteFileWriterBuilder::new(rolling_builder)
            .build(Some(partition_key))
            .await?;

        writer
            .write(vec![PositionDeleteInput::new(
                Arc::from("s3://bucket/data/file.parquet"),
                10,
            )])
            .await?;

        let data_files = writer.close().await?;
        assert_eq!(data_files.len(), 1);
        assert_eq!(
            data_files[0].content_type(),
            DataContentType::PositionDeletes
        );
        assert_eq!(data_files[0].partition(), &partition_value);

        Ok(())
    }
}
