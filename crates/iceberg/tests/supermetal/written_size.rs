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

//! `feat(writer): expose written size of partitioning writers`

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::io::FileIO;
use iceberg::spec::{
    DataFileFormat, Literal, NestedField, PartitionKey, PartitionSpec, PrimitiveType, Schema,
    SchemaRef, Struct, Type,
};
use iceberg::writer::CurrentFileStatus;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::unpartitioned_writer::UnpartitionedWriter;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

fn schema() -> SchemaRef {
    Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap(),
    )
}

fn batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    let field = |name: &str, data_type, id: i32| {
        Field::new(name, data_type, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    };
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            field("id", DataType::Int32, 1),
            field("name", DataType::Utf8, 2),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn data_file_writer_builder(
    temp_dir: &TempDir,
) -> DataFileWriterBuilder<ParquetWriterBuilder, DefaultLocationGenerator, DefaultFileNameGenerator>
{
    DataFileWriterBuilder::new(RollingFileWriterBuilder::new_with_default_file_size(
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema()),
        FileIO::new_with_fs(),
        DefaultLocationGenerator::with_data_location(temp_dir.path().to_str().unwrap().to_string()),
        DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet),
    ))
}

#[tokio::test]
async fn unpartitioned_writer_reports_current_file_status() {
    let temp_dir = TempDir::new().unwrap();
    let mut writer = UnpartitionedWriter::new(data_file_writer_builder(&temp_dir));

    // Before the first write there is no file yet.
    assert_eq!(writer.current_file_path(), "");
    assert_eq!(writer.current_row_num(), 0);
    assert_eq!(writer.current_written_size(), 0);

    writer
        .write(batch(vec![1, 2], vec!["a", "b"]))
        .await
        .unwrap();
    writer
        .write(batch(vec![3, 4], vec!["c", "d"]))
        .await
        .unwrap();

    assert!(!writer.current_file_path().is_empty());
    assert_eq!(writer.current_row_num(), 4);
    assert!(writer.current_written_size() > 0);
    writer.close().await.unwrap();
}

#[tokio::test]
async fn fanout_writer_sums_written_size_over_partitions() {
    let temp_dir = TempDir::new().unwrap();
    let mut writer = FanoutWriter::new(data_file_writer_builder(&temp_dir));
    let partition_spec = PartitionSpec::builder(schema()).build().unwrap();
    let partition_key = |name: &str| {
        PartitionKey::new(
            partition_spec.clone(),
            schema(),
            Struct::from_iter([Some(Literal::string(name))]),
        )
    };

    assert_eq!(writer.total_written_size(), 0);
    writer
        .write(partition_key("a"), batch(vec![1], vec!["a"]))
        .await
        .unwrap();
    let first = writer.total_written_size();
    assert!(first > 0);

    // The total covers every open partition writer.
    writer
        .write(partition_key("b"), batch(vec![2], vec!["b"]))
        .await
        .unwrap();
    assert!(writer.total_written_size() > first);
    writer.close().await.unwrap();
}
