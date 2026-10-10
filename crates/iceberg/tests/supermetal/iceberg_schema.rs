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

//! `feat(writer): add `iceberg.schema` to the parquet footer` and
//! `fix(writer): embed `iceberg.schema` in the Arrow schema metadata`

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_schema::{DataType, SchemaRef as ArrowSchemaRef};
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, ListType, NestedField, PrimitiveType, Schema,
    Struct, StructType, Type,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

/// The key Java and this writer store the Iceberg schema JSON under.
const ICEBERG_SCHEMA_KEY: &str = "iceberg.schema";

/// Helper to read the Parquet file footer key-value metadata from a written data file.
async fn read_parquet_kv_metadata(file_io: &FileIO, data_file: &DataFile) -> Option<Vec<KeyValue>> {
    let input_file = file_io.new_input(data_file.file_path()).unwrap();
    let input_content = input_file.read().await.unwrap();
    let reader_builder = ParquetRecordBatchReaderBuilder::try_new(input_content).unwrap();
    reader_builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .cloned()
}

/// Helper to write a simple parquet file and return the data file, file_io,
/// and TempDir (must be kept alive so the file remains on disk).
async fn write_simple_parquet_file(
    props: WriterProperties,
) -> anyhow::Result<(DataFile, FileIO, TempDir)> {
    let temp_dir = TempDir::new().unwrap();
    let file_io = FileIO::new_with_fs();
    let location_gen =
        DefaultLocationGenerator::with_data_location(temp_dir.path().to_str().unwrap().to_string());
    let file_name_gen =
        DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

    let iceberg_schema = Arc::new(
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(0, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(1, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap(),
    );

    let arrow_schema: ArrowSchemaRef = Arc::new(iceberg_schema.as_ref().try_into().unwrap());
    let col0 = Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef;
    let col1 = Arc::new(arrow_array::StringArray::from(vec![
        Some("a"),
        Some("b"),
        Some("c"),
        None,
        Some("e"),
        Some("f"),
        Some("g"),
        Some("h"),
        Some("i"),
        Some("j"),
    ])) as ArrayRef;
    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![col0, col1]).unwrap();

    let output_file = file_io
        .new_output(location_gen.generate_location(None, &file_name_gen.generate_file_name()))?;

    let mut pw = ParquetWriterBuilder::new(props, iceberg_schema)
        .build(output_file)
        .await?;
    pw.write(&batch).await?;
    let res = pw.close().await?;
    assert_eq!(res.len(), 1);
    let data_file = res
        .into_iter()
        .next()
        .unwrap()
        .content(DataContentType::Data)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .build()
        .unwrap();

    Ok((data_file, file_io, temp_dir))
}

#[tokio::test]
async fn test_iceberg_schema_metadata_present_in_parquet_footer() -> anyhow::Result<()> {
    let (data_file, file_io, _temp_dir) =
        write_simple_parquet_file(WriterProperties::builder().build()).await?;

    let kv_metadata = read_parquet_kv_metadata(&file_io, &data_file).await;
    let kv_metadata = kv_metadata.expect("key_value_metadata should be present");

    let iceberg_schema_entry = kv_metadata.iter().find(|kv| kv.key == ICEBERG_SCHEMA_KEY);
    assert!(
        iceberg_schema_entry.is_some(),
        "Parquet footer must contain '{ICEBERG_SCHEMA_KEY}' key-value metadata"
    );

    let schema_json = iceberg_schema_entry
        .unwrap()
        .value
        .as_ref()
        .expect("iceberg.schema value should not be None");
    assert!(
        !schema_json.is_empty(),
        "iceberg.schema JSON should not be empty"
    );

    Ok(())
}

#[tokio::test]
async fn test_iceberg_schema_metadata_roundtrip() -> anyhow::Result<()> {
    let (data_file, file_io, _temp_dir) =
        write_simple_parquet_file(WriterProperties::builder().build()).await?;

    let kv_metadata = read_parquet_kv_metadata(&file_io, &data_file)
        .await
        .expect("key_value_metadata should be present");

    let schema_json = kv_metadata
        .iter()
        .find(|kv| kv.key == ICEBERG_SCHEMA_KEY)
        .expect("iceberg.schema key must be present")
        .value
        .as_ref()
        .expect("iceberg.schema value must not be None");

    // Deserialize back to an Iceberg Schema
    let deserialized: Schema = serde_json::from_str(schema_json)
        .expect("iceberg.schema JSON should deserialize to a valid Schema");

    // Verify schema ID
    assert_eq!(deserialized.schema_id(), 1);

    // Verify field count
    assert_eq!(deserialized.as_struct().fields().len(), 2);

    // Verify field IDs and names
    let id_field = deserialized.field_by_id(0).expect("field 0 should exist");
    assert_eq!(id_field.name, "id");

    let name_field = deserialized.field_by_id(1).expect("field 1 should exist");
    assert_eq!(name_field.name, "name");

    Ok(())
}

#[tokio::test]
async fn test_iceberg_schema_metadata_with_nested_schema() -> anyhow::Result<()> {
    // Use a nested schema with Struct and List types to verify the iceberg.schema
    // JSON correctly roundtrips complex types including field IDs.
    let schema = Schema::builder()
        .with_schema_id(2)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(
                2,
                "location",
                Type::Struct(StructType::new(vec![
                    NestedField::required(3, "lat", Type::Primitive(PrimitiveType::Double)).into(),
                    NestedField::required(4, "lon", Type::Primitive(PrimitiveType::Double)).into(),
                ])),
            )
            .into(),
            NestedField::optional(
                5,
                "tags",
                Type::List(ListType::new(
                    NestedField::required(6, "element", Type::Primitive(PrimitiveType::String))
                        .into(),
                )),
            )
            .into(),
        ])
        .build()
        .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let file_io = FileIO::new_with_fs();
    let location_gen =
        DefaultLocationGenerator::with_data_location(temp_dir.path().to_str().unwrap().to_string());
    let file_name_gen =
        DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

    let arrow_schema: ArrowSchemaRef = Arc::new((&schema).try_into().unwrap());

    // Build columns
    let col_id = Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef;
    let col_location = Arc::new(StructArray::new(
        {
            if let DataType::Struct(fields) = arrow_schema.field(1).data_type() {
                fields.clone()
            } else {
                unreachable!()
            }
        },
        vec![
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
            Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0])) as ArrayRef,
        ],
        None,
    )) as ArrayRef;
    let col_tags = Arc::new({
        let values = arrow_array::StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
        let offsets =
            arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(vec![0i32, 2, 4, 6]));
        let field = arrow_schema.field(2).clone();
        if let DataType::List(inner) = field.data_type() {
            ListArray::new(inner.clone(), offsets, Arc::new(values), None)
        } else {
            unreachable!()
        }
    }) as ArrayRef;

    let batch =
        RecordBatch::try_new(arrow_schema.clone(), vec![col_id, col_location, col_tags]).unwrap();

    let output_file = file_io
        .new_output(location_gen.generate_location(None, &file_name_gen.generate_file_name()))?;

    let mut pw = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        Arc::new(schema.clone()),
    )
    .build(output_file)
    .await?;
    pw.write(&batch).await?;
    let res = pw.close().await?;
    assert_eq!(res.len(), 1);
    let data_file = res
        .into_iter()
        .next()
        .unwrap()
        .content(DataContentType::Data)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .build()
        .unwrap();

    let kv_metadata = read_parquet_kv_metadata(&file_io, &data_file)
        .await
        .expect("key_value_metadata should be present");

    let schema_json = kv_metadata
        .iter()
        .find(|kv| kv.key == ICEBERG_SCHEMA_KEY)
        .expect("iceberg.schema key must be present for nested schema")
        .value
        .as_ref()
        .expect("iceberg.schema value must not be None");

    let deserialized: Schema =
        serde_json::from_str(schema_json).expect("nested iceberg.schema JSON should deserialize");

    // Verify the nested schema round-trips correctly
    assert_eq!(deserialized, schema);

    // Spot-check nested field IDs survived the roundtrip
    assert_eq!(deserialized.field_by_id(3).unwrap().name, "lat");
    assert_eq!(deserialized.field_by_id(6).unwrap().name, "element");

    Ok(())
}

#[tokio::test]
async fn test_iceberg_schema_preserves_existing_kv_metadata() -> anyhow::Result<()> {
    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "custom.key".to_string(),
            "custom_value".to_string(),
        )]))
        .build();

    let (data_file, file_io, _temp_dir) = write_simple_parquet_file(props).await?;

    let kv_metadata = read_parquet_kv_metadata(&file_io, &data_file)
        .await
        .expect("key_value_metadata should be present");

    // The caller's custom metadata should still be present
    let custom_entry = kv_metadata.iter().find(|kv| kv.key == "custom.key");
    assert!(
        custom_entry.is_some(),
        "Caller's custom key-value metadata must be preserved"
    );
    assert_eq!(custom_entry.unwrap().value.as_deref(), Some("custom_value"));

    // iceberg.schema should also be present
    let iceberg_entry = kv_metadata.iter().find(|kv| kv.key == ICEBERG_SCHEMA_KEY);
    assert!(
        iceberg_entry.is_some(),
        "iceberg.schema must be present alongside custom metadata"
    );

    Ok(())
}
