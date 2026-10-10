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

//! `fix(arrow): write unshredded variants with Java's parquet layout`

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, RecordBatch, StructArray};
use arrow_schema::{DataType, SchemaRef as ArrowSchemaRef};
use arrow_select::concat::concat_batches;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileFormat, NestedField, PrimitiveType, Schema, Struct, Type, VariantType,
};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator, FileNameGenerator, LocationGenerator,
};
use iceberg::writer::file_writer::{FileWriter, FileWriterBuilder, ParquetWriterBuilder};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{LogicalType, Repetition};
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

#[tokio::test]
async fn unshredded_variant_uses_java_parquet_layout() -> anyhow::Result<()> {
    // Unshredded variant columns must be laid out the way Java Iceberg writes them
    // (`TypeToMessageType.variant`): a group annotated with the VARIANT logical type that
    // holds required `metadata` and `value` binary columns.
    let temp_dir = TempDir::new().unwrap();
    let file_io = FileIO::new_with_fs();
    let location_gen =
        DefaultLocationGenerator::with_data_location(temp_dir.path().to_str().unwrap().to_string());
    let file_name_gen =
        DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
            ])
            .build()?,
    );
    let arrow_schema: ArrowSchemaRef = Arc::new(schema_to_arrow_schema(&schema)?);
    let DataType::Struct(variant_fields) = arrow_schema.field(1).data_type() else {
        panic!("variant must map to an Arrow struct");
    };
    // Variant metadata with an empty dictionary, and the int8 value 42.
    let metadata: &[u8] = &[0x01, 0x00, 0x00];
    let value: &[u8] = &[0x0C, 0x2A];
    let variant = StructArray::new(
        variant_fields.clone(),
        vec![
            Arc::new(BinaryArray::from_vec(vec![metadata, metadata])) as ArrayRef,
            Arc::new(BinaryArray::from_vec(vec![value, value])) as ArrayRef,
        ],
        Some(vec![true, false].into()),
    );
    let to_write = RecordBatch::try_new(arrow_schema.clone(), vec![
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        Arc::new(variant) as ArrayRef,
    ])?;

    let output_file = file_io
        .new_output(location_gen.generate_location(None, &file_name_gen.generate_file_name()))?;
    let mut pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), schema)
        .build(output_file)
        .await?;
    pw.write(&to_write).await?;
    let data_file = pw
        .close()
        .await?
        .into_iter()
        .next()
        .unwrap()
        .content(DataContentType::Data)
        .partition(Struct::empty())
        .partition_spec_id(0)
        .build()?;

    // The variant column's leaf columns carry no metrics of their own.
    assert_eq!(data_file.record_count(), 2);
    assert_eq!(*data_file.value_counts(), HashMap::from([(1, 2)]));

    let input = file_io.new_input(data_file.file_path())?.read().await?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(input)?;
    let root = reader.parquet_schema().root_schema();
    let v = &root.get_fields()[1];
    assert_eq!(v.name(), "v");
    assert!(matches!(
        v.get_basic_info().logical_type_ref(),
        Some(LogicalType::Variant { .. })
    ));
    assert_eq!(v.get_basic_info().repetition(), Repetition::OPTIONAL);
    let leaves: Vec<_> = v
        .get_fields()
        .iter()
        .map(|f| (f.name().to_string(), f.get_basic_info().repetition()))
        .collect();
    assert_eq!(leaves, vec![
        ("metadata".to_string(), Repetition::REQUIRED),
        ("value".to_string(), Repetition::REQUIRED),
    ]);

    // The written rows read back unchanged.
    let batches: Vec<_> = reader.build()?.collect::<Result<_, _>>()?;
    let read = concat_batches(&batches[0].schema(), &batches)?;
    let read_variant = read
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert!(read_variant.is_valid(0));
    assert!(read_variant.is_null(1));
    let read_value = read_variant
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(read_value.value(0), value);
    Ok(())
}
