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

//! `fix(arrow): tag timestamptz columns with the "UTC" zone name`

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, TimestampMicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use iceberg::arrow::{UTC_TIME_ZONE, schema_to_arrow_schema};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type};
use iceberg::transform::create_transform_function;

#[test]
fn timestamptz_columns_use_the_utc_zone_name() {
    assert_eq!(UTC_TIME_ZONE, "UTC");
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "ts", Type::Primitive(PrimitiveType::Timestamptz)).into(),
        ])
        .build()
        .unwrap();
    let arrow_schema = schema_to_arrow_schema(&schema).unwrap();
    assert_eq!(
        arrow_schema.field(0).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
}

#[test]
fn year_and_month_transforms_accept_the_utc_zone_name() {
    // Arrow arrays for Iceberg `timestamptz` carry "UTC", so the year and month transforms
    // must accept the named zone, not only "+00:00".
    let micros = 946_758_642_123_000_i64; // 2000-01-01T20:30:42.123Z
    let array: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone(UTC_TIME_ZONE));

    let years = create_transform_function(&Transform::Year)
        .unwrap()
        .transform(array.clone())
        .unwrap();
    let years = years.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(years.value(0), 30);

    let months = create_transform_function(&Transform::Month)
        .unwrap()
        .transform(array)
        .unwrap();
    let months = months.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(months.value(0), 360);
}
