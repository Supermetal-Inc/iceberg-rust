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

//! `fix(writer): don't descend into variant columns when projecting equality fields`

use std::sync::Arc;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type, VariantType};
use iceberg::writer::base_writer::equality_delete_writer::EqualityDeleteWriterConfig;

#[test]
fn equality_field_search_skips_variant_columns() {
    // A variant column's `metadata` and `value` children carry no field id, so the
    // equality field search must not descend into it, wherever it sits in the schema.
    for fields in [
        vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "payload", Type::Variant(VariantType)).into(),
        ],
        vec![
            NestedField::optional(2, "payload", Type::Variant(VariantType)).into(),
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ],
    ] {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(fields)
                .build()
                .unwrap(),
        );
        let config = EqualityDeleteWriterConfig::new(vec![1], schema).unwrap();
        let projected = config.projected_arrow_schema_ref();
        assert_eq!(projected.fields().len(), 1);
        assert_eq!(projected.field(0).name(), "id");
    }
}
