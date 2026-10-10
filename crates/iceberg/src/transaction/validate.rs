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

use std::future::Future;

use crate::error::Result;
use crate::table::Table;

/// A trait for validating snapshots in an Iceberg table.
///
/// This trait provides methods to validate snapshots and their history,
/// ensuring data integrity and consistency across table operations.
pub(crate) trait SnapshotValidator {
    /// Validates a snapshot against a table.
    ///
    /// # Arguments
    ///
    /// * `base` - The base table to validate against
    /// * `parent_snapshot_id` - The ID of the parent snapshot, if any. This is usually
    ///   the latest snapshot of the base table, unless it's a non-main branch
    ///   (note: writing to branches is not currently supported)
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or an error if validation fails
    fn validate(
        &self,
        _base: &Table,
        _parent_snapshot_id: Option<i64>,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}
