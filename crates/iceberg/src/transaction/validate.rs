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

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::sync::Arc;

use futures::SinkExt;
use futures::future::try_join_all;

use crate::delete_file_index::DeleteFileIndex;
use crate::error::Result;
use crate::scan::DeleteFileContext;
use crate::spec::{
    DataContentType, DataFile, FormatVersion, INITIAL_SEQUENCE_NUMBER, Literal,
    ManifestContentType, ManifestFile, Operation, PrimitiveType, SnapshotRef, Struct, StructType,
    TableMetadata,
};
use crate::table::Table;
use crate::util::snapshot::ancestors_between;
use crate::{Error, ErrorKind};

/// Operations whose snapshots can add delete files that apply to existing data files.
const VALIDATE_ADDED_DELETE_FILES_OPERATIONS: &[Operation] =
    &[Operation::Overwrite, Operation::Delete];

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

/// Collects, for each snapshot after `from_snapshot_id` up to `to_snapshot_id` whose operation
/// is one of `matching_operations`, the manifests of `manifest_content_type` that the snapshot
/// added, newest snapshot first.
///
/// # Arguments
///
/// * `base` - The base table to retrieve history from
/// * `from_snapshot_id` - The starting snapshot ID (exclusive), or None to start from the beginning
/// * `to_snapshot_id` - The ending snapshot ID (inclusive)
/// * `matching_operations` - Operations to match when collecting snapshots
/// * `manifest_content_type` - The content type of manifests to collect
///
/// # Errors
///
/// Returns an error if the history between the snapshots cannot be determined
async fn validation_history(
    base: &Table,
    from_snapshot_id: Option<i64>,
    to_snapshot_id: i64,
    matching_operations: &[Operation],
    manifest_content_type: ManifestContentType,
) -> Result<Vec<Vec<ManifestFile>>> {
    let mut manifests_by_snapshot = Vec::new();
    let mut last_snapshot: Option<SnapshotRef> = None;

    let snapshots: Vec<SnapshotRef> =
        ancestors_between(&base.metadata_ref(), to_snapshot_id, from_snapshot_id).collect();

    for current_snapshot in snapshots {
        last_snapshot = Some(current_snapshot.clone());

        // Find all snapshots with the matching operations
        // and their manifest files with the matching content type
        if matching_operations.contains(&current_snapshot.summary().operation) {
            let manifests = base
                .manifest_list_reader(&current_snapshot)
                .load()
                .await?
                .consume_entries()
                .into_iter()
                .filter(|manifest| {
                    manifest.content == manifest_content_type
                        && manifest.added_snapshot_id == current_snapshot.snapshot_id()
                })
                .collect();
            manifests_by_snapshot.push(manifests);
        }
    }

    if let Some(last_snapshot) = last_snapshot
        && last_snapshot.parent_snapshot_id() != from_snapshot_id
    {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot determine history between starting snapshot {} and the last known ancestor {}",
                from_snapshot_id.unwrap_or(-1),
                last_snapshot.snapshot_id()
            ),
        ));
    }

    Ok(manifests_by_snapshot)
}

/// Validates that no delete files that apply to `data_files` were added after
/// `from_snapshot_id`.
///
/// # Arguments
///
/// * `base` - The base table to validate against
/// * `from_snapshot_id` - The starting snapshot ID (exclusive), or None to start from the beginning
/// * `to_snapshot_id` - The ending snapshot ID (inclusive), or None if there is no current table state
/// * `data_files` - The data files to check for conflicting delete files
/// * `ignore_equality_deletes` - Whether to ignore equality deletes and only check for positional deletes
///
/// # Errors
///
/// Returns an error if new delete files are found for any of the data files
pub(crate) async fn validate_no_new_deletes_for_data_files(
    base: &Table,
    from_snapshot_id: Option<i64>,
    to_snapshot_id: Option<i64>,
    data_files: &[DataFile],
    ignore_equality_deletes: bool,
) -> Result<()> {
    // Without a current snapshot, the table has no files. V1 tables
    // cannot contain row delete files.
    let Some(to_snapshot_id) = to_snapshot_id else {
        return Ok(());
    };
    if base.metadata().format_version() == FormatVersion::V1 {
        return Ok(());
    }

    // Get the delete manifests each snapshot added since the from_snapshot_id
    let delete_manifests_by_snapshot = validation_history(
        base,
        from_snapshot_id,
        to_snapshot_id,
        VALIDATE_ADDED_DELETE_FILES_OPERATIONS,
        ManifestContentType::Deletes,
    )
    .await?;

    // Get starting seq num from starting snapshot if available
    let starting_sequence_number = from_snapshot_id
        .and_then(|id| base.metadata().snapshot_by_id(id))
        .map_or(INITIAL_SEQUENCE_NUMBER, |snapshot| {
            snapshot.sequence_number()
        });

    let mut partition_types = HashMap::new();
    let data_files = data_files
        .iter()
        .map(|data_file| {
            let promoted = promoted_partition(data_file, base.metadata(), &mut partition_types)?;
            Ok(match promoted {
                Some(partition) => Cow::Owned(DataFile {
                    partition,
                    ..data_file.clone()
                }),
                None => Cow::Borrowed(data_file),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Index each snapshot's deletes separately: deletes added by different snapshots can
    // replace each other, such as a data file's deletion vector and the one replacing it,
    // and an index holds at most one deletion vector per data file.
    for delete_manifests in delete_manifests_by_snapshot {
        let (delete_file_index, mut delete_file_tx) = DeleteFileIndex::new(base.runtime().clone());
        let manifests = try_join_all(
            delete_manifests
                .iter()
                .map(|f| base.manifest_reader().read(f))
                .collect::<Vec<_>>(),
        )
        .await?;
        for entry in manifests.iter().flat_map(|manifest| manifest.entries()) {
            // Only deletes added after the starting snapshot can conflict. DELETED entries are
            // not live, and a rewritten manifest carries older deletes forward as EXISTING
            // entries with their original sequence numbers.
            if !entry.is_alive()
                || entry
                    .sequence_number()
                    .is_some_and(|seq| seq <= starting_sequence_number)
            {
                continue;
            }
            let promoted =
                promoted_partition(entry.data_file(), base.metadata(), &mut partition_types)?;
            let manifest_entry = match promoted {
                Some(partition) => {
                    let mut entry = entry.as_ref().clone();
                    entry.data_file.partition = partition;
                    Arc::new(entry)
                }
                None => entry.clone(),
            };
            let delete_file_ctx = DeleteFileContext {
                partition_spec_id: manifest_entry.data_file().partition_spec_id,
                manifest_entry,
            };
            delete_file_tx.send(delete_file_ctx).await?;
        }
        // Close the channel so the index finishes populating; lookups wait until it has.
        drop(delete_file_tx);

        // Validate if there are deletes using delete file index
        for data_file in &data_files {
            let delete_files = delete_file_index
                .get_deletes_for_data_file(data_file, Some(starting_sequence_number))
                .await?;

            if ignore_equality_deletes {
                if delete_files
                    .iter()
                    .any(|delete_file| delete_file.file_type() == DataContentType::PositionDeletes)
                {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Cannot commit, found new positional delete for added data file: {}",
                            data_file.file_path
                        ),
                    ));
                }
            } else if !delete_files.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "Cannot commit, found new delete for added data file: {}",
                        data_file.file_path
                    ),
                ));
            }
        }
    }

    Ok(())
}

/// Returns `file`'s partition with values written before a type promotion widened, or `None`
/// when nothing widens. A file written before an `int` or `float` partition source was
/// promoted keeps the narrower value, while one written after carries the wider value, and the
/// delete index compares values exactly. The scan widens values the same way before it
/// matches deletes. A dropped source has no current type, so its numeric values widen to
/// their ceiling. Both sides of a comparison go through here, and the result is never written.
fn promoted_partition(
    file: &DataFile,
    metadata: &TableMetadata,
    partition_types: &mut HashMap<i32, StructType>,
) -> Result<Option<Struct>> {
    let current = match partition_types.entry(file.partition_spec_id) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            let spec = metadata
                .partition_spec_by_id(file.partition_spec_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Partition spec {} of {} is not in the table",
                            file.partition_spec_id, file.file_path
                        ),
                    )
                })?;
            entry.insert(spec.partition_type(metadata.current_schema())?)
        }
    };
    let promoted: Vec<_> = file
        .partition
        .iter()
        .zip(current.fields())
        .map(|(value, field)| {
            let Some(Literal::Primitive(literal)) = value else {
                return None;
            };
            match field.field_type.as_primitive_type()? {
                PrimitiveType::Unknown => literal.widen(),
                target => literal.promote_to(target),
            }
        })
        .collect();
    if promoted.iter().all(Option::is_none) {
        return Ok(None);
    }
    Ok(Some(
        file.partition
            .iter()
            .zip(promoted)
            .map(|(value, promoted)| promoted.map(Literal::Primitive).or_else(|| value.cloned()))
            .collect(),
    ))
}
