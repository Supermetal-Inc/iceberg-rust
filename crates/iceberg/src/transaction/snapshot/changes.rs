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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use uuid::Uuid;

use crate::error::invalid_data;
use crate::spec::{
    DataContentType, DataFile, Datum, FormatVersion, ManifestContentType, ManifestEntry,
    ManifestFile, ManifestStatus, ManifestWriter, Operation, PartitionSpecRef, PrimitiveLiteral,
    PrimitiveType, Schema, SchemaRef,
};
use crate::table::Table;
use crate::transaction::ActionCommit;
use crate::transaction::validate::SnapshotValidator;
use crate::{Error, ErrorKind, Result};

pub(crate) trait SnapshotProduceOperation: Send + Sync + SnapshotValidator {
    /// Returns the operation type that will be recorded in the snapshot summary.
    ///
    /// This determines what kind of operation is being performed (e.g., `Append`, `Overwrite`),
    /// which is stored in the snapshot metadata for tracking and auditing purposes.
    fn operation(&self) -> Operation;

    /// Whether this operation replaces the complete logical table contents.
    fn is_full_table_overwrite(&self) -> bool {
        false
    }

    /// Returns manifest entries that should be marked as deleted in the new snapshot.
    fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer,
    ) -> impl Future<Output = Result<Vec<ManifestEntry>>> + Send;

    /// Returns existing manifest files that should be included in the new snapshot.
    ///
    /// This method determines which manifest files from the current snapshot should be
    /// carried forward to the new snapshot. The selection depends on the operation type:
    ///
    /// - **Append operations**: Typically include all existing manifests
    /// - **Overwrite operations**: May exclude manifests for partitions being overwritten
    /// - **Delete operations**: May exclude manifests for partitions being deleted
    fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> impl Future<Output = Result<Vec<ManifestFile>>> + Send;
}

pub(crate) struct ChangeSnapshotState {
    commit_uuid: Uuid,
    snapshot_id: Option<i64>,
    manifest_counter: u64,
    attempt: u64,
}

impl ChangeSnapshotState {
    pub(crate) fn new(commit_uuid: Option<Uuid>) -> Self {
        Self {
            commit_uuid: commit_uuid.unwrap_or_else(Uuid::now_v7),
            snapshot_id: None,
            manifest_counter: 0,
            attempt: 0,
        }
    }
}

pub(crate) struct SnapshotProducer<'a> {
    pub(crate) table: &'a Table,
    state: &'a mut ChangeSnapshotState,
    snapshot_id: i64,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    pub(crate) deleted_data_files: Vec<DataFile>,
    pub(crate) deleted_delete_files: Vec<DataFile>,
}

impl<'a> SnapshotProducer<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        table: &'a Table,
        state: &'a mut ChangeSnapshotState,
        key_metadata: Option<Vec<u8>>,
        snapshot_properties: HashMap<String, String>,
        added_data_files: Vec<DataFile>,
        added_delete_files: Vec<DataFile>,
        deleted_data_files: Vec<DataFile>,
        deleted_delete_files: Vec<DataFile>,
    ) -> Self {
        let snapshot_id = *state
            .snapshot_id
            .get_or_insert_with(|| super::generate_snapshot_id(table));
        Self {
            table,
            state,
            snapshot_id,
            key_metadata,
            snapshot_properties,
            added_data_files,
            added_delete_files,
            deleted_data_files,
            deleted_delete_files,
        }
    }
    pub(crate) fn validate_added_data_files(&self, added_data_files: &[DataFile]) -> Result<()> {
        for data_file in added_data_files {
            // Check if the data file partition spec id matches the table default partition spec id.
            if self.table.metadata().default_partition_spec_id() != data_file.partition_spec_id {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Data file partition spec id does not match table default partition spec id",
                ));
            }
            super::validate_partition_value(
                data_file.partition(),
                self.table.metadata().default_partition_type(),
            )?;
        }

        Ok(())
    }

    pub(crate) fn validate_added_delete_files(&self) -> Result<()> {
        crate::transaction::row_delta::RowDeltaAction::validate_delete_files(
            &self.added_delete_files,
            "added",
        )?;
        let metadata = self.table.metadata();
        for delete_file in &self.added_delete_files {
            crate::transaction::row_delta::RowDeltaAction::validate_format_version(
                [delete_file],
                metadata.format_version(),
            )?;
            // A delete file is written under the partition spec of the data it deletes from,
            // which need not be the default spec.
            let partition_spec = self.partition_spec(delete_file.partition_spec_id)?;
            let schema =
                super::partition_schema(self.table, partition_spec, delete_file.partition());
            super::validate_partition_value(
                delete_file.partition(),
                &partition_spec.partition_type(&schema)?,
            )?;
        }
        Ok(())
    }

    pub(crate) fn removed_files(&self) -> RemovedFiles<'_> {
        RemovedFiles::new(&self.deleted_data_files, &self.deleted_delete_files)
    }

    pub(crate) async fn validate_duplicate_files(&self) -> Result<()> {
        let files: Vec<_> = self
            .added_data_files
            .iter()
            .chain(&self.added_delete_files)
            .cloned()
            .collect();
        super::validate_files_not_live(self.table, &files).await
    }

    pub(crate) fn new_manifest_writer(
        &mut self,
        content: ManifestContentType,
        schema: SchemaRef,
        spec: PartitionSpecRef,
    ) -> Result<ManifestWriter> {
        let path = super::manifest_path(
            self.table,
            self.state.commit_uuid,
            self.state.manifest_counter,
        )?;
        self.state.manifest_counter += 1;
        super::new_manifest_writer(self.table, path, self.snapshot_id, content, schema, spec)
    }
    fn partition_spec(&self, spec_id: i32) -> Result<&PartitionSpecRef> {
        self.table
            .metadata()
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| invalid_data!("Unknown partition spec id {spec_id}"))
    }

    fn group_by_partition_schema<T>(
        &self,
        files: impl IntoIterator<Item = T>,
        data_file: impl Fn(&T) -> &DataFile,
    ) -> Result<Vec<(PartitionSpecRef, SchemaRef, Vec<T>)>> {
        let mut groups: Vec<(PartitionSpecRef, SchemaRef, Vec<T>)> = Vec::new();
        for file in files {
            let partition_spec = self.partition_spec(data_file(&file).partition_spec_id)?;
            let schema =
                super::partition_schema(self.table, partition_spec, data_file(&file).partition());
            match groups.iter_mut().find(|(spec, group_schema, _)| {
                spec.spec_id() == partition_spec.spec_id()
                    && group_schema.schema_id() == schema.schema_id()
            }) {
                Some((_, _, group)) => group.push(file),
                None => groups.push((partition_spec.clone(), schema, vec![file])),
            }
        }
        Ok(groups)
    }

    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifests(
        &mut self,
        content_type: ManifestContentType,
    ) -> Result<Vec<ManifestFile>> {
        let added_data_files = match content_type {
            ManifestContentType::Data => std::mem::take(&mut self.added_data_files),
            ManifestContentType::Deletes => std::mem::take(&mut self.added_delete_files),
        };
        if added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files found when write an added manifest file",
            ));
        }

        let snapshot_id = self.snapshot_id;
        let format_version = self.table.metadata().format_version();
        let mut manifests = Vec::new();
        for (partition_spec, schema, files) in
            self.group_by_partition_schema(added_data_files, |file| file)?
        {
            let mut writer =
                self.new_manifest_writer(content_type, schema.clone(), partition_spec)?;
            for mut data_file in files {
                fit_bounds_to_schema(&mut data_file, &schema);
                let builder = ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(data_file);
                let entry = if format_version == FormatVersion::V1 {
                    builder.snapshot_id(snapshot_id).build()
                } else {
                    // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                    // commit failed.
                    builder.build()
                };
                writer.add_entry(entry)?;
            }
            manifests.push(writer.write_manifest_file().await?);
        }
        Ok(manifests)
    }

    async fn write_deleted_manifests(
        &mut self,
        deleted_entries: Vec<ManifestEntry>,
    ) -> Result<Vec<ManifestFile>> {
        let mut deleted_manifests = Vec::new();
        for (partition_spec, schema, entries) in
            self.group_by_partition_schema(deleted_entries, |entry| entry.data_file())?
        {
            let mut data_manifest_writer: Option<ManifestWriter> = None;
            let mut delete_manifest_writer: Option<ManifestWriter> = None;
            for mut entry in entries {
                fit_bounds_to_schema(&mut entry.data_file, &schema);
                let (writer, content) = match entry.data_file().content_type() {
                    DataContentType::Data => (&mut data_manifest_writer, ManifestContentType::Data),
                    DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                        (&mut delete_manifest_writer, ManifestContentType::Deletes)
                    }
                };
                if writer.is_none() {
                    *writer = Some(self.new_manifest_writer(
                        content,
                        schema.clone(),
                        partition_spec.clone(),
                    )?);
                }
                if let Some(writer) = writer {
                    writer.add_delete_entry(entry)?;
                }
            }

            for writer in [data_manifest_writer, delete_manifest_writer]
                .into_iter()
                .flatten()
            {
                deleted_manifests.push(writer.write_manifest_file().await?);
            }
        }

        Ok(deleted_manifests)
    }

    pub(crate) async fn existing_manifests(&mut self) -> Result<Vec<ManifestFile>> {
        let table = self.table;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = table.manifest_list_reader(snapshot).load().await?;

        if self.deleted_delete_files.is_empty() && self.deleted_data_files.is_empty() {
            return Ok(manifest_list
                .entries()
                .iter()
                .filter(|entry| entry.has_added_files() || entry.has_existing_files())
                .cloned()
                .collect());
        }

        let mut existing = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await?;

            let removed = self.removed_files();
            let mut has_removal = false;
            let mut has_survivor = false;
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }
                if removed.contains(entry) {
                    has_removal = true;
                } else {
                    has_survivor = true;
                }
            }

            if !has_removal {
                if manifest_file.has_added_files() || manifest_file.has_existing_files() {
                    existing.push(manifest_file.clone());
                }
                continue;
            }

            // Skip the rewrite when every live entry would be dropped.
            // `delete_entries` emits the DELETED rows separately.
            if !has_survivor {
                continue;
            }

            // Preserve `manifest_file.content` so deletes manifests stay
            // deletes after the rewrite. `add_existing_entry` keeps each
            // survivor's original snapshot id and sequence numbers.
            let survivors: Vec<_> = manifest
                .entries()
                .iter()
                .filter(|entry| entry.is_alive() && !removed.contains(entry))
                .collect();
            let mut writer = self.new_manifest_writer(
                manifest_file.content,
                manifest.metadata().schema().clone(),
                Arc::new(manifest.metadata().partition_spec().clone()),
            )?;
            for entry in survivors {
                writer.add_existing_entry((**entry).clone())?;
            }
            existing.push(writer.write_manifest_file().await?);
        }
        Ok(existing)
    }

    pub(crate) async fn delete_entries(&self, require_all: bool) -> Result<Vec<ManifestEntry>> {
        if self.deleted_delete_files.is_empty() && self.deleted_data_files.is_empty() {
            return Ok(vec![]);
        }
        let table = self.table;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = table.manifest_list_reader(snapshot).load().await?;
        let removed = self.removed_files();

        let mut entries = Vec::with_capacity(self.deleted_delete_files.len());
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await?;
            for entry in manifest.entries() {
                if require_all && entry.is_alive() && !removed.contains(entry) {
                    return Err(invalid_data!(
                        "Full table overwrite must delete every live file, including {}",
                        entry.file_path()
                    ));
                }
                if entry.is_alive() && removed.contains(entry) {
                    entries.push(entry.as_ref().clone());
                }
            }
        }
        removed.check_unambiguous(&entries)?;
        Ok(entries)
    }

    async fn manifest_file<OP: SnapshotProduceOperation>(
        &mut self,
        snapshot_produce_operation: &OP,
        deleted_entries: Vec<ManifestEntry>,
    ) -> Result<Vec<ManifestFile>> {
        // Assert current snapshot producer contains new content to add to new snapshot.
        //
        // TODO: Allowing snapshot property setup with no added data files is a workaround.
        // We should clean it up after all necessary actions are supported.
        // For details, please refer to https://github.com/apache/iceberg-rust/issues/1548
        if self.added_data_files.is_empty()
            && self.added_delete_files.is_empty()
            && self.snapshot_properties.is_empty()
            && deleted_entries.is_empty()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "No added data files, delete files, or snapshot properties found when write a manifest file",
            ));
        }

        let existing_manifests = snapshot_produce_operation.existing_manifest(self).await?;
        let mut manifest_files = existing_manifests;

        // Process added data entries.
        if !self.added_data_files.is_empty() {
            manifest_files.extend(
                self.write_added_manifests(ManifestContentType::Data)
                    .await?,
            );
        }
        if !self.added_delete_files.is_empty() {
            manifest_files.extend(
                self.write_added_manifests(ManifestContentType::Deletes)
                    .await?,
            );
        }

        let delete_manifests = self.write_deleted_manifests(deleted_entries).await?;
        manifest_files.extend(delete_manifests);

        Ok(manifest_files)
    }

    pub(crate) async fn commit<OP: SnapshotProduceOperation>(
        mut self,
        operation: OP,
    ) -> Result<ActionCommit> {
        operation
            .validate(self.table, self.table.metadata().current_snapshot_id())
            .await?;
        let deleted_entries = operation.delete_entries(&self).await?;
        let added_files: Vec<_> = self
            .added_data_files
            .iter()
            .chain(&self.added_delete_files)
            .cloned()
            .collect();
        let removed_files: Vec<_> = deleted_entries
            .iter()
            .map(|e| e.data_file().clone())
            .collect();
        let mut manifests = self.manifest_file(&operation, deleted_entries).await?;
        if let Some(key_metadata) = &self.key_metadata {
            for manifest in &mut manifests {
                if manifest.added_snapshot_id == self.snapshot_id && manifest.key_metadata.is_none()
                {
                    manifest.key_metadata = Some(key_metadata.clone());
                }
            }
        }
        let path = super::manifest_list_path(
            self.table,
            self.snapshot_id,
            self.state.attempt,
            self.state.commit_uuid,
        )?;
        self.state.attempt += 1;
        super::write_snapshot_commit(self.table, super::SnapshotCommitInput {
            snapshot_id: self.snapshot_id,
            manifest_list_path: path,
            operation: operation.operation(),
            properties: self.snapshot_properties,
            added_files: &added_files,
            removed_files: &removed_files,
            manifests,
            truncate_full_table: operation.is_full_table_overwrite(),
        })
        .await
    }
}

pub(crate) struct RemovedFiles<'a> {
    data_paths: HashSet<&'a str>,
    delete_paths: HashSet<&'a str>,
    delete_offsets: HashSet<(&'a str, i64)>,
    delete_references: HashSet<(&'a str, &'a str)>,
}

impl<'a> RemovedFiles<'a> {
    fn new(data_files: &'a [DataFile], delete_files: &'a [DataFile]) -> Self {
        let mut removed = Self {
            data_paths: data_files.iter().map(|f| f.file_path()).collect(),
            delete_paths: HashSet::new(),
            delete_offsets: HashSet::new(),
            delete_references: HashSet::new(),
        };
        for file in delete_files {
            let path = file.file_path();
            if let Some(offset) = file.content_offset {
                removed.delete_offsets.insert((path, offset));
            } else if let Some(reference) = file.referenced_data_file.as_deref() {
                removed.delete_references.insert((path, reference));
            } else {
                removed.delete_paths.insert(path);
            }
        }
        removed
    }

    /// Whether `entry` is one of the removed files. Does not look at the entry's status.
    pub(crate) fn contains(&self, entry: &ManifestEntry) -> bool {
        let file = entry.data_file();
        let path = file.file_path();
        match file.content_type() {
            DataContentType::Data => self.data_paths.contains(path),
            DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                self.delete_paths.contains(path)
                    || file
                        .content_offset
                        .is_some_and(|offset| self.delete_offsets.contains(&(path, offset)))
                    || file
                        .referenced_data_file
                        .as_deref()
                        .is_some_and(|reference| {
                            self.delete_references.contains(&(path, reference))
                        })
            }
        }
    }

    /// Fails when a removed delete file identified only by path matched more than one of the
    /// live `entries`, for example a Puffin file holding several deletion vectors: removing all
    /// of them would drop deletes for data files the caller did not mean to touch.
    pub(crate) fn check_unambiguous(&self, entries: &[ManifestEntry]) -> Result<()> {
        let mut matches: HashMap<&str, usize> = HashMap::new();
        for entry in entries {
            let file = entry.data_file();
            if file.content_type() != DataContentType::Data
                && self.delete_paths.contains(file.file_path())
            {
                *matches.entry(file.file_path()).or_default() += 1;
            }
        }
        match matches.into_iter().find(|(_, count)| *count > 1) {
            Some((path, count)) => Err(invalid_data!(
                "Removing delete file {path} is ambiguous: it holds {count} live delete files; \
                 identify the one to remove by content offset or referenced data file"
            )),
            None => Ok(()),
        }
    }
}

fn fit_bounds_to_schema(file: &mut DataFile, schema: &Schema) {
    for bounds in [&mut file.lower_bounds, &mut file.upper_bounds] {
        *bounds = std::mem::take(bounds)
            .into_iter()
            .filter_map(|(field_id, bound)| {
                let field_type = schema
                    .field_by_id(field_id)?
                    .field_type
                    .as_primitive_type()?;
                promote_bound(bound, field_type).map(|bound| (field_id, bound))
            })
            .collect();
    }
}
fn promote_bound(bound: Datum, target: &PrimitiveType) -> Option<Datum> {
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    let literal = match (bound.data_type(), bound.literal(), target) {
        (source, _, target) if source == target => return Some(bound),
        (PrimitiveType::Int, PrimitiveLiteral::Int(_), PrimitiveType::Long) => {
            return bound.promote_to(target);
        }
        (PrimitiveType::Float, PrimitiveLiteral::Float(_), PrimitiveType::Double) => {
            return bound.promote_to(target);
        }
        // Dates promote to timestamps without a time zone only. A date outside the
        // timestamp's range cannot be represented, and the bound is dropped.
        (PrimitiveType::Date, PrimitiveLiteral::Int(days), PrimitiveType::Timestamp) => {
            PrimitiveLiteral::Long(i64::from(*days).checked_mul(MICROS_PER_DAY)?)
        }
        (PrimitiveType::Date, PrimitiveLiteral::Int(days), PrimitiveType::TimestampNs) => {
            PrimitiveLiteral::Long(i64::from(*days).checked_mul(MICROS_PER_DAY * 1_000)?)
        }
        (
            PrimitiveType::Decimal {
                precision: source_precision,
                scale: source_scale,
            },
            literal,
            PrimitiveType::Decimal { precision, scale },
        ) if source_scale == scale && source_precision <= precision => literal.clone(),
        _ => return None,
    };
    Some(Datum::new(target.clone(), literal))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_promote_bound() {
        let decimal = |precision| PrimitiveType::Decimal {
            precision,
            scale: 2,
        };
        // Kept, or promoted along Iceberg's type promotions.
        for (bound, target, expected) in [
            (Datum::int(7), PrimitiveType::Int, Datum::int(7)),
            (Datum::int(7), PrimitiveType::Long, Datum::long(7)),
            (
                Datum::float(1.5_f32),
                PrimitiveType::Double,
                Datum::double(1.5_f64),
            ),
            (
                Datum::date(1),
                PrimitiveType::Timestamp,
                Datum::timestamp_micros(86_400_000_000),
            ),
            (
                Datum::date(1),
                PrimitiveType::TimestampNs,
                Datum::timestamp_nanos(86_400_000_000_000),
            ),
            // 0001-01-01 is within the microsecond range.
            (
                Datum::date(-719_162),
                PrimitiveType::Timestamp,
                Datum::timestamp_micros(-719_162 * 86_400_000_000),
            ),
            (
                Datum::new(decimal(9), PrimitiveLiteral::Int128(1234)),
                decimal(18),
                Datum::new(decimal(18), PrimitiveLiteral::Int128(1234)),
            ),
        ] {
            assert_eq!(promote_bound(bound, &target), Some(expected));
        }
        // Narrowing, unrelated types, promotions the spec does not allow and values outside
        // the target's range are dropped.
        for (bound, target) in [
            (Datum::date(1), PrimitiveType::Timestamptz),
            (Datum::date(1), PrimitiveType::TimestamptzNs),
            // 0001-01-01 is before the earliest nanosecond timestamp.
            (Datum::date(-719_162), PrimitiveType::TimestampNs),
            (Datum::date(i32::MIN), PrimitiveType::TimestampNs),
            (Datum::date(i32::MAX), PrimitiveType::Timestamp),
            (Datum::long(7), PrimitiveType::Int),
            (Datum::double(1.5_f64), PrimitiveType::Float),
            (Datum::string("a"), PrimitiveType::Long),
            (
                Datum::new(decimal(18), PrimitiveLiteral::Int128(1234)),
                decimal(9),
            ),
        ] {
            assert_eq!(promote_bound(bound, &target), None);
        }
    }
}
