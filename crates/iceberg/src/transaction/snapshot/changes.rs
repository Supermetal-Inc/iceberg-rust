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

use std::collections::HashMap;
use std::future::Future;

use uuid::Uuid;

use crate::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestEntry, ManifestFile,
    ManifestWriter, Operation,
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
        spec_id: i32,
    ) -> Result<ManifestWriter> {
        let path = super::manifest_path(
            self.table,
            self.state.commit_uuid,
            self.state.manifest_counter,
        )?;
        self.state.manifest_counter += 1;
        let spec = self
            .table
            .metadata()
            .partition_spec_by_id(spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Partition spec with id: {spec_id} is not found!"),
                )
            })?;
        super::new_manifest_writer(
            self.table,
            path,
            self.snapshot_id,
            content,
            self.table.metadata().current_schema().clone(),
            spec.clone(),
        )
    }
    // Write manifest file for added data files and return the ManifestFile for ManifestList.
    async fn write_added_manifest(
        &mut self,
        content_type: ManifestContentType,
    ) -> Result<ManifestFile> {
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
        let manifest_entries = added_data_files.into_iter().map(|data_file| {
            let builder = ManifestEntry::builder()
                .status(crate::spec::ManifestStatus::Added)
                .data_file(data_file);
            if format_version == FormatVersion::V1 {
                builder.snapshot_id(snapshot_id).build()
            } else {
                // For format version > 1, we set the snapshot id at the inherited time to avoid rewrite the manifest file when
                // commit failed.
                builder.build()
            }
        });
        let mut writer = self.new_manifest_writer(
            content_type,
            self.table.metadata().default_partition_spec_id(),
        )?;
        for entry in manifest_entries {
            writer.add_entry(entry)?;
        }
        writer.write_manifest_file().await
    }

    async fn write_deleted_manifest(
        &mut self,
        deleted_entries: Vec<ManifestEntry>,
    ) -> Result<Vec<ManifestFile>> {
        if deleted_entries.is_empty() {
            Ok(Vec::new())
        } else {
            let mut partition_groups = HashMap::new();
            for entry in deleted_entries {
                partition_groups
                    .entry(entry.data_file().partition_spec_id)
                    .or_insert_with(Vec::new)
                    .push(entry);
            }

            let mut deleted_manifests = Vec::new();
            for (spec_id, entries) in partition_groups {
                let mut data_manifest_writer: Option<ManifestWriter> = None;
                let mut delete_manifest_writer: Option<ManifestWriter> = None;
                for entry in entries {
                    let (writer, content) = match entry.data_file().content_type() {
                        DataContentType::Data => {
                            (&mut data_manifest_writer, ManifestContentType::Data)
                        }
                        DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                            (&mut delete_manifest_writer, ManifestContentType::Deletes)
                        }
                    };
                    if writer.is_none() {
                        *writer = Some(self.new_manifest_writer(content, spec_id)?);
                    }
                    writer.as_mut().unwrap().add_delete_entry(entry)?;
                }

                if let Some(writer) = data_manifest_writer {
                    deleted_manifests.push(writer.write_manifest_file().await?);
                };
                if let Some(writer) = delete_manifest_writer {
                    deleted_manifests.push(writer.write_manifest_file().await?);
                };
            }

            Ok(deleted_manifests)
        }
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
            let added_manifest = self.write_added_manifest(ManifestContentType::Data).await?;
            manifest_files.push(added_manifest);
        }
        if !self.added_delete_files.is_empty() {
            let added_manifest = self
                .write_added_manifest(ManifestContentType::Deletes)
                .await?;
            manifest_files.push(added_manifest);
        }

        let delete_manifests = self.write_deleted_manifest(deleted_entries).await?;
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
