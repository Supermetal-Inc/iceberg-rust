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
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, DataFileFormat, FormatVersion, ManifestEntry, ManifestFile,
    ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::validate::SnapshotValidator;
use crate::transaction::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind};

/// Row level changes packed into one snapshot.
pub struct RowDeltaAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
}

impl RowDeltaAction {
    pub(crate) fn new() -> Self {
        Self {
            check_duplicate: false,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            added_delete_files: vec![],
            removed_delete_files: vec![],
        }
    }

    /// Set whether to reject files already referenced by the table.
    /// The check reads the current snapshot's manifests and is disabled by default.
    pub fn with_check_duplicate(mut self, value: bool) -> Self {
        self.check_duplicate = value;
        self
    }

    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    pub fn add_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    pub fn remove_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_delete_files.extend(delete_files);
        self
    }

    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    fn validate_delete_files(files: &[DataFile], label: &str) -> Result<()> {
        for file in files {
            match file.content_type() {
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {}
                DataContentType::Data => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "{label} file {} has content type Data; use add_data_files",
                            file.file_path()
                        ),
                    ));
                }
            }
            if file.content_type() == DataContentType::EqualityDeletes
                && file.equality_ids().is_none_or(|ids| ids.is_empty())
            {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("equality delete {} missing equality_ids", file.file_path()),
                ));
            }
        }
        Ok(())
    }

    fn validate_format_version(files: &[DataFile], format: FormatVersion) -> Result<()> {
        for file in files {
            if format == FormatVersion::V1 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "delete files are not supported in V1",
                ));
            }
            if file.content_type() != DataContentType::PositionDeletes {
                continue;
            }
            let is_dv = file.file_format() == DataFileFormat::Puffin;
            match (format, is_dv) {
                (FormatVersion::V2, true) => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "V2 forbids deletion vectors for position deletes: {}",
                            file.file_path()
                        ),
                    ));
                }
                (FormatVersion::V3, false) => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "V3 requires deletion vectors for position deletes: {}",
                            file.file_path()
                        ),
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl Default for RowDeltaAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for RowDeltaAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        if self.added_data_files.is_empty()
            && self.added_delete_files.is_empty()
            && self.removed_delete_files.is_empty()
            && self.snapshot_properties.is_empty()
        {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "row delta requires at least one added or removed file",
            ));
        }

        Self::validate_delete_files(&self.added_delete_files, "added")?;
        Self::validate_delete_files(&self.removed_delete_files, "removed")?;
        Self::validate_format_version(&self.added_delete_files, table.metadata().format_version())?;

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            vec![],
            self.removed_delete_files.clone(),
        );

        snapshot_producer.validate_added_data_files(&self.added_data_files)?;
        snapshot_producer.validate_added_data_files(&self.added_delete_files)?;

        if self.check_duplicate {
            snapshot_producer.validate_duplicate_files().await?;
        }

        let operation = RowDeltaOperation {
            has_added_data: !self.added_data_files.is_empty(),
            has_added_deletes: !self.added_delete_files.is_empty(),
        };

        snapshot_producer
            .commit(operation, DefaultManifestProcess)
            .await
    }
}

struct RowDeltaOperation {
    has_added_data: bool,
    has_added_deletes: bool,
}

impl SnapshotValidator for RowDeltaOperation {}

impl SnapshotProduceOperation for RowDeltaOperation {
    // Removing delete files alone does not change the operation because
    // the live row set is unaffected.
    fn operation(&self) -> Operation {
        if self.has_added_data && !self.has_added_deletes {
            Operation::Append
        } else if self.has_added_deletes && !self.has_added_data {
            Operation::Delete
        } else {
            Operation::Overwrite
        }
    }

    async fn delete_entries(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        if snapshot_producer.deleted_delete_files.is_empty() {
            return Ok(vec![]);
        }
        let Some(snapshot) = snapshot_producer.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_producer.table.file_io(),
                snapshot_producer.table.metadata(),
            )
            .await?;

        let removed_paths: HashSet<&str> = snapshot_producer
            .deleted_delete_files
            .iter()
            .map(|f| f.file_path())
            .collect();

        let mut entries = Vec::with_capacity(snapshot_producer.deleted_delete_files.len());
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;
            for entry in manifest.entries() {
                if !entry.is_alive() {
                    continue;
                }
                if !matches!(
                    entry.content_type(),
                    DataContentType::PositionDeletes | DataContentType::EqualityDeletes
                ) {
                    continue;
                }
                if removed_paths.contains(entry.data_file().file_path()) {
                    entries.push(copy_with_deleted_status(entry.as_ref())?);
                }
            }
        }
        Ok(entries)
    }

    async fn existing_manifest(
        &self,
        snapshot_producer: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_producer.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_producer.table.file_io(),
                snapshot_producer.table.metadata(),
            )
            .await?;

        if snapshot_producer.deleted_delete_files.is_empty() {
            return Ok(manifest_list
                .entries()
                .iter()
                .filter(|entry| entry.has_added_files() || entry.has_existing_files())
                .cloned()
                .collect());
        }

        let removed_paths: HashSet<String> = snapshot_producer
            .deleted_delete_files
            .iter()
            .map(|f| f.file_path().to_string())
            .collect();

        let mut existing = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(snapshot_producer.table.file_io())
                .await?;

            let mut has_removal = false;
            let mut has_survivor = false;
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }
                if removed_paths.contains(entry.data_file().file_path()) {
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
            let mut writer = snapshot_producer
                .new_manifest_writer(manifest_file.content, manifest_file.partition_spec_id)?;
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }
                if removed_paths.contains(entry.data_file().file_path()) {
                    continue;
                }
                writer.add_existing_entry((**entry).clone())?;
            }
            existing.push(writer.write_manifest_file().await?);
        }
        Ok(existing)
    }
}

fn copy_with_deleted_status(entry: &ManifestEntry) -> Result<ManifestEntry> {
    Ok(ManifestEntry::builder()
        .status(ManifestStatus::Deleted)
        .snapshot_id_opt(entry.snapshot_id())
        .sequence_number_opt(entry.sequence_number())
        .file_sequence_number_opt(entry.file_sequence_number)
        .data_file(entry.data_file().clone())
        .build())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Literal, MAIN_BRANCH, ManifestStatus,
        Operation, Struct,
    };
    use crate::transaction::tests::{
        data_file, first_snapshot, make_v2_minimal_table, pos_delete_v2, table_with_snapshot,
    };
    use crate::transaction::{Transaction, TransactionAction};
    use crate::{Error, TableUpdate};

    fn extract_operation(updates: &[TableUpdate]) -> Operation {
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            snapshot.summary().operation.clone()
        } else {
            panic!("expected AddSnapshot");
        }
    }

    #[tokio::test]
    async fn empty_row_delta_errors() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx.row_delta();
        let result = Arc::new(action).commit(&table).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn add_data_only_emits_append() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let file = data_file("test/data.parquet", &table);
        let action = tx.row_delta().add_data_files(vec![file]);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(extract_operation(&updates), Operation::Append);
    }

    #[tokio::test]
    async fn add_deletes_only_emits_delete() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let delete = pos_delete_v2("test/delete.parquet", &table);
        let action = tx.row_delta().add_delete_files(vec![delete]);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(extract_operation(&updates), Operation::Delete);
    }

    #[tokio::test]
    async fn add_data_and_deletes_emits_overwrite() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_data_files(vec![data_file("test/data.parquet", &table)])
            .add_delete_files(vec![pos_delete_v2("test/delete.parquet", &table)]);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(extract_operation(&updates), Operation::Overwrite);
    }

    #[tokio::test]
    async fn duplicate_check_rejects_referenced_delete_file() {
        let base = make_v2_minimal_table();
        let delete = pos_delete_v2("test/delete.parquet", &base);

        let mut first_commit = Arc::new(
            Transaction::new(&base)
                .row_delta()
                .add_delete_files(vec![delete.clone()]),
        )
        .commit(&base)
        .await
        .unwrap();
        let table = table_with_snapshot(&base, first_snapshot(first_commit.take_updates()));

        let result = Arc::new(
            Transaction::new(&table)
                .row_delta()
                .with_check_duplicate(true)
                .add_delete_files(vec![delete]),
        )
        .commit(&table)
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected duplicate delete file to be rejected"),
        };
        assert!(error.to_string().contains("test/delete.parquet"));
    }

    #[tokio::test]
    async fn rejects_data_file_in_delete_bucket() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_delete_files(vec![data_file("test/wrong.parquet", &table)]);
        let result = Arc::new(action).commit(&table).await;
        assert!(matches!(result, Err(Error { .. })));
    }

    #[tokio::test]
    async fn v3_rejects_parquet_position_deletes() {
        let table = make_v2_minimal_table();
        let mut metadata = (*table.metadata_ref()).clone();
        metadata.format_version = crate::spec::FormatVersion::V3;
        let table = table.with_metadata(Arc::new(metadata));

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_delete_files(vec![pos_delete_v2("test/del.parquet", &table)]);
        let result = Arc::new(action).commit(&table).await;
        match result {
            Err(e) => assert!(e.to_string().contains("V3 requires deletion vectors")),
            Ok(_) => panic!("expected V3 to reject parquet position deletes"),
        }
    }

    #[tokio::test]
    async fn v2_rejects_puffin_position_deletes() {
        let table = make_v2_minimal_table();
        let dv = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/dv.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .referenced_data_file(Some("test/data.parquet".to_string()))
            .content_offset(Some(0))
            .content_size_in_bytes(Some(50))
            .build()
            .unwrap();

        let tx = Transaction::new(&table);
        let action = tx.row_delta().add_delete_files(vec![dv]);
        let result = Arc::new(action).commit(&table).await;
        match result {
            Err(e) => assert!(e.to_string().contains("V2 forbids deletion vectors")),
            Ok(_) => panic!("expected V2 to reject puffin position deletes"),
        }
    }

    #[tokio::test]
    async fn add_data_and_remove_deletes_emits_append() {
        let table = make_v2_minimal_table();
        let action = Transaction::new(&table)
            .row_delta()
            .add_data_files(vec![data_file("test/new.parquet", &table)])
            .remove_delete_files(vec![pos_delete_v2("test/prior.parquet", &table)]);

        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(extract_operation(&updates), Operation::Append);
    }

    #[tokio::test]
    async fn dv_merge_shape_emits_overwrite() {
        let table = make_v2_minimal_table();
        let action = Transaction::new(&table)
            .row_delta()
            .add_data_files(vec![data_file("test/data.parquet", &table)])
            .add_delete_files(vec![pos_delete_v2("test/new_del.parquet", &table)])
            .remove_delete_files(vec![pos_delete_v2("test/prior_del.parquet", &table)]);

        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(extract_operation(&updates), Operation::Overwrite);
    }

    #[tokio::test]
    async fn passes_snapshot_properties_through() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let mut props = HashMap::new();
        props.insert("sm.connector_id".to_string(), "abc".to_string());
        let action = tx
            .row_delta()
            .set_snapshot_properties(props)
            .add_data_files(vec![data_file("test/data.parquet", &table)]);
        let mut commit = Arc::new(action).commit(&table).await.unwrap();
        let updates = commit.take_updates();
        if let TableUpdate::AddSnapshot { snapshot } = &updates[0] {
            assert_eq!(
                snapshot
                    .summary()
                    .additional_properties
                    .get("sm.connector_id")
                    .unwrap(),
                "abc"
            );
            assert_eq!(snapshot.summary().operation, Operation::Append);
            assert_eq!(updates[1], TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: crate::spec::SnapshotReference::new(
                    snapshot.snapshot_id(),
                    crate::spec::SnapshotRetention::branch(None, None, None),
                ),
            });
        } else {
            panic!("expected AddSnapshot");
        }
    }

    #[tokio::test]
    async fn remove_delete_file_rewrites_manifest_and_preserves_sequence_numbers() {
        let base = make_v2_minimal_table();
        let prior = pos_delete_v2("test/prior.parquet", &base);
        let mut prior_shell = prior.clone();
        prior_shell.file_size_in_bytes = 0;
        prior_shell.record_count = 0;
        let survivor = pos_delete_v2("test/survivor.parquet", &base);

        let mut commit_s1 = Arc::new(
            Transaction::new(&base)
                .fast_append()
                .add_data_files(vec![prior.clone(), survivor.clone()]),
        )
        .commit(&base)
        .await
        .unwrap();
        let snap_s1 = first_snapshot(commit_s1.take_updates());
        let s1_seq = snap_s1.sequence_number();
        let table_s1 = table_with_snapshot(&base, snap_s1);

        let mut commit_s2 = Arc::new(
            Transaction::new(&table_s1)
                .row_delta()
                .add_data_files(vec![data_file("test/new.parquet", &table_s1)])
                .remove_delete_files(vec![prior_shell]),
        )
        .commit(&table_s1)
        .await
        .unwrap();
        let updates_s2 = commit_s2.take_updates();
        assert_eq!(extract_operation(&updates_s2), Operation::Append);

        let snap_s2 = first_snapshot(updates_s2);
        assert_eq!(
            snap_s2
                .summary()
                .additional_properties
                .get("removed-position-deletes")
                .unwrap(),
            "5"
        );
        assert_eq!(
            snap_s2
                .summary()
                .additional_properties
                .get("total-position-deletes")
                .unwrap(),
            "5"
        );
        let manifests = snap_s2
            .load_manifest_list(table_s1.file_io(), table_s1.metadata())
            .await
            .unwrap();

        let mut saw_prior_deleted = false;
        let mut saw_survivor_existing = false;
        let mut saw_new_added = false;

        for mf in manifests.entries() {
            let manifest = mf.load_manifest(table_s1.file_io()).await.unwrap();
            for entry in manifest.entries() {
                let path = entry.data_file().file_path();
                let status = entry.status();
                if path == prior.file_path() {
                    assert_eq!(status, ManifestStatus::Deleted);
                    assert_eq!(entry.sequence_number(), Some(s1_seq));
                    assert_eq!(entry.file_sequence_number, Some(s1_seq));
                    saw_prior_deleted = true;
                } else if path == survivor.file_path() {
                    assert_eq!(status, ManifestStatus::Existing);
                    assert_eq!(entry.sequence_number(), Some(s1_seq));
                    assert_eq!(entry.file_sequence_number, Some(s1_seq));
                    saw_survivor_existing = true;
                } else if path == "test/new.parquet" {
                    assert_eq!(status, ManifestStatus::Added);
                    saw_new_added = true;
                }
            }
        }

        assert!(saw_prior_deleted, "prior delete file missing DELETED entry");
        assert!(saw_survivor_existing, "survivor missing EXISTING entry");
        assert!(saw_new_added, "new data file missing ADDED entry");
    }
}
