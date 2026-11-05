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
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{
    DataFile, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer, validate_delete_files,
};
use crate::transaction::validate::SnapshotValidator;
use crate::transaction::{ActionCommit, StatelessAction};
use crate::{Error, ErrorKind};

/// Row level changes packed into one snapshot.
///
/// Adds data files and delete files, and optionally marks delete files already in the table
/// as DELETED, for example when a deletion vector replaces the previous one for a data file.
#[derive(Clone)]
pub struct RowDeltaAction {
    check_duplicate: bool,
    commit_uuid: Option<Uuid>,
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

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(data_files);
        self
    }

    /// Add position or equality delete files to the snapshot.
    pub fn add_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_delete_files.extend(delete_files);
        self
    }

    /// Mark delete files already in the table as DELETED. Files are matched by path.
    pub fn remove_delete_files(mut self, delete_files: impl IntoIterator<Item = DataFile>) -> Self {
        self.removed_delete_files.extend(delete_files);
        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }
}

impl Default for RowDeltaAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StatelessAction for RowDeltaAction {
    async fn commit(&self, table: &Table) -> Result<ActionCommit> {
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

        validate_delete_files(&self.removed_delete_files, "removed")?;

        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            vec![],
            self.removed_delete_files.clone(),
        );

        snapshot_producer.validate_added_data_files()?;
        snapshot_producer.validate_added_delete_files()?;

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

    // A row delta changes rows in place and never replaces the whole table, even when it
    // records an overwrite.
    fn truncate_full_table(&self) -> bool {
        false
    }

    async fn delete_entries(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        if snapshot_producer.deleted_delete_files.is_empty() {
            return Ok(vec![]);
        }
        let table = snapshot_producer.table;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = table.manifest_list_reader(snapshot).load().await?;
        let removed = snapshot_producer.removed_files();

        let mut entries = Vec::with_capacity(snapshot_producer.deleted_delete_files.len());
        for manifest_file in manifest_list.entries() {
            if skips_unread(manifest_file, snapshot_producer) {
                continue;
            }
            let manifest = table.manifest_reader().read(manifest_file).await?;
            for entry in manifest.entries() {
                if entry.is_alive() && removed.contains(entry) {
                    entries.push(copy_with_deleted_status(entry.as_ref()));
                }
            }
        }
        removed.check_unambiguous(&entries)?;
        Ok(entries)
    }

    async fn existing_manifest(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let table = snapshot_producer.table;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = table.manifest_list_reader(snapshot).load().await?;

        if snapshot_producer.deleted_delete_files.is_empty() {
            return Ok(manifest_list
                .entries()
                .iter()
                .filter(|entry| entry.has_added_files() || entry.has_existing_files())
                .cloned()
                .collect());
        }

        let removed = snapshot_producer.removed_files();

        let mut existing = Vec::new();
        for manifest_file in manifest_list.entries() {
            if skips_unread(manifest_file, snapshot_producer) {
                if manifest_file.has_added_files() || manifest_file.has_existing_files() {
                    existing.push(manifest_file.clone());
                }
                continue;
            }
            let manifest = table.manifest_reader().read(manifest_file).await?;

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
            let mut writer = snapshot_producer.new_manifest_writer(
                manifest_file.content,
                manifest.metadata().schema().clone(),
                Arc::new(manifest.metadata().partition_spec().clone()),
            )?;
            for entry in manifest.entries() {
                if entry.status() == ManifestStatus::Deleted {
                    continue;
                }
                if removed.contains(entry) {
                    continue;
                }
                writer.add_existing_entry((**entry).clone())?;
            }
            existing.push(writer.write_manifest_file().await?);
        }
        Ok(existing)
    }
}

/// A data manifest holds no removed file when the operation removes only delete files, so it
/// carries forward without being read.
fn skips_unread(manifest_file: &ManifestFile, snapshot_producer: &SnapshotProducer<'_>) -> bool {
    manifest_file.content == ManifestContentType::Data
        && snapshot_producer.deleted_data_files.is_empty()
}

fn copy_with_deleted_status(entry: &ManifestEntry) -> ManifestEntry {
    ManifestEntry::builder()
        .status(ManifestStatus::Deleted)
        .snapshot_id_opt(entry.snapshot_id())
        .sequence_number_opt(entry.sequence_number())
        .file_sequence_number_opt(entry.file_sequence_number)
        .data_file(entry.data_file().clone())
        .build()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, Literal,
        MAIN_BRANCH, ManifestStatus, Operation, Struct,
    };
    use crate::table::Table;
    use crate::test_utils::commit_with_fresh_state;
    use crate::transaction::Transaction;
    use crate::transaction::fork_test_utils::{
        data_file, first_snapshot, pos_delete_v2, table_with_snapshot,
    };
    use crate::transaction::tests::make_v2_minimal_table;
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
        let result = commit_with_fresh_state(action, &table).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn add_data_only_emits_append() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let file = data_file("test/data.parquet", &table);
        let action = tx.row_delta().add_data_files(vec![file]);
        let mut commit = commit_with_fresh_state(action, &table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(extract_operation(&updates), Operation::Append);
    }

    #[tokio::test]
    async fn add_deletes_only_emits_delete() {
        let table = make_v2_minimal_table();
        let tx = Transaction::new(&table);
        let delete = pos_delete_v2("test/delete.parquet", &table);
        let action = tx.row_delta().add_delete_files(vec![delete]);
        let mut commit = commit_with_fresh_state(action, &table).await.unwrap();
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
        let mut commit = commit_with_fresh_state(action, &table).await.unwrap();
        let updates = commit.take_updates();
        assert_eq!(extract_operation(&updates), Operation::Overwrite);
    }

    #[tokio::test]
    async fn overwrite_keeps_table_totals() {
        // A row delta that records an overwrite must not reset the totals like a truncate.
        let base = make_v2_minimal_table();
        let mut first = commit_with_fresh_state(
            Transaction::new(&base)
                .row_delta()
                .add_data_files(vec![data_file("test/first.parquet", &base)]),
            &base,
        )
        .await
        .unwrap();
        let table = table_with_snapshot(&base, first_snapshot(first.take_updates()));

        let mut second = commit_with_fresh_state(
            Transaction::new(&table)
                .row_delta()
                .add_data_files(vec![data_file("test/second.parquet", &table)])
                .add_delete_files(vec![pos_delete_v2("test/delete.parquet", &table)]),
            &table,
        )
        .await
        .unwrap();
        let updates = second.take_updates();
        assert_eq!(extract_operation(&updates), Operation::Overwrite);
        let snapshot = first_snapshot(updates);
        let properties = &snapshot.summary().additional_properties;
        assert_eq!(properties.get("total-data-files").unwrap(), "2");
        assert_eq!(properties.get("total-records").unwrap(), "20");
        assert_eq!(properties.get("total-delete-files").unwrap(), "1");
        assert_eq!(properties.get("total-position-deletes").unwrap(), "5");
    }

    #[tokio::test]
    async fn duplicate_check_rejects_referenced_delete_file() {
        let base = make_v2_minimal_table();
        let delete = pos_delete_v2("test/delete.parquet", &base);

        let mut first_commit = commit_with_fresh_state(
            Transaction::new(&base)
                .row_delta()
                .add_delete_files(vec![delete.clone()]),
            &base,
        )
        .await
        .unwrap();
        let table = table_with_snapshot(&base, first_snapshot(first_commit.take_updates()));

        let result = commit_with_fresh_state(
            Transaction::new(&table)
                .row_delta()
                .with_check_duplicate(true)
                .add_delete_files(vec![delete]),
            &table,
        )
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
        let result = commit_with_fresh_state(action, &table).await;
        assert!(matches!(result, Err(Error { .. })));
    }

    #[tokio::test]
    async fn v3_rejects_parquet_position_deletes() {
        let table = make_v2_minimal_table();
        let mut metadata = (*table.metadata_ref()).clone();
        metadata.format_version = FormatVersion::V3;
        let table = table.with_metadata(Arc::new(metadata));

        let tx = Transaction::new(&table);
        let action = tx
            .row_delta()
            .add_delete_files(vec![pos_delete_v2("test/del.parquet", &table)]);
        let result = commit_with_fresh_state(action, &table).await;
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
        let result = commit_with_fresh_state(action, &table).await;
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

        let mut commit = commit_with_fresh_state(action, &table).await.unwrap();
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

        let mut commit = commit_with_fresh_state(action, &table).await.unwrap();
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
        let mut commit = commit_with_fresh_state(action, &table).await.unwrap();
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

    async fn entries_by_path(
        table: &Table,
        snapshot: &crate::spec::Snapshot,
    ) -> HashMap<String, (ManifestStatus, Option<i64>, Option<i64>)> {
        let manifest_list = table
            .manifest_list_reader(&Arc::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let mut entries = HashMap::new();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries() {
                entries.insert(
                    entry.data_file().file_path().to_string(),
                    (
                        entry.status(),
                        entry.sequence_number(),
                        entry.file_sequence_number,
                    ),
                );
            }
        }
        entries
    }

    #[tokio::test]
    async fn remove_delete_file_rewrites_manifest_and_preserves_sequence_numbers() {
        let base = make_v2_minimal_table();
        let prior = pos_delete_v2("test/prior.parquet", &base);
        let mut prior_shell = prior.clone();
        prior_shell.file_size_in_bytes = 0;
        prior_shell.record_count = 0;
        let survivor = pos_delete_v2("test/survivor.parquet", &base);

        let mut commit_s1 = commit_with_fresh_state(
            Transaction::new(&base)
                .row_delta()
                .add_delete_files(vec![prior.clone(), survivor.clone()]),
            &base,
        )
        .await
        .unwrap();
        let snap_s1 = first_snapshot(commit_s1.take_updates());
        let s1_seq = snap_s1.sequence_number();
        let table_s1 = table_with_snapshot(&base, snap_s1);

        let mut commit_s2 = commit_with_fresh_state(
            Transaction::new(&table_s1)
                .row_delta()
                .add_data_files(vec![data_file("test/new.parquet", &table_s1)])
                .remove_delete_files(vec![prior_shell]),
            &table_s1,
        )
        .await
        .unwrap();
        let updates_s2 = commit_s2.take_updates();
        assert_eq!(extract_operation(&updates_s2), Operation::Append);

        // The summary counts the removed file as recorded in the table, not the shell passed in.
        let snap_s2 = first_snapshot(updates_s2);
        let properties = &snap_s2.summary().additional_properties;
        assert_eq!(properties.get("removed-position-deletes").unwrap(), "5");
        assert_eq!(properties.get("total-position-deletes").unwrap(), "5");

        let entries = entries_by_path(&table_s1, &snap_s2).await;
        assert_eq!(
            entries[prior.file_path()],
            (ManifestStatus::Deleted, Some(s1_seq), Some(s1_seq))
        );
        assert_eq!(
            entries[survivor.file_path()],
            (ManifestStatus::Existing, Some(s1_seq), Some(s1_seq))
        );
        assert_eq!(entries["test/new.parquet"].0, ManifestStatus::Added);
    }

    fn deletion_vector(path: &str, offset: i64, data_file: &str, table: &Table) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(500)
            .record_count(3)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .referenced_data_file(Some(data_file.to_string()))
            .content_offset(Some(offset))
            .content_size_in_bytes(Some(40))
            .build()
            .unwrap()
    }

    /// A v3 table whose current snapshot holds the deletion vectors for `a.parquet` and
    /// `b.parquet` in one Puffin file, as Java's writer produces.
    async fn table_with_shared_puffin() -> (Table, DataFile, DataFile) {
        let table = make_v2_minimal_table();
        let mut metadata = (*table.metadata_ref()).clone();
        metadata.format_version = FormatVersion::V3;
        let base = table.with_metadata(Arc::new(metadata));
        let dv_a = deletion_vector("test/shared.puffin", 4, "test/a.parquet", &base);
        let dv_b = deletion_vector("test/shared.puffin", 44, "test/b.parquet", &base);
        let mut commit = commit_with_fresh_state(
            Transaction::new(&base)
                .row_delta()
                .add_delete_files(vec![dv_a.clone(), dv_b.clone()]),
            &base,
        )
        .await
        .unwrap();
        let table = table_with_snapshot(&base, first_snapshot(commit.take_updates()));
        (table, dv_a, dv_b)
    }

    #[tokio::test]
    async fn removes_one_deletion_vector_from_a_shared_puffin_file() {
        let (table, dv_a, dv_b) = table_with_shared_puffin().await;
        let replacement = deletion_vector("test/new.puffin", 4, "test/a.parquet", &table);

        // Identified by content offset, as a delete file read from a manifest is.
        let mut by_offset = dv_a.clone();
        by_offset.referenced_data_file = None;
        // Identified by referenced data file.
        let mut by_reference = dv_a.clone();
        by_reference.content_offset = None;

        for removal in [by_offset, by_reference] {
            let mut commit = commit_with_fresh_state(
                Transaction::new(&table)
                    .row_delta()
                    .add_delete_files(vec![replacement.clone()])
                    .remove_delete_files(vec![removal]),
                &table,
            )
            .await
            .unwrap();
            let snapshot = first_snapshot(commit.take_updates());

            let manifest_list = table
                .manifest_list_reader(&Arc::new(snapshot))
                .load()
                .await
                .unwrap();
            let mut statuses = Vec::new();
            for manifest_file in manifest_list.entries() {
                let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
                for entry in manifest.entries() {
                    statuses.push((
                        entry.file_path().to_string(),
                        entry.data_file().content_offset(),
                        entry.status(),
                    ));
                }
            }
            statuses.sort_by_key(|(path, offset, _)| (path.clone(), *offset));
            assert_eq!(statuses, vec![
                (
                    "test/new.puffin".to_string(),
                    Some(4),
                    ManifestStatus::Added
                ),
                (
                    "test/shared.puffin".to_string(),
                    Some(4),
                    ManifestStatus::Deleted
                ),
                (
                    "test/shared.puffin".to_string(),
                    Some(44),
                    ManifestStatus::Existing
                ),
            ]);
            assert_eq!(dv_b.content_offset(), Some(44));
        }
    }

    #[tokio::test]
    async fn rejects_ambiguous_path_only_removal() {
        let (table, dv_a, _) = table_with_shared_puffin().await;
        let mut path_only = dv_a;
        path_only.referenced_data_file = None;
        path_only.content_offset = None;

        let result = commit_with_fresh_state(
            Transaction::new(&table)
                .row_delta()
                .remove_delete_files(vec![path_only]),
            &table,
        )
        .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected an ambiguous removal to be rejected"),
        };
        assert!(error.to_string().contains("ambiguous"), "{error}");
    }

    #[tokio::test]
    async fn adds_delete_files_under_their_own_partition_spec() {
        use crate::spec::{
            TableMetadataBuilder, Transform, UnboundPartitionField, UnboundPartitionSpec,
        };

        // The default spec moves from identity(x) to identity(y), identity(z).
        let base = make_v2_minimal_table();
        let field = |source_id, name: &str| {
            UnboundPartitionField::builder()
                .source_ids(vec![source_id])
                .name(name)
                .transform(Transform::Identity)
                .build()
                .unwrap()
        };
        let metadata = TableMetadataBuilder::new_from_metadata(base.metadata().clone(), None)
            .add_partition_spec(
                UnboundPartitionSpec::builder()
                    .add_partition_fields([field(2, "y"), field(3, "z")])
                    .unwrap()
                    .build(),
            )
            .unwrap()
            .set_default_partition_spec(TableMetadataBuilder::LAST_ADDED)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = base.clone().with_metadata(Arc::new(metadata));
        let default_spec_id = table.metadata().default_partition_spec_id();
        assert_ne!(default_spec_id, 0);

        // Deletes for data written under spec 0 keep spec 0 and its partition value, next to
        // deletes for data written under the default spec.
        let old_spec_delete = pos_delete_v2("test/old-spec-delete.parquet", &base);
        let mut new_spec_delete = pos_delete_v2("test/new-spec-delete.parquet", &table);
        new_spec_delete.partition =
            Struct::from_iter([Some(Literal::long(1)), Some(Literal::long(2))]);
        let mut commit = commit_with_fresh_state(
            Transaction::new(&table)
                .row_delta()
                .add_delete_files(vec![old_spec_delete.clone(), new_spec_delete]),
            &table,
        )
        .await
        .unwrap();
        let snapshot = first_snapshot(commit.take_updates());
        let snapshot_clone = snapshot.clone();

        let manifest_list = table
            .manifest_list_reader(&Arc::new(snapshot))
            .load()
            .await
            .unwrap();
        let mut specs = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries() {
                assert_eq!(
                    entry.data_file().partition_spec_id,
                    manifest_file.partition_spec_id
                );
                specs.push((
                    entry.file_path().to_string(),
                    manifest_file.partition_spec_id,
                    entry.data_file().partition().clone(),
                ));
            }
        }
        specs.sort_by_key(|(path, _, _)| path.clone());
        assert_eq!(specs, vec![
            (
                "test/new-spec-delete.parquet".to_string(),
                default_spec_id,
                Struct::from_iter([Some(Literal::long(1)), Some(Literal::long(2))]),
            ),
            (
                "test/old-spec-delete.parquet".to_string(),
                0,
                old_spec_delete.partition().clone(),
            ),
        ]);

        // The old-spec delete applies to data of spec 0 in its partition.
        let snapshot = table_with_snapshot(&table, snapshot_clone)
            .metadata()
            .current_snapshot()
            .unwrap()
            .clone();
        let applicable = deletes_for(
            &table,
            &snapshot,
            &data_file("test/old-data.parquet", &base),
            0,
        )
        .await;
        assert_eq!(applicable, vec![old_spec_delete.file_path().to_string()]);

        let mut wrong_value = old_spec_delete;
        wrong_value.partition = Struct::from_iter([Some(Literal::string("not a long"))]);
        assert!(
            commit_with_fresh_state(
                Transaction::new(&table)
                    .row_delta()
                    .add_delete_files(vec![wrong_value]),
                &table
            )
            .await
            .is_err()
        );
    }

    /// An equality delete written after its partition source and equality column were both
    /// promoted from int to long keeps the data's int partition value, so it is written under
    /// the int schema. Its long bounds cannot be encoded there and are dropped, leaving the
    /// manifest readable and the delete applicable to the data written before the promotion.
    #[tokio::test]
    async fn equality_delete_written_after_a_type_promotion() {
        use arrow_array::{Int64Array, RecordBatch};
        use futures::SinkExt;
        use parquet::file::properties::WriterProperties;

        use crate::TableIdent;
        use crate::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
        use crate::delete_file_index::DeleteFileIndex;
        use crate::io::FileIO;
        use crate::scan::DeleteFileContext;
        use crate::spec::{
            Datum, ManifestContentType, NestedField, PartitionKey, PrimitiveType, Schema,
            SortOrder, TableMetadataBuilder, Transform, Type, UnboundPartitionField,
            UnboundPartitionSpec,
        };
        use crate::test_utils::test_runtime;
        use crate::writer::base_writer::equality_delete_writer::{
            EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
        };
        use crate::writer::file_writer::ParquetWriterBuilder;
        use crate::writer::file_writer::location_generator::{
            DefaultFileNameGenerator, DefaultLocationGenerator,
        };
        use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
        use crate::writer::{IcebergWriter, IcebergWriterBuilder};

        let schema = |schema_id, field_type: PrimitiveType| {
            Schema::builder()
                .with_schema_id(schema_id)
                .with_fields(vec![
                    NestedField::required(1, "x", Type::Primitive(field_type.clone())).into(),
                    NestedField::required(2, "y", Type::Primitive(field_type)).into(),
                ])
                .build()
                .unwrap()
        };
        let metadata = TableMetadataBuilder::new(
            schema(0, PrimitiveType::Int),
            UnboundPartitionSpec::builder()
                .add_partition_fields([UnboundPartitionField::builder()
                    .source_ids(vec![1])
                    .name("x")
                    .transform(Transform::Identity)
                    .build()
                    .unwrap()])
                .unwrap()
                .build(),
            SortOrder::unsorted_order(),
            "memory://warehouse/t".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let table = Table::builder()
            .metadata(metadata)
            .metadata_location("memory://warehouse/t/metadata/v1.json")
            .identifier(TableIdent::from_strs(["ns", "t"]).unwrap())
            .file_io(FileIO::new_with_memory())
            .runtime(test_runtime())
            .build()
            .unwrap();

        // Data written while x and y are ints.
        let data = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory://warehouse/t/data/old.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::int(100))]))
            .build()
            .unwrap();
        let mut commit = commit_with_fresh_state(
            Transaction::new(&table)
                .fast_append()
                .add_data_files([data]),
            &table,
        )
        .await
        .unwrap();
        let table = table_with_snapshot(&table, first_snapshot(commit.take_updates()));

        // x and y are promoted to long.
        let metadata = TableMetadataBuilder::new_from_metadata(table.metadata().clone(), None)
            .add_current_schema(schema(1, PrimitiveType::Long))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));
        let current_schema = table.metadata().current_schema().clone();

        // A real equality delete on y, written under the current schema with the old data's
        // partition key, which is bound to the schema that data was written under.
        let config = EqualityDeleteWriterConfig::new(vec![2], current_schema.clone()).unwrap();
        let delete_schema = arrow_schema_to_schema(config.projected_arrow_schema_ref()).unwrap();
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            ParquetWriterBuilder::new(WriterProperties::builder().build(), Arc::new(delete_schema)),
            table.file_io().clone(),
            DefaultLocationGenerator::with_data_location("memory://warehouse/t/data".to_string()),
            DefaultFileNameGenerator::new("eq".to_string(), None, DataFileFormat::Parquet),
        );
        let partition_key = PartitionKey::new(
            table
                .metadata()
                .partition_spec_by_id(0)
                .unwrap()
                .as_ref()
                .clone(),
            table.metadata().schema_by_id(0).unwrap().clone(),
            Struct::from_iter([Some(Literal::int(100))]),
        );
        let mut writer = EqualityDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(Some(partition_key))
            .await
            .unwrap();
        writer
            .write(
                RecordBatch::try_new(
                    Arc::new(schema_to_arrow_schema(&current_schema).unwrap()),
                    vec![
                        Arc::new(Int64Array::from(vec![100])),
                        Arc::new(Int64Array::from(vec![10])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let delete = writer.close().await.unwrap().pop().unwrap();
        assert_eq!(delete.lower_bounds().get(&2), Some(&Datum::long(10)));

        let mut commit = commit_with_fresh_state(
            Transaction::new(&table)
                .row_delta()
                .add_delete_files([delete]),
            &table,
        )
        .await
        .unwrap();
        let table = table_with_snapshot(&table, first_snapshot(commit.take_updates()));

        // Every manifest reads back.
        let snapshot = table.metadata().current_snapshot().unwrap();
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut data_entries = Vec::new();
        let mut delete_contexts = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries() {
                assert_eq!(
                    entry.data_file().partition(),
                    &Struct::from_iter([Some(Literal::int(100))])
                );
                match manifest_file.content {
                    ManifestContentType::Data => data_entries.push(entry.clone()),
                    ManifestContentType::Deletes => {
                        assert!(entry.data_file().lower_bounds().get(&2).is_none());
                        delete_contexts.push(DeleteFileContext {
                            manifest_entry: entry.clone(),
                            partition_spec_id: entry.data_file().partition_spec_id,
                        });
                    }
                }
            }
        }
        assert_eq!((data_entries.len(), delete_contexts.len()), (1, 1));

        // The delete applies to the data written before the promotion.
        let (index, mut sender) = DeleteFileIndex::new(table.runtime().clone());
        for context in delete_contexts {
            sender.send(context).await.unwrap();
        }
        drop(sender);
        let deletes = index
            .get_deletes_for_data_file(
                data_entries[0].data_file(),
                data_entries[0].sequence_number(),
            )
            .await
            .unwrap();
        assert_eq!(deletes.len(), 1);
    }

    #[tokio::test]
    async fn position_deletes_keep_their_file_path_and_pos_bounds() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use parquet::file::properties::WriterProperties;

        use crate::metadata_columns::{
            RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS,
        };
        use crate::spec::{Datum, ManifestContentType, PartitionKey};
        use crate::writer::base_writer::position_delete_writer::{
            PositionDeleteFileWriterBuilder, position_delete_arrow_schema, position_delete_schema,
        };
        use crate::writer::file_writer::ParquetWriterBuilder;
        use crate::writer::file_writer::location_generator::{
            DefaultFileNameGenerator, DefaultLocationGenerator,
        };
        use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
        use crate::writer::{IcebergWriter, IcebergWriterBuilder};

        // One commit adds two data files of one partition and a V2 position delete of rows 2
        // and 5 of the first, as equality mode commits its local deletes.
        let table = make_v2_minimal_table();
        let deleted_from = data_file("memory://test/data/a.parquet", &table);
        let sibling = data_file("memory://test/data/b.parquet", &table);
        let mut writer = PositionDeleteFileWriterBuilder::new(
            RollingFileWriterBuilder::new_with_default_file_size(
                ParquetWriterBuilder::new(
                    WriterProperties::builder().build(),
                    position_delete_schema(),
                ),
                table.file_io().clone(),
                DefaultLocationGenerator::with_data_location("memory://test/data".to_string()),
                DefaultFileNameGenerator::new("pos".to_string(), None, DataFileFormat::Parquet),
            ),
        )
        .build(Some(PartitionKey::new(
            table.metadata().default_partition_spec().as_ref().clone(),
            table.metadata().current_schema().clone(),
            deleted_from.partition().clone(),
        )))
        .await
        .unwrap();
        writer
            .write(
                RecordBatch::try_new(position_delete_arrow_schema(), vec![
                    Arc::new(StringArray::from(vec![
                        deleted_from.file_path(),
                        deleted_from.file_path(),
                    ])),
                    Arc::new(Int64Array::from(vec![2, 5])),
                ])
                .unwrap(),
            )
            .await
            .unwrap();
        let delete = writer.close().await.unwrap().pop().unwrap();
        let mut commit = commit_with_fresh_state(
            Transaction::new(&table)
                .row_delta()
                .add_data_files([deleted_from.clone(), sibling.clone()])
                .add_delete_files([delete]),
            &table,
        )
        .await
        .unwrap();
        let table = table_with_snapshot(&table, first_snapshot(commit.take_updates()));
        let snapshot = table.metadata().current_snapshot().unwrap();

        // The committed manifest keeps the bounds the writer produced.
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        let mut committed = Vec::new();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content == ManifestContentType::Deletes {
                let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
                committed.extend(manifest.entries().iter().cloned());
            }
        }
        assert_eq!(committed.len(), 1);
        let committed = committed[0].data_file();
        let bounds = |field_id| {
            (
                committed.lower_bounds().get(&field_id),
                committed.upper_bounds().get(&field_id),
            )
        };
        let path = Datum::string(deleted_from.file_path());
        assert_eq!(
            bounds(RESERVED_FIELD_ID_DELETE_FILE_PATH),
            (Some(&path), Some(&path))
        );
        assert_eq!(
            bounds(RESERVED_FIELD_ID_DELETE_FILE_POS),
            (Some(&Datum::long(2)), Some(&Datum::long(5)))
        );

        // The bounds scope the delete to its data file and leave the sibling clear.
        let sequence = snapshot.sequence_number();
        assert_eq!(
            deletes_for(&table, snapshot, &deleted_from, sequence).await,
            vec![committed.file_path().to_string()]
        );
        assert!(
            deletes_for(&table, snapshot, &sibling, sequence)
                .await
                .is_empty()
        );
    }

    /// Paths of the delete files in `snapshot` that apply to `data_file` with data sequence
    /// number `sequence_number`, as a scan finds them.
    async fn deletes_for(
        table: &Table,
        snapshot: &crate::spec::SnapshotRef,
        data_file: &DataFile,
        sequence_number: i64,
    ) -> Vec<String> {
        use futures::SinkExt;

        use crate::delete_file_index::DeleteFileIndex;
        use crate::scan::DeleteFileContext;
        use crate::spec::ManifestContentType;

        let (index, mut sender) = DeleteFileIndex::new(table.runtime().clone());
        let manifest_list = table.manifest_list_reader(snapshot).load().await.unwrap();
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Deletes {
                continue;
            }
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                sender
                    .send(DeleteFileContext {
                        manifest_entry: entry.clone(),
                        partition_spec_id: entry.data_file().partition_spec_id,
                    })
                    .await
                    .unwrap();
            }
        }
        drop(sender);
        let mut paths: Vec<String> = index
            .get_deletes_for_data_file(data_file, Some(sequence_number))
            .await
            .unwrap()
            .iter()
            .map(|delete| delete.file_path().to_string())
            .collect();
        paths.sort();
        paths
    }

    #[tokio::test]
    async fn rewrites_manifests_of_an_encrypted_table_encrypted() {
        let base = crate::test_utils::make_encrypted_table().await;
        assert!(base.encryption_manager().is_some());
        let dv = |path: &str, data: &str| {
            DataFileBuilder::default()
                .content(DataContentType::PositionDeletes)
                .file_path(path.to_string())
                .file_format(DataFileFormat::Puffin)
                .file_size_in_bytes(50)
                .record_count(1)
                .partition_spec_id(base.metadata().default_partition_spec_id())
                .partition(Struct::empty())
                .referenced_data_file(Some(data.to_string()))
                .content_offset(Some(4))
                .content_size_in_bytes(Some(40))
                .build()
                .unwrap()
        };
        let dv_a = dv(
            "memory:///table/dv-a.puffin",
            "memory:///table/data/a.parquet",
        );
        let dv_b = dv(
            "memory:///table/dv-b.puffin",
            "memory:///table/data/b.parquet",
        );
        let replacement = dv(
            "memory:///table/dv-a2.puffin",
            "memory:///table/data/a.parquet",
        );

        let mut commit = commit_with_fresh_state(
            Transaction::new(&base)
                .row_delta()
                .add_delete_files(vec![dv_a.clone(), dv_b.clone()]),
            &base,
        )
        .await
        .unwrap();
        let table = table_with_snapshot(&base, first_snapshot(commit.take_updates()));

        // Replacing a's vector rewrites the manifest holding both into one keeping b.
        let mut commit = commit_with_fresh_state(
            Transaction::new(&table)
                .row_delta()
                .add_delete_files(vec![replacement.clone()])
                .remove_delete_files(vec![dv_a.clone()]),
            &table,
        )
        .await
        .unwrap();
        let snapshot = Arc::new(first_snapshot(commit.take_updates()));

        let manifest_list = table.manifest_list_reader(&snapshot).load().await.unwrap();
        let mut statuses = Vec::new();
        for manifest_file in manifest_list.entries() {
            assert!(
                manifest_file.key_metadata.is_some(),
                "manifest {} is not encrypted",
                manifest_file.manifest_path
            );
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries() {
                statuses.push((entry.file_path().to_string(), entry.status()));
            }
        }
        statuses.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(statuses, vec![
            (dv_a.file_path().to_string(), ManifestStatus::Deleted),
            (replacement.file_path().to_string(), ManifestStatus::Added),
            (dv_b.file_path().to_string(), ManifestStatus::Existing),
        ]);
    }
}
