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
use std::marker::PhantomData;

use async_trait::async_trait;
use uuid::Uuid;

use super::snapshot::changes::{ChangeSnapshotState, SnapshotProduceOperation, SnapshotProducer};
use super::{ActionCommit, TransactionAction};
use crate::error::{Error, ErrorKind, Result};
use crate::spec::{DataContentType, DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::validate::SnapshotValidator;

/// Which snapshot operation a file replacement records.
///
/// Rewrites and overwrites share the same manifest handling but have different
/// logical meanings in the Iceberg snapshot summary.
pub(crate) trait ReplaceFilesMode: Clone + Send + Sync + 'static {
    const OPERATION: Operation;
}

/// Files were replaced without changing table data.
#[derive(Clone)]
pub struct Rewrite;

/// Files were replaced as a logical overwrite.
#[derive(Clone)]
pub struct Overwrite;

impl ReplaceFilesMode for Rewrite {
    const OPERATION: Operation = Operation::Replace;
}

impl ReplaceFilesMode for Overwrite {
    const OPERATION: Operation = Operation::Overwrite;
}

/// Transaction action for replacing files.
#[allow(private_bounds)]
#[derive(Clone)]
pub struct ReplaceFilesAction<M: ReplaceFilesMode> {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    deleted_delete_files: Vec<DataFile>,
    data_sequence_number: Option<i64>,
    starting_snapshot_id: Option<i64>,
    full_table_overwrite: bool,
    _mode: PhantomData<M>,
}

/// Rewrites files without changing table data.
pub type RewriteFilesAction = ReplaceFilesAction<Rewrite>;

/// Replaces files as a logical overwrite.
pub type OverwriteFilesAction = ReplaceFilesAction<Overwrite>;

struct ReplaceFilesOperation<M: ReplaceFilesMode> {
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    deleted_delete_files: Vec<DataFile>,
    starting_snapshot_id: Option<i64>,
    data_sequence_number: Option<i64>,
    full_table_overwrite: bool,
    _mode: PhantomData<M>,
}

#[allow(private_bounds)]
impl<M: ReplaceFilesMode> ReplaceFilesAction<M> {
    pub fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: Default::default(),
            added_data_files: vec![],
            added_delete_files: vec![],
            deleted_data_files: vec![],
            deleted_delete_files: vec![],
            data_sequence_number: None,
            starting_snapshot_id: None,
            full_table_overwrite: false,
            _mode: PhantomData,
        }
    }

    /// Add added data files to the snapshot.
    pub fn add_data_files(
        mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<Self> {
        for data_file in data_files {
            match data_file.content {
                DataContentType::Data => self.added_data_files.push(data_file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(data_file)
                }
            }
        }
        Ok(self)
    }

    /// Add deleted data files to the snapshot.
    pub fn delete_data_files(
        mut self,
        data_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<Self> {
        for data_file in data_files {
            match data_file.content {
                DataContentType::Data => self.deleted_data_files.push(data_file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.deleted_delete_files.push(data_file)
                }
            }
        }

        Ok(self)
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Set the data sequence number for this rewrite operation.
    /// The number will be used for all new data files that are added in this rewrite.
    pub fn set_data_sequence_number(mut self, sequence_number: i64) -> Self {
        self.data_sequence_number = Some(sequence_number);
        self
    }

    /// Set the snapshot ID used in any reads for this operation.
    pub fn set_starting_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
        self
    }
}

impl ReplaceFilesAction<Overwrite> {
    /// Mark this overwrite as replacing the complete logical table contents.
    pub fn full_table_overwrite(mut self) -> Self {
        self.full_table_overwrite = true;
        self
    }
}

impl<M: ReplaceFilesMode> Default for ReplaceFilesAction<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<M: ReplaceFilesMode> TransactionAction for ReplaceFilesAction<M> {
    type State = ChangeSnapshotState;

    fn new_state(&self) -> Self::State {
        ChangeSnapshotState::new(self.commit_uuid)
    }

    async fn cleanup(self, _state: Self::State, _table: &Table, _status: super::CommitStatus) {}

    async fn commit(&self, state: &mut Self::State, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            state,
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.deleted_data_files.clone(),
            self.deleted_delete_files.clone(),
        );

        snapshot_producer.validate_added_data_files(&self.added_data_files)?;
        snapshot_producer.validate_added_delete_files()?;

        let replace_operation = ReplaceFilesOperation::<M> {
            added_data_files: self.added_data_files.clone(),
            added_delete_files: self.added_delete_files.clone(),
            deleted_data_files: self.deleted_data_files.clone(),
            deleted_delete_files: self.deleted_delete_files.clone(),
            starting_snapshot_id: self.starting_snapshot_id,
            data_sequence_number: self.data_sequence_number,
            full_table_overwrite: self.full_table_overwrite,
            _mode: PhantomData,
        };

        // todo should be able to configure to use the merge manifest process
        snapshot_producer.commit(replace_operation).await
    }
}

impl<M: ReplaceFilesMode> SnapshotValidator for ReplaceFilesOperation<M> {
    async fn validate(&self, base: &Table, parent_snapshot_id: Option<i64>) -> Result<()> {
        // Validate replaced and added files
        if self.deleted_data_files.is_empty() && self.deleted_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Files to delete cannot be empty",
            ));
        }
        if self.deleted_data_files.is_empty() && !self.added_data_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Data files to add must be empty because there's no data file to be rewritten",
            ));
        }
        if self.deleted_delete_files.is_empty() && !self.added_delete_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Delete files to add must be empty because there's no delete file to be rewritten",
            ));
        }

        // todo add use_starting_seq_number to determine if we want to use data_sequence_number
        // If there are replaced data files, there cannot be any new row-level deletes for those data files
        if !self.deleted_data_files.is_empty() {
            super::validate::validate_no_new_deletes_for_data_files(
                base,
                self.starting_snapshot_id,
                parent_snapshot_id,
                &self.deleted_data_files,
                self.data_sequence_number.is_some(),
            )
            .await?;
        }

        Ok(())
    }
}

impl<M: ReplaceFilesMode> SnapshotProduceOperation for ReplaceFilesOperation<M> {
    fn operation(&self) -> Operation {
        M::OPERATION.clone()
    }

    fn is_full_table_overwrite(&self) -> bool {
        self.full_table_overwrite
    }

    async fn delete_entries(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        snapshot_producer
            .delete_entries(self.full_table_overwrite)
            .await
    }

    async fn existing_manifest(
        &self,
        snapshot_producer: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        snapshot_producer.existing_manifests().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use super::{Overwrite, ReplaceFilesAction, ReplaceFilesMode, Rewrite};
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestStatus,
        Operation, Snapshot, Struct,
    };
    use crate::table::Table;
    use crate::transaction::tests::{
        commit_action, data_file, first_snapshot, make_v2_minimal_table, make_v3_minimal_table,
        pos_delete_v2, table_with_snapshot,
    };
    use crate::transaction::{Transaction, TransactionAction};

    const VALIDATION_TIMEOUT: Duration = Duration::from_secs(5);

    #[test]
    fn modes_map_to_snapshot_operations() {
        assert_eq!(Rewrite::OPERATION, Operation::Replace);
        assert_eq!(Overwrite::OPERATION, Operation::Overwrite);
    }

    #[tokio::test]
    async fn v2_overwrite_validation_completes() {
        let (table, existing_file) = table_with_existing_data(make_v2_minimal_table()).await;
        assert_overwrite_validation_completes(&table, existing_file).await;
    }

    #[tokio::test]
    async fn v3_overwrite_validation_completes() {
        let (table, existing_file) = table_with_existing_data(make_v3_minimal_table()).await;
        assert_overwrite_validation_completes(&table, existing_file).await;
    }

    #[tokio::test]
    async fn v2_rewrite_validation_completes() {
        let (table, existing_file) = table_with_existing_data(make_v2_minimal_table()).await;
        assert_rewrite_validation_completes(&table, existing_file).await;
    }

    #[tokio::test]
    async fn v3_rewrite_validation_completes() {
        let (table, existing_file) = table_with_existing_data(make_v3_minimal_table()).await;
        assert_rewrite_validation_completes(&table, existing_file).await;
    }

    #[tokio::test]
    async fn rejects_concurrent_positional_delete() {
        assert_rejects_concurrent_positional_delete(
            false,
            false,
            "Cannot commit, found new delete for added data file",
        )
        .await;
    }

    #[tokio::test]
    async fn rejects_concurrent_positional_delete_with_data_sequence_number() {
        assert_rejects_concurrent_positional_delete(
            true,
            false,
            "Cannot commit, found new positional delete for added data file",
        )
        .await;
    }

    #[tokio::test]
    async fn rejects_concurrent_fast_append_delete() {
        assert_rejects_concurrent_positional_delete(
            false,
            true,
            "Cannot commit, found new delete for added data file",
        )
        .await;
    }

    async fn assert_rejects_concurrent_positional_delete(
        set_data_sequence_number: bool,
        fast_append: bool,
        expected_message: &str,
    ) {
        let (table_s1, existing_file) = table_with_existing_data(make_v2_minimal_table()).await;
        let starting_snapshot = table_s1.metadata().current_snapshot().unwrap();
        let starting_snapshot_id = starting_snapshot.snapshot_id();
        let starting_sequence_number = starting_snapshot.sequence_number();
        let concurrent_delete = pos_delete_v2("test/concurrent-delete.parquet", &table_s1);
        let tx = Transaction::new(&table_s1);
        let mut concurrent_commit = if fast_append {
            commit_action(
                tx.fast_append().add_data_files([concurrent_delete]),
                &table_s1,
            )
            .await
        } else {
            commit_action(
                tx.row_delta().add_delete_files([concurrent_delete]),
                &table_s1,
            )
            .await
        }
        .unwrap();
        let table_s2 =
            table_with_snapshot(&table_s1, first_snapshot(concurrent_commit.take_updates()));
        let action = ReplaceFilesAction::<Overwrite>::new()
            .set_starting_snapshot_id(starting_snapshot_id)
            .set_snapshot_properties(truncate_snapshot_properties(&table_s2))
            .delete_data_files([existing_file])
            .unwrap()
            .full_table_overwrite();
        let action = if set_data_sequence_number {
            action.set_data_sequence_number(starting_sequence_number)
        } else {
            action
        };

        let result = tokio::time::timeout(VALIDATION_TIMEOUT, commit_action(action, &table_s2))
            .await
            .expect("concurrent delete validation timed out");
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected concurrent positional delete to be rejected"),
        };
        assert!(error.to_string().contains(expected_message), "{error}");
    }

    async fn assert_overwrite_validation_completes(table: &Table, existing_file: DataFile) {
        let starting_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        let action = ReplaceFilesAction::<Overwrite>::new()
            .set_starting_snapshot_id(starting_snapshot_id)
            .set_snapshot_properties(truncate_snapshot_properties(table))
            .delete_data_files([existing_file])
            .unwrap()
            .full_table_overwrite();

        tokio::time::timeout(VALIDATION_TIMEOUT, commit_action(action, table))
            .await
            .expect("overwrite validation timed out")
            .expect("overwrite commit failed");
    }

    async fn assert_rewrite_validation_completes(table: &Table, existing_file: DataFile) {
        let replacement = data_file("test/rewrite-replacement.parquet", table);
        let action = ReplaceFilesAction::<Rewrite>::new()
            .delete_data_files([existing_file])
            .unwrap()
            .add_data_files([replacement])
            .unwrap();

        tokio::time::timeout(VALIDATION_TIMEOUT, commit_action(action, table))
            .await
            .expect("rewrite validation timed out")
            .expect("rewrite commit failed");
    }

    fn truncate_snapshot_properties(table: &Table) -> HashMap<String, String> {
        HashMap::from([(
            "sm.truncated_from_snapshot".to_string(),
            table
                .metadata()
                .current_snapshot()
                .unwrap()
                .snapshot_id()
                .to_string(),
        )])
    }

    async fn table_with_existing_data(base: Table) -> (Table, DataFile) {
        let existing_file = data_file("test/existing.parquet", &base);
        let action = Transaction::new(&base)
            .fast_append()
            .add_data_files([existing_file.clone()]);
        let mut commit = commit_action(action, &base).await.unwrap();
        let table = table_with_snapshot(&base, first_snapshot(commit.take_updates()));

        (table, existing_file)
    }
    async fn table_with_files(base: Table, paths: &[&str]) -> (Table, Vec<DataFile>) {
        let files: Vec<DataFile> = paths.iter().map(|path| data_file(path, &base)).collect();
        let action = Transaction::new(&base)
            .fast_append()
            .add_data_files(files.clone());
        let mut commit = commit_action(action, &base).await.unwrap();
        let table = table_with_snapshot(&base, first_snapshot(commit.take_updates()));

        (table, files)
    }

    async fn entries_by_path(
        table: &Table,
        snapshot: &Snapshot,
    ) -> HashMap<String, ManifestStatus> {
        let manifest_list = table
            .manifest_list_reader(&Arc::new(snapshot.clone()))
            .load()
            .await
            .unwrap();
        let mut entries = HashMap::new();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await.unwrap();
            for entry in manifest.entries() {
                entries.insert(entry.data_file().file_path().to_string(), entry.status());
            }
        }
        entries
    }

    async fn commit_updates(table: &Table, action: impl TransactionAction + 'static) -> Table {
        let mut commit = tokio::time::timeout(VALIDATION_TIMEOUT, commit_action(action, table))
            .await
            .expect("commit timed out")
            .unwrap();
        table_with_snapshot(table, first_snapshot(commit.take_updates()))
    }

    async fn overwrite_error(table: &Table, action: super::OverwriteFilesAction) -> String {
        match tokio::time::timeout(VALIDATION_TIMEOUT, commit_action(action, table))
            .await
            .expect("overwrite timed out")
        {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected the overwrite to be rejected"),
        }
    }

    fn file_in_partition(path: &str, content: DataContentType, table: &Table) -> DataFile {
        DataFileBuilder::default()
            .content(content)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(5)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(200))]))
            .build()
            .unwrap()
    }

    fn deletion_vector(path: &str, data_file: &str, table: &Table) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(50)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .referenced_data_file(Some(data_file.to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(40))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn full_table_overwrite_resets_totals() {
        let (table, files) = table_with_files(make_v2_minimal_table(), &[
            "test/a.parquet",
            "test/b.parquet",
        ])
        .await;
        let action = Transaction::new(&table)
            .overwrite_files()
            .set_snapshot_properties(truncate_snapshot_properties(&table))
            .delete_data_files(files)
            .unwrap()
            .full_table_overwrite();

        let mut commit = tokio::time::timeout(VALIDATION_TIMEOUT, commit_action(action, &table))
            .await
            .expect("overwrite validation timed out")
            .unwrap();
        let snapshot = first_snapshot(commit.take_updates());
        let properties = &snapshot.summary().additional_properties;
        assert_eq!(snapshot.summary().operation, Operation::Overwrite);
        assert_eq!(properties.get("deleted-data-files").unwrap(), "2");
        assert_eq!(properties.get("deleted-records").unwrap(), "20");
        assert_eq!(properties.get("total-data-files").unwrap(), "0");
        assert_eq!(properties.get("total-records").unwrap(), "0");

        let entries = entries_by_path(&table, &snapshot).await;
        assert_eq!(
            entries,
            HashMap::from([
                ("test/a.parquet".to_string(), ManifestStatus::Deleted),
                ("test/b.parquet".to_string(), ManifestStatus::Deleted),
            ])
        );
    }

    #[tokio::test]
    async fn partial_overwrite_keeps_other_files() {
        let (table, mut files) = table_with_files(make_v2_minimal_table(), &[
            "test/a.parquet",
            "test/b.parquet",
        ])
        .await;
        let replacement = data_file("test/c.parquet", &table);
        let action = Transaction::new(&table)
            .overwrite_files()
            .delete_data_files([files.remove(0)])
            .unwrap()
            .add_data_files([replacement])
            .unwrap();

        let mut commit = tokio::time::timeout(VALIDATION_TIMEOUT, commit_action(action, &table))
            .await
            .expect("overwrite validation timed out")
            .unwrap();
        let snapshot = first_snapshot(commit.take_updates());
        let properties = &snapshot.summary().additional_properties;
        assert_eq!(properties.get("total-data-files").unwrap(), "2");
        assert_eq!(properties.get("total-records").unwrap(), "20");

        let entries = entries_by_path(&table, &snapshot).await;
        assert_eq!(
            entries,
            HashMap::from([
                ("test/a.parquet".to_string(), ManifestStatus::Deleted),
                ("test/b.parquet".to_string(), ManifestStatus::Existing),
                ("test/c.parquet".to_string(), ManifestStatus::Added),
            ])
        );
    }

    #[tokio::test]
    async fn full_overwrite_rejects_files_it_does_not_delete() {
        let (table, files) = table_with_files(make_v2_minimal_table(), &["test/a.parquet"]).await;
        let starting_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // A file appended after the caller listed the table's files.
        let table = commit_updates(
            &table,
            Transaction::new(&table)
                .fast_append()
                .add_data_files([data_file("test/b.parquet", &table)]),
        )
        .await;
        let action = Transaction::new(&table)
            .overwrite_files()
            .set_starting_snapshot_id(starting_snapshot_id)
            .delete_data_files(files)
            .unwrap()
            .full_table_overwrite();
        let error = overwrite_error(&table, action).await;
        assert!(error.contains("must delete every live file"), "{error}");
        assert!(error.contains("test/b.parquet"), "{error}");

        // A delete list naming files the table no longer has.
        let action = Transaction::new(&table)
            .overwrite_files()
            .delete_data_files([data_file("test/gone.parquet", &table)])
            .unwrap()
            .full_table_overwrite();
        let error = overwrite_error(&table, action).await;
        assert!(error.contains("must delete every live file"), "{error}");
    }

    #[tokio::test]
    async fn validation_ignores_a_replaced_deletion_vector_of_another_file() {
        let (table, mut files) = table_with_files(make_v3_minimal_table(), &[
            "test/a.parquet",
            "test/b.parquet",
        ])
        .await;
        let starting_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // b's deletion vector is added and then replaced by a newer one.
        let first_dv = deletion_vector("test/first.puffin", "test/b.parquet", &table);
        let table = commit_updates(
            &table,
            Transaction::new(&table)
                .row_delta()
                .add_delete_files([first_dv.clone()]),
        )
        .await;
        let table = commit_updates(
            &table,
            Transaction::new(&table)
                .row_delta()
                .add_delete_files([deletion_vector(
                    "test/second.puffin",
                    "test/b.parquet",
                    &table,
                )])
                .remove_delete_files([first_dv]),
        )
        .await;

        let action = Transaction::new(&table)
            .overwrite_files()
            .set_starting_snapshot_id(starting_snapshot_id)
            .delete_data_files([files.remove(0)])
            .unwrap();
        commit_updates(&table, action).await;
    }

    #[tokio::test]
    async fn validation_ignores_deletes_that_predate_the_starting_snapshot() {
        let (table, mut files) =
            table_with_files(make_v2_minimal_table(), &["test/a.parquet"]).await;
        // Deletes in a's partition and in another one, committed before the starting snapshot.
        let old_delete = pos_delete_v2("test/old-delete.parquet", &table);
        let other_delete = file_in_partition(
            "test/other-delete.parquet",
            DataContentType::PositionDeletes,
            &table,
        );
        let table = commit_updates(
            &table,
            Transaction::new(&table)
                .row_delta()
                .add_delete_files([old_delete.clone(), other_delete.clone()]),
        )
        .await;
        let starting_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();

        // A later delete snapshot carries `old_delete` forward as EXISTING in a rewritten
        // manifest, and adds a delete in the other partition only.
        let table = commit_updates(
            &table,
            Transaction::new(&table)
                .row_delta()
                .add_delete_files([file_in_partition(
                    "test/new-other-delete.parquet",
                    DataContentType::PositionDeletes,
                    &table,
                )])
                .remove_delete_files([other_delete]),
        )
        .await;
        // Another removes `old_delete`, leaving it DELETED.
        let table = commit_updates(
            &table,
            Transaction::new(&table)
                .row_delta()
                .add_delete_files([file_in_partition(
                    "test/newest-other-delete.parquet",
                    DataContentType::PositionDeletes,
                    &table,
                )])
                .remove_delete_files([old_delete]),
        )
        .await;

        let action = Transaction::new(&table)
            .overwrite_files()
            .set_starting_snapshot_id(starting_snapshot_id)
            .delete_data_files([files.remove(0)])
            .unwrap();
        commit_updates(&table, action).await;
    }

    #[tokio::test]
    async fn validates_added_delete_files() {
        let (table, files) = table_with_files(make_v3_minimal_table(), &["test/a.parquet"]).await;
        let dv = deletion_vector("test/dv.puffin", files[0].file_path(), &table);
        let table = commit_updates(
            &table,
            Transaction::new(&table)
                .row_delta()
                .add_delete_files([dv.clone()]),
        )
        .await;

        // v3 position deletes must be deletion vectors.
        let action = Transaction::new(&table)
            .overwrite_files()
            .delete_data_files([dv])
            .unwrap()
            .add_data_files([pos_delete_v2("test/parquet-delete.parquet", &table)])
            .unwrap();
        let error = overwrite_error(&table, action).await;
        assert!(error.contains("V3 requires deletion vectors"), "{error}");
    }
}
