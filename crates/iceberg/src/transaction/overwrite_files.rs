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

use super::snapshot::{DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer};
use super::{ActionCommit, StatelessAction};
use crate::error::{Error, ErrorKind, Result};
use crate::spec::{
    DataContentType, DataFile, ManifestEntry, ManifestFile, ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::validate::{SnapshotValidator, validate_no_new_deletes_for_data_files};

/// Transaction action that replaces files as a logical overwrite.
///
/// The deleted files are marked DELETED and the added files are added in one `overwrite`
/// snapshot. Before committing, it checks that no delete files that apply to the deleted data
/// files were added since the starting snapshot.
#[derive(Clone)]
pub struct OverwriteFilesAction {
    commit_uuid: Option<Uuid>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    deleted_delete_files: Vec<DataFile>,
    starting_snapshot_id: Option<i64>,
    full_table_overwrite: bool,
}

struct OverwriteFilesOperation {
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
    deleted_delete_files: Vec<DataFile>,
    starting_snapshot_id: Option<i64>,
    full_table_overwrite: bool,
}

impl OverwriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            snapshot_properties: Default::default(),
            added_data_files: vec![],
            added_delete_files: vec![],
            deleted_data_files: vec![],
            deleted_delete_files: vec![],
            starting_snapshot_id: None,
            full_table_overwrite: false,
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

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, snapshot_properties: HashMap<String, String>) -> Self {
        self.snapshot_properties = snapshot_properties;
        self
    }

    /// Set the snapshot ID used in any reads for this operation.
    pub fn set_starting_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.starting_snapshot_id = Some(snapshot_id);
        self
    }

    /// Mark this overwrite as replacing the complete logical table contents.
    pub fn full_table_overwrite(mut self) -> Self {
        self.full_table_overwrite = true;
        self
    }
}

impl Default for OverwriteFilesAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StatelessAction for OverwriteFilesAction {
    async fn commit(&self, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.deleted_data_files.clone(),
            self.deleted_delete_files.clone(),
        );

        snapshot_producer.validate_added_data_files()?;
        snapshot_producer.validate_added_delete_files()?;

        let overwrite_operation = OverwriteFilesOperation {
            added_data_files: self.added_data_files.clone(),
            added_delete_files: self.added_delete_files.clone(),
            deleted_data_files: self.deleted_data_files.clone(),
            deleted_delete_files: self.deleted_delete_files.clone(),
            starting_snapshot_id: self.starting_snapshot_id,
            full_table_overwrite: self.full_table_overwrite,
        };

        // todo should be able to configure to use the merge manifest process
        snapshot_producer
            .commit(overwrite_operation, DefaultManifestProcess)
            .await
    }
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

impl SnapshotValidator for OverwriteFilesOperation {
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

        // If there are replaced data files, there cannot be any new row-level deletes for those data files
        if !self.deleted_data_files.is_empty() {
            validate_no_new_deletes_for_data_files(
                base,
                self.starting_snapshot_id,
                parent_snapshot_id,
                &self.deleted_data_files,
                false,
            )
            .await?;
        }

        Ok(())
    }
}

impl SnapshotProduceOperation for OverwriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Overwrite
    }

    fn truncate_full_table(&self) -> bool {
        self.full_table_overwrite
    }

    async fn delete_entries(
        &self,
        snapshot_producer: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // Find entries that are associated with deleted files
        let table = snapshot_producer.table;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        let manifest_list = table.manifest_list_reader(snapshot).load().await?;
        let removed = snapshot_producer.removed_files();

        let mut delete_entries = Vec::new();
        let mut surviving_files = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await?;
            for entry in manifest.entries().iter().filter(|entry| entry.is_alive()) {
                if removed.contains(entry) {
                    delete_entries.push(copy_with_deleted_status(entry.as_ref()));
                } else {
                    surviving_files.push(entry.file_path().to_string());
                }
            }
        }
        removed.check_unambiguous(&delete_entries)?;

        // A full table overwrite resets the table totals, so it must actually remove every
        // live file. Files added after the caller listed the table's files, or a delete list
        // naming files that are no longer there, would otherwise survive under totals of zero.
        if self.full_table_overwrite && !surviving_files.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "A full table overwrite must delete every live file, but {} are not deleted, e.g. {}",
                    surviving_files.len(),
                    surviving_files[0]
                ),
            ));
        }

        Ok(delete_entries)
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
        let removed = snapshot_producer.removed_files();

        let mut existing_files = Vec::new();
        for manifest_file in manifest_list.entries() {
            let manifest = table.manifest_reader().read(manifest_file).await?;

            // Whether this manifest has live entries for files to delete
            let has_deleted_files = manifest
                .entries()
                .iter()
                .any(|entry| entry.is_alive() && removed.contains(entry));

            if !has_deleted_files
                && (manifest_file.has_added_files() || manifest_file.has_existing_files())
            {
                // All files from the existing manifest entries are still valid
                existing_files.push(manifest_file.clone());
            } else if manifest
                .entries()
                .iter()
                .any(|entry| entry.is_alive() && !removed.contains(entry))
            {
                let mut manifest_writer = snapshot_producer.new_manifest_writer(
                    manifest_file.content,
                    manifest.metadata().schema().clone(),
                    Arc::new(manifest.metadata().partition_spec().clone()),
                )?;

                manifest
                    .entries()
                    .iter()
                    .filter(|entry| entry.is_alive() && !removed.contains(entry))
                    .try_for_each(|entry| manifest_writer.add_existing_entry((**entry).clone()))?;

                existing_files.push(manifest_writer.write_manifest_file().await?);
            }
        }

        Ok(existing_files)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, ManifestStatus,
        Operation, Snapshot, Struct,
    };
    use crate::table::Table;
    use crate::test_utils::commit_with_fresh_state;
    use crate::transaction::fork_test_utils::{
        data_file, first_snapshot, make_v3_minimal_table, pos_delete_v2, table_with_snapshot,
    };
    use crate::transaction::tests::make_v2_minimal_table;
    use crate::transaction::{Transaction, TransactionAction};

    const VALIDATION_TIMEOUT: Duration = Duration::from_secs(5);

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

    async fn table_with_existing_data(base: Table, paths: &[&str]) -> (Table, Vec<DataFile>) {
        let files: Vec<DataFile> = paths.iter().map(|path| data_file(path, &base)).collect();
        let action = Transaction::new(&base)
            .fast_append()
            .add_data_files(files.clone());
        let mut commit = commit_with_fresh_state(action, &base).await.unwrap();
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

    async fn assert_overwrite_validation_completes(table: &Table, existing_files: Vec<DataFile>) {
        let starting_snapshot_id = table.metadata().current_snapshot().unwrap().snapshot_id();
        let action = Transaction::new(table)
            .overwrite_files()
            .set_starting_snapshot_id(starting_snapshot_id)
            .set_snapshot_properties(truncate_snapshot_properties(table))
            .delete_data_files(existing_files)
            .unwrap()
            .full_table_overwrite();

        tokio::time::timeout(VALIDATION_TIMEOUT, commit_with_fresh_state(action, table))
            .await
            .expect("overwrite validation timed out")
            .expect("overwrite commit failed");
    }

    #[tokio::test]
    async fn v2_overwrite_validation_completes() {
        let (table, files) =
            table_with_existing_data(make_v2_minimal_table(), &["test/existing.parquet"]).await;
        assert_overwrite_validation_completes(&table, files).await;
    }

    #[tokio::test]
    async fn v3_overwrite_validation_completes() {
        let (table, files) =
            table_with_existing_data(make_v3_minimal_table(), &["test/existing.parquet"]).await;
        assert_overwrite_validation_completes(&table, files).await;
    }

    async fn assert_rejects_concurrent_delete(
        table_s1: Table,
        existing: DataFile,
        delete: DataFile,
    ) {
        let starting_snapshot_id = table_s1
            .metadata()
            .current_snapshot()
            .unwrap()
            .snapshot_id();
        let mut concurrent_commit = commit_with_fresh_state(
            Transaction::new(&table_s1)
                .row_delta()
                .add_delete_files([delete]),
            &table_s1,
        )
        .await
        .unwrap();
        let table_s2 =
            table_with_snapshot(&table_s1, first_snapshot(concurrent_commit.take_updates()));
        let action = Transaction::new(&table_s2)
            .overwrite_files()
            .set_starting_snapshot_id(starting_snapshot_id)
            .set_snapshot_properties(truncate_snapshot_properties(&table_s2))
            .delete_data_files([existing])
            .unwrap()
            .full_table_overwrite();

        let result = tokio::time::timeout(
            VALIDATION_TIMEOUT,
            commit_with_fresh_state(action, &table_s2),
        )
        .await
        .expect("concurrent delete validation timed out");
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected concurrent delete to be rejected"),
        };
        assert!(
            error
                .to_string()
                .contains("Cannot commit, found new delete for added data file"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn v2_rejects_concurrent_positional_delete() {
        let (table, mut files) =
            table_with_existing_data(make_v2_minimal_table(), &["test/existing.parquet"]).await;
        let delete = pos_delete_v2("test/concurrent-delete.parquet", &table);
        assert_rejects_concurrent_delete(table, files.remove(0), delete).await;
    }

    #[tokio::test]
    async fn v3_rejects_concurrent_deletion_vector() {
        let (table, mut files) =
            table_with_existing_data(make_v3_minimal_table(), &["test/existing.parquet"]).await;
        let existing = files.remove(0);
        let dv = DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path("test/concurrent-dv.puffin".to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(50)
            .record_count(1)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(Struct::from_iter([Some(Literal::long(100))]))
            .referenced_data_file(Some(existing.file_path().to_string()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(40))
            .build()
            .unwrap();
        assert_rejects_concurrent_delete(table, existing, dv).await;
    }

    /// A delete written after the partition source was promoted to long carries a long value,
    /// while the replaced file written before keeps its int. The scan widens the int before it
    /// matches deletes, so the replacement would hide the delete if validation did not.
    #[tokio::test]
    async fn rejects_concurrent_delete_across_a_partition_type_promotion() {
        use crate::TableIdent;
        use crate::io::FileIO;
        use crate::spec::{
            FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder,
            Transform, Type, UnboundPartitionField, UnboundPartitionSpec,
        };
        use crate::test_utils::test_runtime;

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
        let existing = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory://warehouse/t/data/old.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::int(7))]))
            .build()
            .unwrap();
        let mut commit = commit_with_fresh_state(
            Transaction::new(&table)
                .fast_append()
                .add_data_files([existing.clone()]),
            &table,
        )
        .await
        .unwrap();
        let table = table_with_snapshot(&table, first_snapshot(commit.take_updates()));

        let metadata = TableMetadataBuilder::new_from_metadata(table.metadata().clone(), None)
            .add_current_schema(schema(1, PrimitiveType::Long))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));
        let delete = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path("memory://warehouse/t/data/eq.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(50)
            .record_count(1)
            .partition_spec_id(0)
            .partition(Struct::from_iter([Some(Literal::long(7))]))
            .equality_ids(Some(vec![2]))
            .build()
            .unwrap();
        assert_rejects_concurrent_delete(table, existing, delete).await;
    }

    #[tokio::test]
    async fn rejects_empty_delete_list() {
        let (table, _) =
            table_with_existing_data(make_v2_minimal_table(), &["test/existing.parquet"]).await;
        let action = Transaction::new(&table)
            .overwrite_files()
            .full_table_overwrite();
        let error = match commit_with_fresh_state(action, &table).await {
            Err(error) => error,
            Ok(_) => panic!("expected an overwrite without deletes to be rejected"),
        };
        assert!(
            error
                .to_string()
                .contains("Files to delete cannot be empty")
        );
    }

    #[tokio::test]
    async fn full_table_overwrite_resets_totals() {
        let (table, files) = table_with_existing_data(make_v2_minimal_table(), &[
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

        let mut commit =
            tokio::time::timeout(VALIDATION_TIMEOUT, commit_with_fresh_state(action, &table))
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
        let (table, mut files) = table_with_existing_data(make_v2_minimal_table(), &[
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

        let mut commit =
            tokio::time::timeout(VALIDATION_TIMEOUT, commit_with_fresh_state(action, &table))
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

    async fn commit_updates(table: &Table, action: impl TransactionAction + 'static) -> Table {
        let mut commit =
            tokio::time::timeout(VALIDATION_TIMEOUT, commit_with_fresh_state(action, table))
                .await
                .expect("commit timed out")
                .unwrap();
        table_with_snapshot(table, first_snapshot(commit.take_updates()))
    }

    async fn overwrite_error(table: &Table, action: super::OverwriteFilesAction) -> String {
        match tokio::time::timeout(VALIDATION_TIMEOUT, commit_with_fresh_state(action, table))
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
    async fn full_overwrite_rejects_files_it_does_not_delete() {
        let (table, files) =
            table_with_existing_data(make_v2_minimal_table(), &["test/a.parquet"]).await;
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
        let (table, mut files) = table_with_existing_data(make_v3_minimal_table(), &[
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
            table_with_existing_data(make_v2_minimal_table(), &["test/a.parquet"]).await;
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
        let (table, files) =
            table_with_existing_data(make_v3_minimal_table(), &["test/a.parquet"]).await;
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
