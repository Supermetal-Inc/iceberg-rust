<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Supermetal patch stack

`supermetal` is upstream `apache/iceberg-rust` plus one commit per patch, in the order
below, with no merges. The branch `supermetal-0.9` keeps the earlier fork on upstream 0.9. It sits on upstream commit `8119f796c`. Patch ids stay fixed across
rebases while commit hashes change, so find a patch by its commit subject.

Every commit builds and passes its tests on top of the commits before it. A patch can be dropped
alone when no later patch depends on it.

## Patches

| Id | Commit subject | Author | Depends on | Upstream | Owning tests | Drop when |
|---|---|---|---|---|---|---|
| SM-01 | `fix(arrow): tag timestamptz columns with the "UTC" zone name` | Wasif | none | Upstream writes `"+00:00"` | `tests/supermetal/utc.rs` and the UTC expectations in the `arrow::schema` and `inspect` tests | Snowflake reads `"+00:00"` as it reads `"UTC"` |
| SM-02 | `feat(writer): allow writing variant columns to parquet` | Christian Thiel | none | Upstream reads variants since #2188 and has no write path | `parquet_writer::tests::test_index_by_parquet_path_variant` and `tests/supermetal/variant.rs` | Upstream writes variant columns |
| SM-03 | `fix(arrow): write unshredded variants with Java's parquet layout` | Wasif | SM-02 | none | `tests/supermetal/variant.rs` and the variant expectations in the `arrow::schema` tests | Upstream writes `value` as required for unshredded variants |
| SM-04 | `fix(writer): stop at variant columns when projecting equality fields` | Wasif | none | none, and a good candidate for an upstream PR | `tests/supermetal/variant_equality_delete.rs` | Upstream stops at variant columns |
| SM-05 | `feat(writer): add iceberg.schema to the parquet footer` | Jonathan Chen | none | #2249 and #2724, both closed | `tests/supermetal/iceberg_schema.rs` | Upstream writes the footer key |
| SM-06 | `fix(writer): embed iceberg.schema in the Arrow schema metadata` | vovacf201 | SM-05 | #2250, closed | `tests/supermetal/iceberg_schema.rs` | Snowflake reads the schema from the footer alone |
| SM-07 | `feat(writer): expose written size of partitioning writers` | Wasif | none | none, and a good candidate for an upstream PR | `tests/supermetal/written_size.rs` | Upstream exposes the sizes |
| SM-08 | `feat(spec): make DataFile bounds public` | Wasif | none | none, and metrics truncation through `write.metadata.metrics.*` would replace it | `tests/supermetal/app_api.rs` | Upstream truncates bounds or exposes setters |
| SM-09 | `feat(writer): add a deletion vector writer` | Wasif, from risingwavelabs/iceberg-rust | none | #3232 proposed the serializer and closed, and #3461 made `delete_vector` public | the `writer::base_writer::deletion_vector_writer` tests with the `ArrowReader` round trip, and `tests/supermetal/app_api.rs` | Upstream writes deletion vectors |
| SM-10 | `feat(storage-opendal): make the write chunk size configurable` | Wasif | none | #3179 bounds S3 parts and is open | `crates/storage/opendal/tests/supermetal_chunk_size.rs` | Upstream bounds parts for GCS and ADLS as well |
| SM-11 | `feat(rest): expose OAuth token expiry and regeneration` | Wasif | none | #3160 proposed automatic refresh and closed | `crates/catalog/rest/tests/supermetal_token.rs` | Upstream exposes the expiry and a forced refresh, which proactive refresh needs |
| SM-12 | `feat(transaction): add commit_once so callers control retries` | Wasif | none | #3075, closed, and a good candidate for an upstream PR | `transaction::tests::test_commit_once_does_not_reload_or_retry`, `test_commit_once_readonly_table_fails` and `tests/supermetal/app_api.rs` | Upstream offers a commit without reload and retry |
| SM-13 | `feat(transaction): let the snapshot producer add delete files and remove files` | Shawn Chang | none | builds on #3253, and #3564 and #3046 propose a merging producer | `transaction::snapshot::tests` | An upstream merging producer adds and removes delete files and passes these tests |
| SM-14 | `feat(transaction): add a row delta action` | Wasif | SM-13 | #2203 covers copy on write only | `transaction::row_delta::tests` | An upstream merge on read row delta passes these tests |
| SM-15 | `feat(transaction): add an overwrite files action` | Wasif | SM-13 and SM-14 | #3300 and #3046, both open | `transaction::overwrite_files::tests` | An upstream overwrite passes these tests |
| SM-16 | `test: pin the fork APIs Supermetal calls` | Wasif | SM-08, SM-09, SM-10, SM-12, SM-14 and SM-15 | none | `tests/supermetal/app_api.rs` | Never, and it changes with the patches |
| SM-17 | `chore: document and check the patch stack` | Wasif | none | none | `supermetal/check-stack.sh` | Never |

## Behavior a new revision must keep

Adopt a new revision when these tests pass and Supermetal builds and passes its Iceberg tests
against it. A commit can succeed and still write a manifest that readers reject, so the tests
read back the manifests a commit writes and check that deletes still apply where that matters.
`tests/supermetal/app_api.rs` catches changes to the fork's own APIs. Only building Supermetal
catches changes to upstream APIs.

| Behavior | Tests |
|---|---|
| Partition spec evolution | `snapshot::tests::test_summary_counts_deleted_entries_under_their_own_spec`, `row_delta::tests::adds_delete_files_under_their_own_partition_spec` |
| Dropped partition source column | `snapshot::tests::test_deleted_entries_under_spec_with_dropped_source_column` |
| Type promotion of partition values and bounds | `snapshot::tests::test_partition_values_from_before_a_type_promotion`, `snapshot::tests::test_promote_bound`, `snapshot::tests::test_removed_file_with_a_bound_outside_the_promoted_type`, `row_delta::tests::equality_delete_written_after_a_type_promotion` |
| Deletion vectors sharing a Puffin file | `row_delta::tests::removes_one_deletion_vector_from_a_shared_puffin_file`, `row_delta::tests::rejects_ambiguous_path_only_removal` |
| Encrypted tables | `row_delta::tests::rewrites_manifests_of_an_encrypted_table_encrypted` |
| Sequence numbers of rewritten and deleted entries | `row_delta::tests::remove_delete_file_rewrites_manifest_and_preserves_sequence_numbers` |
| Concurrent deletes | `overwrite_files::tests::v2_rejects_concurrent_positional_delete`, `v3_rejects_concurrent_deletion_vector`, `rejects_concurrent_delete_across_a_partition_type_promotion`, `validation_ignores_a_replaced_deletion_vector_of_another_file`, `validation_ignores_deletes_that_predate_the_starting_snapshot`, `*_validation_completes` |
| Summary totals | `row_delta::tests::overwrite_keeps_table_totals`, `overwrite_files::tests::full_table_overwrite_resets_totals`, `partial_overwrite_keeps_other_files`, `full_overwrite_rejects_files_it_does_not_delete` |
| Deletion vectors readable by upstream | `deletion_vector_writer::tests::test_deletion_vector_applied_by_reader` |
| Formats Snowflake reads | `tests/supermetal/utc.rs`, `variant.rs`, `iceberg_schema.rs` |

## Rebasing onto a new upstream

A rebase follows these steps.

1. Fetch upstream and keep the reviewed revision with `git branch supermetal-<date> supermetal`.
2. Turn on `git config rerere.enabled true` so conflict resolutions repeat across rebases.
3. Run `git rebase --onto <new upstream commit> <old base> supermetal`. Finish each
   conflict with `git rebase --continue`, which keeps the commit's author.
4. For each patch whose upstream replacement landed, port its owning tests onto the replacement
   and drop the patch once they pass. Keep those tests unless upstream covers the same behavior,
   especially the schema evolution, delete applicability and encryption tests above.
5. Review every patch that changed with `git range-diff <old base>..<old tip> <new base>..HEAD`.
6. Run `supermetal/check-stack.sh <new base>`, then
   `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` and the tests.
7. Build Supermetal against the new revision and run its Iceberg tests.

The next rebase crosses apache/iceberg-rust#3474, which replaces `transaction/snapshot.rs` with a
`snapshot` module and an append only producer. Porting SM-13 to SM-15 onto that design is its own
piece of work. It costs least after upstream merges a merging producer such as #3564 or #3046.

## Conventions

The stack follows these rules.

- Each patch is one commit. Original authors stay as commit authors, and mixed work adds a
  `Co-authored-by` trailer.
- Fork tests live in files only the fork has, so upstream test modules stay untouched. These are
  `crates/iceberg/tests/supermetal/`, `crates/*/tests/supermetal_*.rs` and
  `crates/iceberg/src/transaction/fork_test_utils.rs`. Tests that need crate internals sit next
  to the code they test.
- The `public-api.txt` files stay as upstream has them, because regenerating them would conflict
  on every rebase. Upstream's `Public API` workflow then fails on pull requests in the fork, so
  turn it off in the fork's Actions settings.
- The fork runs no CI of its own. Run `supermetal/check-stack.sh` and the checks in step 6 before
  pushing.
- The fork's default branch tracks upstream main, because Dependabot scans only the default
  branch.

## What Supermetal relies on

Supermetal depends on these properties of the stack and its upstream base. Recheck them at every
rebase.

- Supermetal removes a deletion vector by its path and referenced data file. SM-13 rejects a
  path only removal that matches several vectors in one Puffin file.
- Supermetal sets `s3.path-style-access` from its connector config for S3 storage. Storage
  without that config gets the upstream default, which is virtual host addressing since
  apache/iceberg-rust#2330.
- Supermetal injects its own `AuthManager` for REST basic and SigV4 authentication, because
  upstream resolves only `none` and `oauth2` from `rest.auth.type`.
- Supermetal's DuckDB test helpers exchange Arrow data with duckdb through the C data interface,
  so duckdb must use the stack's Arrow version. Supermetal pins duckdb to a duckdb-rs commit on
  Arrow 59 until a duckdb 1.x release moves to it.

The Supermetal branch that adopts this stack explains each migration step in its commit messages.
