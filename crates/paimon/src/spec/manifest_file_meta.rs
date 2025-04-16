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

use crate::spec::stats::BinaryTableStats;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Metadata of a manifest file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestFileMeta.java>
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFileMeta {
    #[serde(rename = "_VERSION")]
    version: i32,

    /// manifest file name
    #[serde(rename = "_FILE_NAME")]
    file_name: String,

    /// manifest file size.
    #[serde(rename = "_FILE_SIZE")]
    file_size: i64,

    /// number added files in manifest.
    #[serde(rename = "_NUM_ADDED_FILES")]
    num_added_files: i64,

    /// number deleted files in manifest.
    #[serde(rename = "_NUM_DELETED_FILES")]
    num_deleted_files: i64,

    /// partition stats, the minimum and maximum values of partition fields in this manifest are beneficial for skipping certain manifest files during queries, it is a SimpleStats.
    #[serde(rename = "_PARTITION_STATS")]
    partition_stats: BinaryTableStats,

    /// schema id when writing this manifest file.
    #[serde(rename = "_SCHEMA_ID")]
    schema_id: i64,

    #[serde(rename = "_MIN_BUCKET", skip_serializing_if = "Option::is_none")]
    min_bucket: Option<i32>,

    #[serde(rename = "_MAX_BUCKET", skip_serializing_if = "Option::is_none")]
    max_bucket: Option<i32>,

    #[serde(rename = "_MIN_LEVEL", skip_serializing_if = "Option::is_none")]
    min_level: Option<i32>,

    #[serde(rename = "_MAX_LEVEL", skip_serializing_if = "Option::is_none")]
    max_level: Option<i32>,
}

impl ManifestFileMeta {
    /// Get the manifest file name
    #[inline]
    pub fn file_name(&self) -> &str {
        self.file_name.as_str()
    }

    /// Get the manifest file size.
    #[inline]
    pub fn file_size(&self) -> i64 {
        self.file_size
    }

    /// Get the number added files in manifest.
    #[inline]
    pub fn num_added_files(&self) -> i64 {
        self.num_added_files
    }

    /// Get the number deleted files in manifest.
    #[inline]
    pub fn num_deleted_files(&self) -> i64 {
        self.num_deleted_files
    }

    /// Get the partition stats
    pub fn partition_stats(&self) -> &BinaryTableStats {
        &self.partition_stats
    }

    /// Get the schema id when writing this manifest file.
    #[inline]
    pub fn schema_id(&self) -> i64 {
        self.schema_id
    }

    /// Get the version of this manifest file
    #[inline]
    pub fn version(&self) -> i32 {
        self.version
    }

    #[inline]
    pub fn min_bucket(&self) -> Option<i32> {
        self.min_bucket
    }

    #[inline]
    pub fn max_bucket(&self) -> Option<i32> {
        self.max_bucket
    }
    #[inline]
    pub fn min_level(&self) -> Option<i32> {
        self.min_level
    }
    #[inline]
    pub fn max_level(&self) -> Option<i32> {
        self.max_level
    }
    pub fn with_min_bucket(mut self, min_bucket: Option<i32>) -> Self {
        self.min_bucket = min_bucket;
        self
    }

    pub fn with_max_bucket(mut self, max_bucket: Option<i32>) -> Self {
        self.max_bucket = max_bucket;
        self
    }

    pub fn with_min_level(mut self, min_level: Option<i32>) -> Self {
        self.min_level = min_level;
        self
    }

    pub fn with_max_level(mut self, max_level: Option<i32>) -> Self {
        self.max_level = max_level;
        self
    }

    #[inline]
    pub fn new(
        file_name: String,
        file_size: i64,
        num_added_files: i64,
        num_deleted_files: i64,
        partition_stats: BinaryTableStats,
        schema_id: i64,
    ) -> ManifestFileMeta {
        Self {
            version: 2,
            file_name,
            file_size,
            num_added_files,
            num_deleted_files,
            partition_stats,
            schema_id,
            min_bucket: None,
            max_bucket: None,
            min_level: None,
            max_level: None,
        }
    }
}

impl Display for ManifestFileMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}, {}, {}, {}, {:?}, {}}}",
            self.file_name,
            self.file_size,
            self.num_added_files,
            self.num_deleted_files,
            self.partition_stats,
            self.schema_id
        )
    }
}
