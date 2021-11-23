/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.CharSequenceWrapper;
import org.apache.iceberg.util.ManifestFileUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionSet;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ManifestFilterManager<F extends ContentFile<F>> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestFilterManager.class);
  private static final Joiner COMMA = Joiner.on(",");

  protected static class DeleteException extends ValidationException {
    private final String partition;

    private DeleteException(String partition) {
      super("Operation would delete existing data");
      this.partition = partition;
    }

    public String partition() {
      return partition;
    }
  }

  private final Map<Integer, PartitionSpec> specsById;
  private final PartitionSet deleteFilePartitions;
  private final PartitionSet dropPartitions;
  private final CharSequenceSet deletePaths = CharSequenceSet.empty();
  private Expression deleteExpression = Expressions.alwaysFalse();
  private long minSequenceNumber = 0;
  private boolean hasPathOnlyDeletes = false;
  private boolean failAnyDelete = false;
  private boolean failMissingDeletePaths = false;
  private int duplicateDeleteCount = 0;

  // cache filtered manifests to avoid extra work when commits fail.
  private final Map<ManifestFile, ManifestFile> filteredManifests = Maps.newConcurrentMap();

  // tracking where files were deleted to validate retries quickly
  private final Map<ManifestFile, Iterable<F>> filteredManifestToDeletedFiles =
      Maps.newConcurrentMap();

  protected ManifestFilterManager(Map<Integer, PartitionSpec> specsById) {
    this.specsById = specsById;
    this.deleteFilePartitions = PartitionSet.create(specsById);
    this.dropPartitions = PartitionSet.create(specsById);
  }

  protected abstract void deleteFile(String location);
  protected abstract ManifestWriter<F> newManifestWriter(PartitionSpec spec);
  protected abstract ManifestReader<F> newManifestReader(ManifestFile manifest);

  protected void failAnyDelete() {
    this.failAnyDelete = true;
  }

  protected void failMissingDeletePaths() {
    this.failMissingDeletePaths = true;
  }

  /**
   * Add a filter to match files to delete. A file will be deleted if all of the rows it contains
   * match this or any other filter passed to this method.
   *
   * @param expr an expression to match rows.
   */
  protected void deleteByRowFilter(Expression expr) {
    Preconditions.checkNotNull(expr, "Cannot delete files using filter: null");
    invalidateFilteredCache();
    this.deleteExpression = Expressions.or(deleteExpression, expr);
  }

  /**
   * Add a partition tuple to drop from the table during the delete phase.
   */
  protected void dropPartition(int specId, StructLike partition) {
    Preconditions.checkNotNull(partition, "Cannot delete files in invalid partition: null");
    invalidateFilteredCache();
    dropPartitions.add(specId, partition);
  }

  /**
   * Set the sequence number used to remove old delete files.
   * <p>
   * Delete files with a sequence number older than the given value will be removed. By setting this to the sequence
   * number of the oldest data file in the table, this will continuously remove delete files that are no longer needed
   * because deletes cannot match any existing rows in the table.
   *
   * @param sequenceNumber a sequence number used to remove old delete files
   */
  protected void dropDeleteFilesOlderThan(long sequenceNumber) {
    Preconditions.checkArgument(sequenceNumber >= 0,
        "Invalid minimum data sequence number: %s", sequenceNumber);
    this.minSequenceNumber = sequenceNumber;
  }

  /**
   * Add a specific path to be deleted in the new snapshot.
   */
  void delete(F file) {
    Preconditions.checkNotNull(file, "Cannot delete file: null");
    invalidateFilteredCache();
    deletePaths.add(file.path());
    deleteFilePartitions.add(file.specId(), file.partition());
  }

  /**
   * Add a specific path to be deleted in the new snapshot.
   */
  void delete(CharSequence path) {
    Preconditions.checkNotNull(path, "Cannot delete file path: null");
    invalidateFilteredCache();
    this.hasPathOnlyDeletes = true;
    deletePaths.add(path);
  }

  /**
   * Filter deleted files out of a list of manifests.
   *
   * @param tableSchema the current table schema
   * @param manifests a list of manifests to be filtered
   * @return an array of filtered manifests
   */
  List<ManifestFile> filterManifests(Schema tableSchema, List<ManifestFile> manifests) {
    if (manifests == null || manifests.isEmpty()) {
      validateRequiredDeletes();
      return ImmutableList.of();
    }

    ManifestFile[] filtered = new ManifestFile[manifests.size()];
    // open all of the manifest files in parallel, use index to avoid reordering
    Tasks.range(filtered.length)
        .stopOnFailure().throwFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .run(index -> {
          ManifestFile manifest = filterManifest(tableSchema, manifests.get(index));
          filtered[index] = manifest;
        });

    validateRequiredDeletes(filtered);

    return Arrays.asList(filtered);
  }

  /**
   * Creates a snapshot summary builder with the files deleted from the set of filtered manifests.
   *
   * @param manifests a set of filtered manifests
   */
  SnapshotSummary.Builder buildSummary(Iterable<ManifestFile> manifests) {
    SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();

    for (ManifestFile manifest : manifests) {
      PartitionSpec manifestSpec = specsById.get(manifest.partitionSpecId());
      Iterable<F> manifestDeletes = filteredManifestToDeletedFiles.get(manifest);
      if (manifestDeletes != null) {
        for (F file : manifestDeletes) {
          summaryBuilder.deletedFile(manifestSpec, file);
        }
      }
    }

    summaryBuilder.incrementDuplicateDeletes(duplicateDeleteCount);

    return summaryBuilder;
  }

  /**
   * Throws a {@link ValidationException} if any deleted file was not present in a filtered manifest.
   *
   * @param manifests a set of filtered manifests
   */
  @SuppressWarnings("CollectionUndefinedEquality")
  private void validateRequiredDeletes(ManifestFile... manifests) {
    if (failMissingDeletePaths) {
      CharSequenceSet deletedFiles = deletedFiles(manifests);
      ValidationException.check(deletedFiles.containsAll(deletePaths),
          "Missing required files to delete: %s",
          COMMA.join(Iterables.filter(deletePaths, path -> !deletedFiles.contains(path))));
    }
  }

  private CharSequenceSet deletedFiles(ManifestFile[] manifests) {
    CharSequenceSet deletedFiles = CharSequenceSet.empty();

    if (manifests != null) {
      for (ManifestFile manifest : manifests) {
        Iterable<F> manifestDeletes = filteredManifestToDeletedFiles.get(manifest);
        if (manifestDeletes != null) {
          for (F file : manifestDeletes) {
            deletedFiles.add(file.path());
          }
        }
      }
    }

    return deletedFiles;
  }

  /**
   * Deletes filtered manifests that were created by this class, but are not in the committed manifest set.
   *
   * @param committed the set of manifest files that were committed
   */
  void cleanUncommitted(Set<ManifestFile> committed) {
    // iterate over a copy of entries to avoid concurrent modification
    List<Map.Entry<ManifestFile, ManifestFile>> filterEntries =
        Lists.newArrayList(filteredManifests.entrySet());

    for (Map.Entry<ManifestFile, ManifestFile> entry : filterEntries) {
      // remove any new filtered manifests that aren't in the committed list
      ManifestFile manifest = entry.getKey();
      ManifestFile filtered = entry.getValue();
      if (!committed.contains(filtered)) {
        // only delete if the filtered copy was created
        if (!manifest.equals(filtered)) {
          deleteFile(filtered.path());
        }

        // remove the entry from the cache
        filteredManifests.remove(manifest);
      }
    }
  }

  private void invalidateFilteredCache() {
    cleanUncommitted(SnapshotProducer.EMPTY_SET);
  }

  /**
   * @return a ManifestReader that is a filtered version of the input manifest.
   */
  private ManifestFile filterManifest(Schema tableSchema, ManifestFile manifest) {
    ManifestFile cached = filteredManifests.get(manifest);
    if (cached != null) {
      return cached;
    }

    boolean hasLiveFiles = manifest.hasAddedFiles() || manifest.hasExistingFiles();
    if (!hasLiveFiles || !canContainDeletedFiles(manifest)) {
      filteredManifests.put(manifest, manifest);
      return manifest;
    }

    try (ManifestReader<F> reader = newManifestReader(manifest)) {
      // this assumes that the manifest doesn't have files to remove and streams through the
      // manifest without copying data. if a manifest does have a file to remove, this will break
      // out of the loop and move on to filtering the manifest.
      ExpressionEvaluator deleteEvaluator = new ExpressionEvaluator(tableSchema, reader.spec(), deleteExpression);

      boolean hasDeletedFiles = manifestHasDeletedFiles(deleteEvaluator, reader);
      if (!hasDeletedFiles) {
        filteredManifests.put(manifest, manifest);
        return manifest;
      }

      return filterManifestWithDeletedFiles(deleteEvaluator, manifest, reader);

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest: %s", manifest);
    }
  }

  private boolean canContainDeletedFiles(ManifestFile manifest) {
    boolean canContainExpressionDeletes;
    if (deleteExpression != null && deleteExpression != Expressions.alwaysFalse()) {
      ManifestEvaluator manifestEvaluator =
          ManifestEvaluator.forRowFilter(deleteExpression, specsById.get(manifest.partitionSpecId()), true);
      canContainExpressionDeletes = manifestEvaluator.eval(manifest);
    } else {
      canContainExpressionDeletes = false;
    }

    boolean canContainDroppedPartitions;
    if (dropPartitions.size() > 0) {
      canContainDroppedPartitions = ManifestFileUtil.canContainAny(manifest, dropPartitions, specsById);
    } else {
      canContainDroppedPartitions = false;
    }

    boolean canContainDroppedFiles;
    if (hasPathOnlyDeletes) {
      canContainDroppedFiles = true;
    } else if (deletePaths.size() > 0) {
      // because there were no path-only deletes, the set of deleted file partitions is valid
      canContainDroppedFiles = ManifestFileUtil.canContainAny(manifest, deleteFilePartitions, specsById);
    } else {
      canContainDroppedFiles = false;
    }

    boolean canContainDropBySeq = manifest.content() == ManifestContent.DELETES &&
        manifest.minSequenceNumber() < minSequenceNumber;

    return canContainExpressionDeletes || canContainDroppedPartitions || canContainDroppedFiles || canContainDropBySeq;
  }

  @SuppressWarnings("CollectionUndefinedEquality")
  private boolean manifestHasDeletedFiles(ExpressionEvaluator deleteEvaluator, ManifestReader<F> reader) {
    boolean isDelete = reader.isDeleteManifestReader();
    boolean hasDeletedFiles = false;
    for (ManifestEntry<F> entry : reader.liveEntries()) {
      F file = entry.file();
      boolean fileDelete = deletePaths.contains(file.path()) ||
          dropPartitions.contains(file.specId(), file.partition()) ||
          (isDelete && entry.sequenceNumber() > 0 && entry.sequenceNumber() < minSequenceNumber);
      if (fileDelete || deleteEvaluator.rowsMightMatch(file)) {
        ValidationException.check(
            fileDelete || deleteEvaluator.rowsMustMatch(file),
            "Cannot delete file where some, but not all, rows match filter %s: %s",
            this.deleteExpression, file.path());

        hasDeletedFiles = true;
        if (failAnyDelete) {
          throw new DeleteException(reader.spec().partitionToPath(file.partition()));
        }
        break; // as soon as a deleted file is detected, stop scanning
      }
    }
    return hasDeletedFiles;
  }

  @SuppressWarnings({"CollectionUndefinedEquality", "checkstyle:CyclomaticComplexity"})
  private ManifestFile filterManifestWithDeletedFiles(ExpressionEvaluator deleteEvaluator,
                                                      ManifestFile manifest, ManifestReader<F> reader) {
    boolean isDelete = reader.isDeleteManifestReader();
    // when this point is reached, there is at least one file that will be deleted in the
    // manifest. produce a copy of the manifest with all deleted files removed.
    List<F> deletedFiles = Lists.newArrayList();
    Set<CharSequenceWrapper> deletedPaths = Sets.newHashSet();

    try {
      ManifestWriter<F> writer = newManifestWriter(reader.spec());
      try {
        reader.entries().forEach(entry -> {
          F file = entry.file();
          boolean fileDelete = deletePaths.contains(file.path()) ||
              dropPartitions.contains(file.specId(), file.partition()) ||
              (isDelete && entry.sequenceNumber() > 0 && entry.sequenceNumber() < minSequenceNumber);
          if (entry.status() != ManifestEntry.Status.DELETED) {
            if (fileDelete || deleteEvaluator.rowsMightMatch(file)) {
              ValidationException.check(
                  fileDelete || deleteEvaluator.rowsMustMatch(file),
                  "Cannot delete file where some, but not all, rows match filter %s: %s",
                  this.deleteExpression, file.path());

              writer.delete(entry);

              CharSequenceWrapper wrapper = CharSequenceWrapper.wrap(entry.file().path());
              if (deletedPaths.contains(wrapper)) {
                LOG.warn("Deleting a duplicate path from manifest {}: {}",
                    manifest.path(), wrapper.get());
                duplicateDeleteCount += 1;
              } else {
                // only add the file to deletes if it is a new delete
                // this keeps the snapshot summary accurate for non-duplicate data
                deletedFiles.add(entry.file().copyWithoutStats());
              }
              deletedPaths.add(wrapper);

            } else {
              writer.existing(entry);
            }
          }
        });
      } finally {
        writer.close();
      }

      // return the filtered manifest as a reader
      ManifestFile filtered = writer.toManifestFile();

      // update caches
      filteredManifests.put(manifest, filtered);
      filteredManifestToDeletedFiles.put(filtered, deletedFiles);

      return filtered;

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest writer");
    }
  }

  // an evaluator that checks whether rows in a file may/must match a given expression
  // this class first partially evaluates the provided expression using the partition tuple
  // and then checks the remaining part of the expression using metrics evaluators
  private class ExpressionEvaluator {
    private final Schema tableSchema;
    private final ResidualEvaluator residualEvaluator;
    private final StructLikeMap<Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator>> metricsEvaluators;

    // TODO: support case sensitive flags
    ExpressionEvaluator(Schema tableSchema, PartitionSpec spec, Expression expr) {
      this.tableSchema = tableSchema;
      this.residualEvaluator = ResidualEvaluator.of(spec, expr, true);
      this.metricsEvaluators = StructLikeMap.create(spec.partitionType());
    }

    boolean rowsMightMatch(F file) {
      Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator> evaluators = metricsEvaluators(file);
      InclusiveMetricsEvaluator inclusiveMetricsEvaluator = evaluators.first();
      return inclusiveMetricsEvaluator.eval(file);
    }

    boolean rowsMustMatch(F file) {
      Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator> evaluators = metricsEvaluators(file);
      StrictMetricsEvaluator strictMetricsEvaluator = evaluators.second();
      return strictMetricsEvaluator.eval(file);
    }

    private Pair<InclusiveMetricsEvaluator, StrictMetricsEvaluator> metricsEvaluators(F file) {
      // this logic depends on ResidualEvaluator that behaves in the following way
      // if strict projection returns true -> the pred would return true -> replace the pred with true
      // if inclusive projection returns false -> the pred would return false -> replace the pred with false
      // otherwise, keep the original predicate and try evaluating it using metrics
      return metricsEvaluators.computeIfAbsent(file.partition(), partition -> {
        Expression residual = residualEvaluator.residualFor(partition);
        InclusiveMetricsEvaluator inclusive = new InclusiveMetricsEvaluator(tableSchema, residual);
        StrictMetricsEvaluator strict = new StrictMetricsEvaluator(tableSchema, residual);
        return Pair.of(inclusive, strict);
      });
    }
  }
}
