/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.extensions;

import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.spark.sql.AnalysisException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.PARQUET_VECTORIZATION_ENABLED;

public class TestCopyOnWriteDelete extends SparkRowLevelOperationsTestBase {

  public TestCopyOnWriteDelete(String catalogName, String implementation, Map<String, String> config,
                               String fileFormat, Boolean vectorized) {
    super(catalogName, implementation, config, fileFormat, vectorized);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS deletes");
  }

  @Test
  public void testDeleteFromEmptyTable() {
    createAndInitUnpartitionedTable();

    sql("DELETE FROM %s WHERE id IN (1)", tableName);
    sql("DELETE FROM %s WHERE dep = 'hr'", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    assertEquals("Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testExplain() {
    createAndInitUnpartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    sql("EXPLAIN DELETE FROM %s WHERE id <=> 1", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 1 snapshot", 1, Iterables.size(table.snapshots()));

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testDeleteWithAlias() {
    createAndInitUnpartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    sql("DELETE FROM %s AS t WHERE t.id IS NULL", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDeleteWithDynamicFileFiltering() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'hardware')", tableName);
    sql("INSERT INTO TABLE %s VALUES (null, 'hr')", tableName);

    sql("DELETE FROM %s WHERE id IS NOT NULL", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = table.currentSnapshot();
    validateSnapshot(currentSnapshot, "overwrite", "2", "2", null);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDeleteNonExistingRecords() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    sql("DELETE FROM %s AS t WHERE t.id > 10", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = table.currentSnapshot();
    validateSnapshot(currentSnapshot, "overwrite", "0", null, null);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testDeleteWithoutCondition() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'hardware')", tableName);
    sql("INSERT INTO TABLE %s VALUES (null, 'hr')", tableName);

    sql("DELETE FROM %s", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));

    // should be a delete instead of an overwrite as it is done through a metadata operation
    Snapshot currentSnapshot = table.currentSnapshot();
    validateSnapshot(currentSnapshot, "delete", "2", "3", null);

    assertEquals("Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testDeleteUsingMetadataWithComplexCondition() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'dep1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'dep2')", tableName);
    sql("INSERT INTO TABLE %s VALUES (null, 'dep3')", tableName);

    sql("DELETE FROM %s WHERE dep > 'dep2' OR dep = CAST(4 AS STRING) OR dep = 'dep2'", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));

    // should be a delete instead of an overwrite as it is done through a metadata operation
    Snapshot currentSnapshot = table.currentSnapshot();
    validateSnapshot(currentSnapshot, "delete", "2", "2", null);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "dep1")),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testDeleteWithArbitraryPartitionPredicates() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'hardware')", tableName);
    sql("INSERT INTO TABLE %s VALUES (null, 'hr')", tableName);

    // %% is an escaped version of %
    sql("DELETE FROM %s WHERE id = 10 OR dep LIKE '%%ware'", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 4 snapshots", 4, Iterables.size(table.snapshots()));

    // should be an overwrite since cannot be executed using a metadata operation
    Snapshot currentSnapshot = table.currentSnapshot();
    validateSnapshot(currentSnapshot, "overwrite", "1", "1", null);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testDeleteWithNonDeterministicCondition() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware')", tableName);

    AssertHelpers.assertThrows("Should complain about non-deterministic expressions",
        AnalysisException.class, "nondeterministic expressions are only allowed",
        () -> sql("DELETE FROM %s WHERE id = 1 AND rand() > 0.5", tableName));
  }

  @Test
  public void testDeleteWithFoldableConditions() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware')", tableName);

    // should keep all rows and don't trigger execution
    sql("DELETE FROM %s WHERE false", tableName);
    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // should keep all rows and don't trigger execution
    sql("DELETE FROM %s WHERE 50 <> 50", tableName);
    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // should keep all rows and don't trigger execution
    sql("DELETE FROM %s WHERE 1 > null", tableName);
    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // should remove all rows
    sql("DELETE FROM %s WHERE 21 = 21", tableName);
    assertEquals("Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 2 snapshots", 2, Iterables.size(table.snapshots()));
  }

  @Test
  public void testDeleteWithNullConditions() {
    createAndInitPartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (0, null), (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    // should keep all rows as null is never equal to null
    sql("DELETE FROM %s WHERE dep = null", tableName);
    assertEquals("Should have expected rows",
        ImmutableList.of(row(0, null), row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    // null = 'software' -> null
    // should delete using metadata operation only
    sql("DELETE FROM %s WHERE dep = 'software'", tableName);
    assertEquals("Should have expected rows",
        ImmutableList.of(row(0, null), row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    // should delete using metadata operation only
    sql("DELETE FROM %s WHERE dep <=> NULL", tableName);
    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "hr"), row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    Snapshot currentSnapshot = table.currentSnapshot();
    validateSnapshot(currentSnapshot, "delete", "1", "1", null);
  }

  @Test
  public void testDeleteWithInAndNotInConditions() {
    createAndInitUnpartitionedTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'hr'), (2, 'hardware'), (null, 'hr')", tableName);

    sql("DELETE FROM %s WHERE id IN (null, 1)", tableName);
    assertEquals("Should have expected rows",
        ImmutableList.of(row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));

    sql("DELETE FROM %s WHERE id NOT IN (null, 1)", tableName);
    assertEquals("Should have expected rows",
        ImmutableList.of(row(2, "hardware"), row(null, "hr")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  // TODO: multiple row groups, predicates on nested columns

  private void validateSnapshot(Snapshot snapshot, String operation, String changedPartitionCount,
                                String deletedDataFiles, String addedDataFiles) {
    Assert.assertEquals("Operation must match", operation, snapshot.operation());
    Assert.assertEquals("Changed partitions count must match",
        changedPartitionCount,
        snapshot.summary().get("changed-partition-count"));
    Assert.assertEquals("Deleted data files count must match",
        deletedDataFiles,
        snapshot.summary().get("deleted-data-files"));
    Assert.assertEquals("Added data files count must match",
        addedDataFiles,
        snapshot.summary().get("added-data-files"));
  }

  private void createAndInitPartitionedTable() {
    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg PARTITIONED BY (dep)", tableName);
    initTable();
  }

  private void createAndInitUnpartitionedTable() {
    sql("CREATE TABLE %s (id INT, dep STRING) USING iceberg", tableName);
    initTable();
  }

  private void initTable() {
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')", tableName, DEFAULT_FILE_FORMAT, fileFormat);

    switch (fileFormat) {
      case "parquet":
        sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%b')", tableName, PARQUET_VECTORIZATION_ENABLED, vectorized);
        break;
      case "orc":
        Assert.assertTrue(vectorized);
        break;
      case "avro":
        Assert.assertFalse(vectorized);
        break;
    }
  }
}
