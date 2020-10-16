/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark.procedures;

import java.lang.invoke.MethodHandle;
import java.sql.Timestamp;
import java.util.Collections;
import org.apache.iceberg.spark.MethodHandleUtil;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.connector.catalog.ProcedureParameter;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ExpireSnapshotProcedure extends BaseProcedure {
  private static final MethodHandle METHOD_HANDLE = MethodHandleUtil.methodHandle(
      ExpireSnapshotProcedure.class,
      "expireSnapshot",
      String.class, String.class, Timestamp.class, Integer.class);

  private final ProcedureParameter[] parameters = new ProcedureParameter[] {
      ProcedureParameter.required("namespace", DataTypes.StringType),
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.optional("older_than", DataTypes.TimestampType, null),
      ProcedureParameter.optional("retain_last", DataTypes.IntegerType, null)
  };

  private final StructField[] outputFields = new StructField[] {
      new StructField("retain_last_num", DataTypes.IntegerType, true, Metadata.empty()),
      new StructField("expire_timestamp", DataTypes.TimestampType, true, Metadata.empty())
  };

  private final StructType outputType = new StructType(outputFields);
  private final MethodHandle methodHandle = METHOD_HANDLE.bindTo(this);

  public ExpireSnapshotProcedure(TableCatalog catalog) {
    super(catalog);
  }

  public Iterable<Row> expireSnapshot(String namespace, String tableName, Timestamp timestamp, Integer retainLastNum) {
    return modifyIcebergTable(namespace, tableName, table -> {
      long expireTime = timestamp != null ? timestamp.getTime() : System.currentTimeMillis();
      if (retainLastNum == null) {
        table.expireSnapshots()
                .expireOlderThan(expireTime)
                .commit();
      } else {
        table.expireSnapshots()
                .expireOlderThan(expireTime)
                .retainLast(retainLastNum)
                .commit();
      }
      Row outputRow = RowFactory.create(retainLastNum, timestamp);
      return Collections.singletonList(outputRow);
    });
  }

  @Override
  public ProcedureParameter[] parameters() {
    return parameters;
  }

  @Override
  public StructType outputType() {
    return outputType;
  }

  @Override
  public MethodHandle methodHandle() {
    return methodHandle;
  }
}
