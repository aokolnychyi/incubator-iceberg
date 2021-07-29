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

package org.apache.iceberg.io;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

public class V2PartitionAwareRow<T> {
  private T row;
  private PartitionSpec spec;
  private StructLike partition;

  public V2PartitionAwareRow<T> wrap(T newRow, PartitionSpec newSpec, StructLike newPartition) {
    this.row = newRow;
    this.spec = newSpec;
    this.partition = newPartition;
    return this;
  }

  public T row() {
    return row;
  }

  public PartitionSpec spec() {
    return spec;
  }

  public StructLike partition() {
    return partition;
  }
}
