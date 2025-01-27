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
package org.apache.asterix.cloud.lazy.accessor;

import java.io.FilenameFilter;
import java.util.Set;

import org.apache.asterix.cloud.CloudFileHandle;
import org.apache.asterix.cloud.bulk.IBulkOperationCallBack;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;

public interface ILazyAccessor {
    boolean isLocalAccessor();

    IBulkOperationCallBack getBulkOperationCallBack();

    void doOnOpen(CloudFileHandle fileHandle, IIOManager.FileReadWriteMode rwMode, IIOManager.FileSyncMode syncMode)
            throws HyracksDataException;

    Set<FileReference> doList(FileReference dir, FilenameFilter filter) throws HyracksDataException;

    boolean doExists(FileReference fileRef) throws HyracksDataException;

    long doGetSize(FileReference fileReference) throws HyracksDataException;

    byte[] doReadAllBytes(FileReference fileReference) throws HyracksDataException;

    void doDelete(FileReference fileReference) throws HyracksDataException;

    void doOverwrite(FileReference fileReference, byte[] bytes) throws HyracksDataException;
}
