/*
<<<<<<< HEAD
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
=======

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

>>>>>>> Initial commit for stats framework
 */
package org.apache.hyracks.storage.am.lsm.common.api;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IStatisticsManager {

    /**
     * Persists the statistics to the metadata page of the given disk component
     * before being immutable.
     * @param newComponent
     *             The new disk component to be persisted
     * @throws HyracksDataException
     */
    void persistComponentStatistics(ILSMDiskComponent newComponent) throws HyracksDataException;

    /**
     * Sends the statistics of the given disk components from their metadata pages
     * to the metadata node.
     * @param diskComponents
     *            The given disk components
     * @throws HyracksDataException
     */
    void sendDiskComponentsStatistics(List<ILSMDiskComponent> diskComponents) throws HyracksDataException;

    /**
     * Adds matter/ antimatter statistics of the qualified disk component
     * @param isAntimatter
     *             Is the statistics for antimatter tuples?
     * @param component
     *             The qualified disk component
     * @throws HyracksDataException
     */
    void addStatistics(ISynopsis synopsis, String dataverse, String dataset, String index, String field,
            boolean isAntimatter, ILSMDiskComponent component) throws HyracksDataException;
}