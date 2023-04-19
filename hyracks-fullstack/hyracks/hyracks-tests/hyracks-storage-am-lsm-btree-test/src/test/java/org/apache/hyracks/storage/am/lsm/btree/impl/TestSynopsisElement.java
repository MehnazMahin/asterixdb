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
package org.apache.hyracks.storage.am.lsm.btree.impl;

import org.apache.hyracks.storage.am.lsm.common.api.ISynopsisElement;

public class TestSynopsisElement implements ISynopsisElement<Comparable>, Comparable<TestSynopsisElement> {

    public TestSynopsisElement(Comparable leftKey, Comparable rightKey, double value) {
        this.leftKey = leftKey;
        this.rightKey = rightKey;
        this.value = value;
    }

    private Comparable leftKey;
    private Comparable rightKey;
    private double value;

    @Override
    public Comparable getLeftKey() {
        return leftKey;
    }

    @Override
    public Comparable getRightKey() {
        return rightKey;
    }

    @Override
    public double getValue() {
        return value;
    }

    public void setValue(double newValue) {
        value = newValue;
    }

    @Override
    public String toString() {
        return "TestSynopsisElement{" + "leftKey=" + leftKey + ", rightKey=" + rightKey + ", value=" + value + '}';
    }

    @Override
    public int compareTo(TestSynopsisElement o) {
        if (leftKey != o.leftKey) {
            return leftKey.compareTo(o.getLeftKey());
        }
        return rightKey.compareTo(o.getRightKey());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TestSynopsisElement that = (TestSynopsisElement) o;

        return (leftKey != null && rightKey != null) ? (leftKey.equals(that.leftKey) && rightKey.equals(that.rightKey))
                : (that.leftKey == null || that.rightKey == null);
    }
}
