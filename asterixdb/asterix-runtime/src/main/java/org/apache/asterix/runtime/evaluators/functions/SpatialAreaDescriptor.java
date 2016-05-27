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
package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.Coordinate;
import org.apache.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.common.SpatialUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class SpatialAreaDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new SpatialAreaDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IHyracksTaskContext ctx) throws AlgebricksException {
                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private final DataOutput out = resultStorage.getDataOutput();
                    private final IPointable argPtr = new VoidPointable();
                    private final IScalarEvaluator eval = args[0].createScalarEvaluator(ctx);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
                        resultStorage.reset();
                        eval.evaluate(tuple, argPtr);

                        try {
                            byte[] bytes = argPtr.getByteArray();
                            int offset = argPtr.getStartOffset();
                            ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
                            double area = 0.0;

                            switch (tag) {
                                case POLYGON:
                                    int numOfPoints = AInt16SerializerDeserializer.getShort(bytes, offset + 1);

                                    if (numOfPoints < 3) {
                                        throw new AlgebricksException(AsterixBuiltinFunctions.SPATIAL_AREA.getName()
                                                + ": polygon must have at least 3 points");
                                    }
                                    area = Math.abs(SpatialUtils.polygonArea(bytes, offset, numOfPoints));
                                    out.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                                    out.writeDouble(area);
                                    break;
                                case CIRCLE:
                                    double radius = ADoubleSerializerDeserializer.getDouble(bytes,
                                            offset + ACircleSerializerDeserializer.getRadiusOffset());
                                    area = SpatialUtils.pi() * radius * radius;
                                    out.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                                    out.writeDouble(area);
                                    break;
                                case RECTANGLE:
                                    double x1 = ADoubleSerializerDeserializer.getDouble(bytes,
                                            offset + ARectangleSerializerDeserializer
                                                    .getBottomLeftCoordinateOffset(Coordinate.X));
                                    double y1 = ADoubleSerializerDeserializer.getDouble(bytes,
                                            offset + ARectangleSerializerDeserializer
                                                    .getBottomLeftCoordinateOffset(Coordinate.Y));

                                    double x2 = ADoubleSerializerDeserializer.getDouble(bytes,
                                            offset + ARectangleSerializerDeserializer
                                                    .getUpperRightCoordinateOffset(Coordinate.X));
                                    double y2 = ADoubleSerializerDeserializer.getDouble(bytes,
                                            offset + ARectangleSerializerDeserializer
                                                    .getUpperRightCoordinateOffset(Coordinate.Y));
                                    area = (x2 - x1) * (y2 - y1);
                                    out.writeByte(ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG);
                                    out.writeDouble(area);
                                    break;
                                default:
                                    throw new NotImplementedException(AsterixBuiltinFunctions.SPATIAL_AREA.getName()
                                            + ": does not support the type: " + tag
                                            + "; it is only implemented for POLYGON, CIRCLE and RECTANGLE.");
                            }
                        } catch (HyracksDataException hde) {
                            throw new AlgebricksException(hde);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                        result.set(resultStorage);
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.SPATIAL_AREA;
    }

}
