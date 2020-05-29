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
package org.apache.asterix.external.input.record.reader.aws;

import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.input.stream.AbstractMultipleInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class AwsS3InputStream extends AbstractMultipleInputStream {

    // Configuration
    private final Map<String, String> configuration;

    private final S3Client s3Client;

    // File fields
    private final List<String> filePaths;
    private int nextFileIndex = 0;

    public AwsS3InputStream(Map<String, String> configuration, List<String> filePaths) throws HyracksDataException {
        this.configuration = configuration;
        this.filePaths = filePaths;
        this.s3Client = buildAwsS3Client(configuration);
    }

    @Override
    protected boolean advance() throws IOException {
        // No files to read for this partition
        if (filePaths == null || filePaths.isEmpty()) {
            return false;
        }

        // Finished reading all the files
        if (nextFileIndex >= filePaths.size()) {
            if (in != null) {
                CleanupUtils.close(in, null);
            }
            return false;
        }

        // Close the current stream before going to the next one
        if (in != null) {
            CleanupUtils.close(in, null);
        }

        String bucket = configuration.get(AwsS3.CONTAINER_NAME_FIELD_NAME);
        GetObjectRequest.Builder getObjectBuilder = GetObjectRequest.builder();
        GetObjectRequest getObjectRequest = getObjectBuilder.bucket(bucket).key(filePaths.get(nextFileIndex)).build();

        // Have a reference to the S3 stream to ensure that if GZipInputStream causes an IOException because of reading
        // the header, then the S3 stream gets closed in the close method
        try {
            in = s3Client.getObject(getObjectRequest);
        } catch (SdkException ex) {
            throw new RuntimeDataException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex.getMessage());
        }

        // Use gzip stream if needed
        String filename = filePaths.get(nextFileIndex).toLowerCase();
        if (filename.endsWith(".gz") || filename.endsWith(".gzip")) {
            in = new GZIPInputStream(s3Client.getObject(getObjectRequest), ExternalDataConstants.DEFAULT_BUFFER_SIZE);
        }

        // Current file ready, point to the next file
        nextFileIndex++;
        if (notificationHandler != null) {
            notificationHandler.notifyNewSource();
        }
        return true;
    }

    private S3Client buildAwsS3Client(Map<String, String> configuration) throws HyracksDataException {
        try {
            return ExternalDataUtils.AwsS3.buildAwsS3Client(configuration);
        } catch (CompilationException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            CleanupUtils.close(in, null);
        }
    }

    @Override
    public String getStreamName() {
        return getStreamNameAt(nextFileIndex - 1);
    }

    @Override
    public String getPreviousStreamName() {
        return getStreamNameAt(nextFileIndex - 2);
    }

    private String getStreamNameAt(int fileIndex) {
        return fileIndex < 0 || filePaths == null || filePaths.isEmpty() ? "" : filePaths.get(fileIndex);
    }
}
