/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
 *
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

package com.nvidia.spark.rapids;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;

class ParquetStream implements InputFile {

    DelegatingSeekableInputStream inputStream;
    FSDataInputStream fsInputStream;
    long fileLength;

    ParquetStream(FSDataInputStream in, long len) {
        fsInputStream = in;
        fileLength = len;
        inputStream =  new DelegatingSeekableInputStream(in) {
            @Override
            public void seek(long newPos) throws IOException {
                fsInputStream.seek(newPos);
            }

            @Override
            public long getPos() throws IOException {
                return fsInputStream.getPos();
            }

            @Override
            public void close() {
                // don't actually close because we want to reuse the input stream
            }
        };
    }

    @Override
    public long getLength() {
        return fileLength;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        // reuse the existing input stream and just seek around
        inputStream.seek(0);
        return inputStream;
    }

    public void close() throws IOException {
        inputStream.close();
    }
}
