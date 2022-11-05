/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;

class ParquetReusableInputFile implements InputFile, AutoCloseable {

    DelegatingSeekableInputStream inputStream;
    SeekableInputStream sInputStream;
    long fileLength = 0;
    long start = 0;
    long entireFileLength = 0;

    ParquetReusableInputFile(FSDataInputStream in, long start, long len, long entireFileLen) {
        this.fileLength = len;
        this.start = start;
        this.entireFileLength = entireFileLen;
        sInputStream = HadoopStreams.wrap(in);
        inputStream = new DelegatingSeekableInputStream(sInputStream) {
            @Override
            public void seek(long newPos) throws IOException {
                sInputStream.seek(newPos);
            }

            @Override
            public long getPos() throws IOException {
                return sInputStream.getPos();
            }

            @Override
            public void close() {
                // don't actually close because we want to reuse the input stream
            }
        };
    }

    @Override
    public long getLength() {
        return entireFileLength;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        // reuse the existing input stream and just seek around
        inputStream.seek(start);
        return inputStream;
    }

    public void close() throws IOException {
        sInputStream.close();
    }
}
