/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.benchmark.benchmarks;

import com.mongodb.benchmark.framework.BenchmarkResult;
import com.mongodb.benchmark.framework.BenchmarkRunner;
import com.mongodb.benchmark.framework.TextBasedBenchmarkResultWriter;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GridFSExportBenchmark extends AbstractMongoBenchmark {
    private MongoDatabase database;
    private GridFSBucket bucket;

    private ExecutorService gridFSService;
    private ExecutorService fileService;

    private File tempDirectory;

    @Override
    public String getName() {
        return "GridFS multi-file upload";
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        database = client.getDatabase("perftest");
        bucket = GridFSBuckets.create(database);

        database.drop();

        gridFSService = Executors.newFixedThreadPool(16);
        fileService = Executors.newFixedThreadPool(1);

        importFiles();
    }

    @Override
    public void tearDown() throws Exception {
        gridFSService.shutdown();
        gridFSService.awaitTermination(1, TimeUnit.MINUTES);
        fileService.shutdown();
        fileService.awaitTermination(1, TimeUnit.MINUTES);

        super.tearDown();
    }

    @Override
    public void before() throws Exception {
        super.before();

        tempDirectory = Files.createTempDirectory("GRIDFS_MULTI").toFile();
    }

    @Override
    public void after() throws Exception {
        for (File file : tempDirectory.listFiles()) {
            file.delete();
        }

        tempDirectory.delete();

        super.after();
    }

    @Override
    public void run() throws Exception {

        CountDownLatch latch = new CountDownLatch(50);

        for (int i = 0; i < 50; i++) {
            gridFSService.submit(exportFile(latch, i));
        }

        latch.await(1, TimeUnit.MINUTES);
    }

    private Runnable exportFile(final CountDownLatch latch, final int fileId) {
        return new Runnable() {
            @Override
            public void run() {
                final UnsafeByteArrayOutputStream outputStream = new UnsafeByteArrayOutputStream(5242880);
                bucket.downloadToStreamByName(GridFSExportBenchmark.this.getFileName(fileId), outputStream);
                fileService.submit((Runnable) new Runnable() {
                    @Override
                    public void run() {
                        try {
                            FileOutputStream fos = new FileOutputStream(new File(tempDirectory, String.format("%d", fileId) + ".txt"));
                            fos.write(outputStream.getByteArray());
                            fos.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        latch.countDown();
                    }
                });
            }
        };
    }

    private void importFiles() throws Exception {
        CountDownLatch latch = new CountDownLatch(50);

        for (int i = 0; i < 50; i++) {
            fileService.submit(importFile(latch, i));
        }

        latch.await(1, TimeUnit.MINUTES);
    }

    private Runnable importFile(final CountDownLatch latch, final int fileId) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    String fileName = GridFSExportBenchmark.this.getFileName(fileId);
                    String resourcePath = "/benchmarks/parallel/GRIDFS_MULTI/" + fileName;
                    bucket.uploadFromStream(fileName, GridFSExportBenchmark.this.getClass().getResource(resourcePath).openStream());
                    latch.countDown();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private String getFileName(final int fileId) {
        return "file" + String.format("%d", fileId) + ".txt";
    }

    @Override
    public int getBytesPerRun() {
        return 262144000;
    }

    public static void main(String[] args) throws Exception {
        BenchmarkResult benchmarkResult = new BenchmarkRunner(new GridFSExportBenchmark(), 20, 100).run();
        new TextBasedBenchmarkResultWriter(System.out, false, true).write(benchmarkResult);
    }

}
