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

import com.mongodb.BasicDBObject;
import com.mongodb.benchmark.framework.Benchmark;
import com.mongodb.benchmark.framework.BenchmarkResult;
import com.mongodb.benchmark.framework.BenchmarkResultWriter;
import com.mongodb.benchmark.framework.BenchmarkRunner;
import com.mongodb.benchmark.framework.MinimalTextBasedBenchmarkResultWriter;
import com.mongodb.benchmark.framework.TextBasedBenchmarkResultWriter;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.MongoClient.getDefaultCodecRegistry;

@SuppressWarnings({"rawtypes", "unchecked"})
public class BenchmarkSuite {

    public static final int NUM_WARMUP_ITERATIONS = 10;
    public static final int NUM_ITERATIONS = 100;

    private static final List<BenchmarkResultWriter> WRITERS = Arrays.<BenchmarkResultWriter>asList(
            new TextBasedBenchmarkResultWriter(System.out, Arrays.asList(50), false, false)
            //,  new MinimalTextBasedBenchmarkResultWriter(System.out)
            //, new MongoBenchmarkResultWriter(new MongoClientURI("mongodb://localhost"), "perf-test-results")
    );

    public static void main(String[] args) throws Exception {
        Class clazz;
        if (args.length > 0) {
            clazz = Class.forName(args[0]);
        } else {
            clazz = Document.class;
        }

	System.out.println("Document class: " + clazz.getName());
	System.out.println();
	
        runBenchmarks(clazz, getDefaultCodecRegistry().get(clazz), createIdRemover(clazz));
    }

    private static <T extends Bson> void runBenchmarks(final Class<T> clazz, final Codec<T> codec, final IdRemover<T> idRemover)
            throws Exception {

//        runBenchmark(new BsonEncodeDocumentBenchmark<T>("Flat", "/benchmarks/flat_bson.json", codec));
//        runBenchmark(new BsonEncodeDocumentBenchmark<T>("Deep", "/benchmarks/deep_bson.json", codec));
//        runBenchmark(new BsonEncodeDocumentBenchmark<T>("Full", "/benchmarks/full_bson.json", codec));
//
//        runBenchmark(new BsonDecodeDocumentBenchmark<T>("Flat", "/benchmarks/flat_bson.json", codec));
//        runBenchmark(new BsonDecodeDocumentBenchmark<T>("Deep", "/benchmarks/deep_bson.json", codec));
//        runBenchmark(new BsonDecodeDocumentBenchmark<T>("Full", "/benchmarks/full_bson.json", codec));
//
//        runBenchmark(new RunCommandBenchmark<T>(codec));
        runBenchmark(new FindOneBenchmark<T>("/benchmarks/TWEET.json", clazz));
//        runBenchmark(new InsertOneBenchmark<T>("Small", "/benchmarks/SMALL_DOC.json", 10000, clazz, idRemover));
//        runBenchmark(new InsertOneBenchmark<T>("Large", "/benchmarks/LARGE_DOC.json", 10, clazz, idRemover));

        runBenchmark(new FindManyBenchmark<T>("/benchmarks/TWEET.json", clazz));
//        runBenchmark(new InsertManyBenchmark<T>("Small", "/benchmarks/SMALL_DOC.json", 10000, clazz));
//        runBenchmark(new InsertManyBenchmark<T>("Large", "/benchmarks/LARGE_DOC.json", 10, clazz));
//
//        runBenchmark(new GridFSUploadBenchmark("/benchmarks/GRIDFS_LARGE"));
//        runBenchmark(new GridFSDownloadBenchmark("/benchmarks/GRIDFS_LARGE"));
//
//        runBenchmark(new ImportBenchmark());
//        runBenchmark(new ExportBenchmark());
//        runBenchmark(new GridFSImportBenchmark());
//        runBenchmark(new GridFSExportBenchmark());
    }

    private static void runBenchmark(final Benchmark benchmark) throws Exception {
        BenchmarkResult benchmarkResult = new BenchmarkRunner(benchmark, NUM_WARMUP_ITERATIONS, NUM_ITERATIONS, 60, 300).run();

        for (BenchmarkResultWriter writer : WRITERS) {
            writer.write(benchmarkResult);
        }
    }

    private static <T> IdRemover<T> createIdRemover(Class<T> clazz) {
        if (clazz.equals(BsonDocument.class)) {
            return (IdRemover<T>) new IdRemover<BsonDocument>() {
                public void removeId(final BsonDocument document) {
                    document.remove("_id");
                }
            };
        }

        if (clazz.equals(Document.class)) {
            return (IdRemover<T>) new IdRemover<Document>() {
                public void removeId(final Document document) {
                    document.remove("_id");
                }
            };
        }

        if (clazz.equals(BasicDBObject.class)) {
            return (IdRemover<T>) new IdRemover<BasicDBObject>() {
                public void removeId(final BasicDBObject document) {
                    document.remove("_id");
                }
            };
        }

        throw new UnsupportedOperationException("Pick a different class");
    }
}
