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

package com.mongodb.benchmark.framework;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;

public class MongoBenchmarkResultWriter implements BenchmarkResultWriter {

    private final MongoClient client;
    private final MongoCollection<BsonDocument> resultsCollection;

    public MongoBenchmarkResultWriter(final MongoClientURI uri, String databaseName) {
        client = new MongoClient(uri);
        resultsCollection = client.getDatabase(databaseName).getCollection("results", BsonDocument.class);
    }

    @Override
    public void write(final BenchmarkResult benchmarkResult) {

        resultsCollection.insertOne(new BsonDocument()
                .append("name", new BsonString(benchmarkResult.getName()))
                .append("megabytesPerIteration", new BsonDouble(getMegabytesPerIteration(benchmarkResult)))
                .append("elapsedTimes", getElapsedTimesInSeconds(benchmarkResult))
                .append("elapsedTimePercentiles", new BsonDocument()
                        .append("10", new BsonDouble(getElapsedTimeSecondsAtPercentile(benchmarkResult, 10)))
                        .append("25", new BsonDouble(getElapsedTimeSecondsAtPercentile(benchmarkResult, 25)))
                        .append("50", new BsonDouble(getElapsedTimeSecondsAtPercentile(benchmarkResult, 50)))
                        .append("75", new BsonDouble(getElapsedTimeSecondsAtPercentile(benchmarkResult, 75)))
                        .append("90", new BsonDouble(getElapsedTimeSecondsAtPercentile(benchmarkResult, 90)))
                        .append("95", new BsonDouble(getElapsedTimeSecondsAtPercentile(benchmarkResult, 95)))
                        .append("99", new BsonDouble(getElapsedTimeSecondsAtPercentile(benchmarkResult, 99))))
                .append("megabytesPerSecondPercentiles", new BsonDocument()
                        .append("10", new BsonDouble(getMegabytesPerSecondAtPercentile(benchmarkResult, 10)))
                        .append("25", new BsonDouble(getMegabytesPerSecondAtPercentile(benchmarkResult, 25)))
                        .append("50", new BsonDouble(getMegabytesPerSecondAtPercentile(benchmarkResult, 50)))
                        .append("75", new BsonDouble(getMegabytesPerSecondAtPercentile(benchmarkResult, 75)))
                        .append("90", new BsonDouble(getMegabytesPerSecondAtPercentile(benchmarkResult, 90)))
                        .append("95", new BsonDouble(getMegabytesPerSecondAtPercentile(benchmarkResult, 95)))
                        .append("99", new BsonDouble(getMegabytesPerSecondAtPercentile(benchmarkResult, 99)))));
    }

    private double getMegabytesPerIteration(final BenchmarkResult benchmarkResult) {
        return benchmarkResult.getBytesPerIteration() / 1000000.0;
    }

    private double getMegabytesPerSecondAtPercentile(final BenchmarkResult benchmarkResult, final int percentile) {
        return getMegabytesPerIteration(benchmarkResult) / getElapsedTimeSecondsAtPercentile(benchmarkResult, percentile);
    }

    private double getElapsedTimeSecondsAtPercentile(final BenchmarkResult benchmarkResult, int percentile) {
        return benchmarkResult.getElapsedTimeNanosAtPercentile(percentile) / 1000000000.0;
    }

    private BsonArray getElapsedTimesInSeconds(final BenchmarkResult benchmarkResult) {
        BsonArray elapsedTimesInSeconds = new BsonArray();

        for (Long elapsedTimeNanos : benchmarkResult.getElapsedTimeNanosList()) {
            elapsedTimesInSeconds.add(new BsonDouble(elapsedTimeNanos / 1000000000.0));
        }

        return elapsedTimesInSeconds;
    }

    @Override
    public void close() {
        client.close();
    }
}
