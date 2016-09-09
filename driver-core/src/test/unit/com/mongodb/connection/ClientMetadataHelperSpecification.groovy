/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.connection

import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.EncoderContext
import org.bson.io.BasicOutputBuffer
import spock.lang.Specification

import static com.mongodb.connection.ClientMetadataHelper.createClientMetadataDocument

class ClientMetadataHelperSpecification extends Specification {

    def 'applicationName can be 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 126 + '\u00A0'

        when:
        def clientMetadataDocument = createClientMetadataDocument(applicationName)

        then:
        clientMetadataDocument.getDocument('application').getString('name').getValue() == applicationName
    }

    def 'should throw IllegalArgumentException if applicationName exceeds 128 bytes when encoded as UTF-8'() {
        given:
        def applicationName = 'a' * 127 + '\u00A0'

        when:
        createClientMetadataDocument(applicationName)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should create client metadata document'() {
        expect:
        createClientMetadataDocument(applicationName) == createExpectedClientMetadataDocument(applicationName)

        where:
        applicationName << ['appName', null]
    }

    def 'should create client metadata document with the correct fields removed to fit within 512 byte limit '() {
        when:
        def clientMetadataDocument = createClientMetadataDocument(null, templateDocument)

        then:
        clientMetadataDocument == expectedDocument

        where:
        [templateDocument, expectedDocument] << [
                [
                        new BsonDocument('os',
                                new BsonDocument('type', new BsonString('unknown'))
                                        .append('name', new BsonString('a' * 512))
                                        .append('architecture', new BsonString('arch1'))
                                        .append('version', new BsonString('1.0')))
                                .append('platform', new BsonString('platform1')),

                        new BsonDocument('os', new BsonDocument('type', new BsonString('unknown')))
                                .append('platform', new BsonString('platform1'))
                ],

                [
                        new BsonDocument('os',
                                new BsonDocument('type', new BsonString('unknown'))
                                        .append('name', new BsonString('name1'))
                                        .append('architecture', new BsonString('arch1'))
                                        .append('version', new BsonString('1.0')))
                                .append('platform', new BsonString('a' * 512)),

                        new BsonDocument('os', new BsonDocument('type', new BsonString('unknown')))
                ],
                [
                        new BsonDocument('driver', new BsonDocument('name', new BsonString('mongo-java-driver')))
                                .append('version', new BsonString('a' * 512))
                                .append('platform', new BsonString('platform1')),

                        null
                ]
        ]
    }

    def encode(final BsonDocument document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
        buffer.toByteArray()
    }

    static BsonDocument createExpectedClientMetadataDocument(String appName) {
        def expectedDriverDocument = new BsonDocument('name', new BsonString('mongo-java-driver'))
                .append('version', new BsonString(getDriverVersion()))
        def expectedOperatingSystemDocument = new BsonDocument('type', new BsonString('unknown'))
                .append('name', new BsonString(System.getProperty('os.name')))
                .append('architecture', new BsonString(System.getProperty('os.arch')))
                .append('version', new BsonString(System.getProperty('os.version')))
        def expectedClientDocument = new BsonDocument('driver', expectedDriverDocument)
                .append('os', expectedOperatingSystemDocument)
                .append('platform', new BsonString('Java/' + System.getProperty('java.vendor') + '/'
                + System.getProperty('java.runtime.version')))
        if (appName != null) {
            expectedClientDocument.append('application', new BsonDocument('name', new BsonString('appName')))
        }
        expectedClientDocument
    }

    // not sure how else to test this.  It's really a test of the build system that generates the version.properties file
    private static String getDriverVersion() {
        String driverVersion = 'unknown';
        Class<InternalStreamConnectionInitializer> clazz = InternalStreamConnectionInitializer;
        URL versionPropertiesFileURL = clazz.getResource('/version.properties');
        if (versionPropertiesFileURL != null) {
            Properties versionProperties = new Properties();
            InputStream versionPropertiesInputStream = versionPropertiesFileURL.openStream();
            versionProperties.load(versionPropertiesInputStream);
            driverVersion = versionProperties.getProperty('version');
        }
        driverVersion;
    }
}
