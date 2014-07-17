/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
 */

package com.mongodb;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommandResultTest {

    @Test
    public void shouldBeOkWhenOkFieldIsTrue() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new org.mongodb.CommandResult(new org.mongodb.connection.ServerAddress(),
                                                                                      new BsonDocument(), 0));
        commandResult.put("ok", true);
        assertTrue(commandResult.ok());
    }

    @Test
    public void shouldNotBeOkWithNoOkField() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new org.mongodb.CommandResult(new org.mongodb.connection.ServerAddress(),
                                                                                      new BsonDocument(), 0));
        assertFalse(commandResult.ok());
    }

    @Test
    public void shouldNotBeOkWhenOkFieldIsFalse() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new org.mongodb.CommandResult(new org.mongodb.connection.ServerAddress(),
                                                                                      new BsonDocument(), 0));
        commandResult.put("ok", false);
        assertFalse(commandResult.ok());
    }

    @Test
    public void shouldBeOkWhenOkFieldIsOne() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new org.mongodb.CommandResult(new org.mongodb.connection.ServerAddress(),
                                                                                      new BsonDocument(), 0));
        commandResult.put("ok", 1.0);
        assertTrue(commandResult.ok());
    }

    @Test
    public void shouldNotBeOkWhenOkFieldIsZero() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new org.mongodb.CommandResult(new org.mongodb.connection.ServerAddress(),
                                                                                      new BsonDocument(), 0));
        commandResult.put("ok", 0.0);
        assertFalse(commandResult.ok());
    }

    @Test
    public void shouldNotHaveExceptionWhenOkIsTrue() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new org.mongodb.CommandResult(new org.mongodb.connection.ServerAddress(),
                                                                                      new BsonDocument(), 0));
        commandResult.put("ok", true);
        assertNull(commandResult.getException());
    }

    @Test
    public void okShouldThrowWhenOkFieldTypeIsNotBooleanOrNumber() throws UnknownHostException {
        CommandResult commandResult = new CommandResult(new org.mongodb.CommandResult(new org.mongodb.connection.ServerAddress(),
                                                                                      new BsonDocument(), 0));
        commandResult.put("ok", "1");
        assertFalse(commandResult.ok());
    }

    @Test
    public void testNullErrorCode() throws UnknownHostException {
        org.mongodb.CommandResult wrapped = new org.mongodb.CommandResult(new org.mongodb.connection.ServerAddress(),
                                                                          new BsonDocument("ok", new BsonInt32(0)), 0);
        try {
            new CommandResult(wrapped).throwOnError();
            fail("Should throw");
        } catch (CommandFailureException e) {
            assertEquals(-1, e.getCode());
        }
    }

    @Test
    public void testCommandFailure() throws UnknownHostException {
        org.mongodb.CommandResult wrapped = new org.mongodb.CommandResult(new org.mongodb.connection.ServerAddress(),
                                                                          new BsonDocument("ok", new BsonInt32(0))
                                                                          .append("errmsg", new BsonString("ns not found"))
                                                                          .append("code", new BsonInt32(5000)), 0);

        try {
            new CommandResult(wrapped).throwOnError();
            fail("Should throw");
        } catch (CommandFailureException e) {
            assertEquals(5000, e.getCode());
        }
    }
}
