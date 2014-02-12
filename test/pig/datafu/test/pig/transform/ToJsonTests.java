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
package datafu.test.pig.transform;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

/**
 * The ToJson UDF converts a pig variable to a JSON string representation.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define ToJson datafu.pig.util.ToJson();
 *
 * -- input: {(foo, 12),(bar, 13),(xenu, 14)}
 * --
 * input1 = LOAD 'input' AS (B:bag{T:tuple(text:chararray, number:int)});
 *
 * -- output:(json:chararray);
 * --
 * -- ({"B":[{"text":"foo","number":12},{"text":"bar","number":13},{"text":"xenu","number":14}]})
 * outfoo = FOREACH input1 GENERATE ToJson(B) as json;
 * }
 * </pre>
 */
public class ToJsonTests extends PigTests
{
    /**
     register $JAR_PATH

     define ToJson datafu.pig.util.ToJson();

     data = LOAD 'input' AS (B: bag {T: tuple(text:chararray, number:int)});

     dump data;

     data2 = FOREACH data GENERATE ToJson(B) AS json;

     dump data2;

     STORE data2 INTO 'output';
     */
    @Multiline
    private String toJsonTestBag;

    @Test
    public void toJsonTestBag() throws Exception
    {
        PigTest test = createPigTestFromString(toJsonTestBag);

        writeLinesToFile("input",
                "{(foo, 12), (bar, 13), (xenu, 14)}",
                "{(,),(bar,),(,14)}");

        test.runScript();

        assertOutput(test, "data2",
                "({\"B\":[{\"text\":\"foo\",\"number\":12},{\"text\":\"bar\",\"number\":13},{\"text\":\"xenu\",\"number\":14}]})",
                "({\"B\":[{\"text\":\"\",\"number\":null},{\"text\":\"bar\",\"number\":null},{\"text\":\"\",\"number\":14}]})");
    }

    /**
     register $JAR_PATH

     define ToJson datafu.pig.util.ToJson();

     data = LOAD 'input' AS (T: tuple(text:chararray, number:int), text_field:chararray);

     dump data;

     data2 = FOREACH data GENERATE ToJson(T) AS json;

     dump data2;

     STORE data2 INTO 'output';
     */
    @Multiline
    private String toJsonTestTuple;

    @Test
    public void toJsonTestTuple() throws Exception
    {
        PigTest test = createPigTestFromString(toJsonTestTuple);

        writeLinesToFile("input",
                "(foo, 12)");

        test.runScript();

        assertOutput(test, "data2",
                "({\"T\":{\"text\":\"foo\",\"number\":12}})");
    }

    /**
     register $JAR_PATH

     define ToJson datafu.pig.util.ToJson();

     data = LOAD 'input' AS (text:chararray, number:int);

     dump data;

     data2 = FOREACH data GENERATE ToJson(text) AS json;

     dump data2;

     STORE data2 INTO 'output';
     */
    @Multiline
    private String toJsonTestField;

    @Test
    public void toJsonTestField() throws Exception
    {
        PigTest test = createPigTestFromString(toJsonTestField);

        writeLinesToFile("input",
                "(foo, 12)");

        test.runScript();

        assertOutput(test, "data2",
                "({\"text\":\"(foo, 12)\"})");
    }

    /**
     register $JAR_PATH

     define ToJson datafu.pig.util.ToJson();

     data = LOAD 'input' AS (T:tuple(dt:datetime, number:float));

     dump data;

     data2 = FOREACH data GENERATE ToJson(T) AS json;

     dump data2;

     STORE data2 INTO 'output';
     */
    @Multiline
    private String toJsonTestDateFloat;

    @Test
    public void toJsonTestDateFloat() throws Exception
    {
        PigTest test = createPigTestFromString(toJsonTestDateFloat);

        writeLinesToFile("input",
                "(2005-01-01T00:00:00.000Z, 12.0)");

        test.runScript();

        assertOutput(test, "data2",
                "({\"T\":{\"dt\":\"2005-01-01T00:00:00.000Z\",\"number\":12.0}})");
    }

    /**
     register $JAR_PATH

     define ToJson datafu.pig.util.ToJson();

     data = LOAD 'input' AS (text:chararray);

     dump data;

     data2 = FOREACH data GENERATE ToJson(text) AS json;

     dump data2;

     STORE data2 INTO 'output';
     */
    @Multiline
    private String toJsonTestOneField;

    @Test
    public void toJsonTestOneField() throws Exception
    {
        PigTest test = createPigTestFromString(toJsonTestOneField);

        writeLinesToFile("input", "foobar");

        test.runScript();

        assertOutput(test, "data2",
                "({\"text\":\"foobar\"})");
    }
}