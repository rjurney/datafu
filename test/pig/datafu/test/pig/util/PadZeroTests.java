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
package datafu.test.pig.util;

import datafu.test.pig.PigTests;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

public class PadZeroTests extends PigTests {

    /**
     register $JAR_PATH

     define PadZero datafu.pig.util.PadZero();

     data = LOAD 'input' AS (value:int);

     dump data;

     data2 = FOREACH data GENERATE PadZero(value) AS padded;

     dump data2

     STORE data2 INTO 'output';
     */
    @Multiline
    private static String inIntTest;

    @Test
    public void inIntTest() throws Exception
    {
        PigTest test = createPigTestFromString(inIntTest);

        writeLinesToFile("input",
                         "0",
                         "5",
                         "19");

        test.runScript();

        assertOutput(test, "data2",
                "(00)",
                "(05)",
                "(19)");
    }
}
