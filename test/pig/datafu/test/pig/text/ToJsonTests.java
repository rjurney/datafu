/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package datafu.test.pig.text;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;


public class ToJsonTests extends PigTests
{
    /**
     register $JAR_PATH

     define ToJson datafu.pig.text.ToJson();

     data = LOAD 'input' AS (B: bag {T: tuple(v:INT)});

     dump data;

     data2 = FOREACH data GENERATE ToJson(B) AS json;

     dump data2;

     STORE data2 INTO 'output';
     */
    @Multiline
    private String toJsonTest;

    @Test
    public void toJsonTest() throws Exception
    {
        PigTest test = createPigTestFromString(toJsonTest);

        writeLinesToFile("input",
                "{(1),(2),(3),(4),(5)}",
                "{(4),(5)}");

        test.runScript();

        assertOutput(test, "data2",
                "({\"B\":[{\"v\":1},{\"v\":2},{\"v\":3},{\"v\":4},{\"v\":5}]})",
                "({\"B\":[{\"v\":4},{\"v\":5}]})");
    }
}
