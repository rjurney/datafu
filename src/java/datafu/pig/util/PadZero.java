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
package datafu.pig.util;

/**
 * Pads integer values < 10 with 0s, as needed in ISO8601 DateTime manipulations.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define PadZero datafu.pig.util.PadZero();
 *
 * -- input:
 * ("2014-01-01T01:00:00.000Z")
 * input = LOAD 'input' AS (date_time:chararray);
 * -- input2:
 * (2014, 1, 1, 1)
 * input2 = FOREACH input GENERATE GetYear(date_time) as year, GetMonth(date_time) as month, GetDay(date_time) as day, GetHour(date_time) as hour;
 *
 * -- output:
 * -- ("2014-01-01T01:00:00.000Z")
 * output = FOREACH input GENERATE ToDate(StringConcat(year, '-', PadZero(month), '-', PadZero(day), 'T', PadZero(hour), ':00:00.000Z')) as date_time;
 * }
 * </pre>
 */


public class PadZero extends SimpleEvalFunc<Boolean> {
    public String call(Integer val)
    {
        String padded = val.toString();
        if(val < 10 || padded.length() == 1) {
            padded = "0" + val.toString();
        }
        return padded;
    }
}
