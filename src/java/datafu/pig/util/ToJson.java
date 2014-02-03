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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.EvalFunc;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.apache.pig.builtin.OutputSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
//import java.math.BigDecimal;
//import java.math.BigInteger;
import java.util.Map;

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
@OutputSchema("json:chararray")
public class ToJson extends EvalFunc<String> {

    private static final int BUF_SIZE = 4 * 1024;

    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        Schema inputSchema = getInputSchema();
        if(inputSchema == null) {
            throw new IOException("Must supply non-null schema of field to ToJson!");
        }

        // Build a ByteArrayOutputStream to write the JSON into
        ByteArrayOutputStream baos = new ByteArrayOutputStream(BUF_SIZE);
        // Build the generator
        JsonGenerator json =
                new JsonFactory().createJsonGenerator(baos, JsonEncoding.UTF8);

        // Write the beginning of the top level tuple object
        json.writeStartObject();

        ResourceSchema.ResourceFieldSchema[] fields = new ResourceSchema(inputSchema).getFields();
        for (int i = 0; i < fields.length; i++) {
            JsonSerializer.writeField(json, fields[i], input.get(i));
        }
        json.writeEndObject();
        json.close();

        return baos.toString();
    }

    // Pulled from https://issues.apache.org/jira/browse/PIG-2641
    public static class JsonSerializer {
        @SuppressWarnings("unchecked")
        public static void writeField(JsonGenerator json,
                                      ResourceSchema.ResourceFieldSchema field,
                                      Object d) throws IOException {

            // If the field is missing or the value is null, write a null
            if (d == null) {
                json.writeNullField(field.getName());
                return;
            }

            // Based on the field's type, write it out
            switch (field.getType()) {
                case DataType.BOOLEAN:
                    json.writeBooleanField(field.getName(), (Boolean)d);
                    return;

                case DataType.INTEGER:
                    json.writeNumberField(field.getName(), (Integer)d);
                    return;

                case DataType.LONG:
                    json.writeNumberField(field.getName(), (Long)d);
                    return;

                case DataType.FLOAT:
                    json.writeNumberField(field.getName(), (Float)d);
                    return;

                case DataType.DOUBLE:
                    json.writeNumberField(field.getName(), (Double)d);
                    return;

                case DataType.DATETIME:
                    json.writeStringField(field.getName(), d.toString());
                    return;

                case DataType.BYTEARRAY:
                    json.writeStringField(field.getName(), d.toString());
                    return;

                case DataType.CHARARRAY:
                    json.writeStringField(field.getName(), (String)d);
                    return;

//                case DataType.BIGINTEGER:
//                    //Since Jackson doesnt have a writeNumberField for BigInteger we
//                    //have to do it manually here.
//                    json.writeFieldName(field.getName());
//                    json.writeNumber((BigInteger)d);
//                    return;
//
//                case DataType.BIGDECIMAL:
//                    json.writeNumberField(field.getName(), (BigDecimal)d);
//                    return;

                case DataType.MAP:
                    json.writeFieldName(field.getName());
                    json.writeStartObject();
                    for (Map.Entry<String, Object> e : ((Map<String, Object>)d).entrySet()) {
                        json.writeStringField(e.getKey(), e.getValue() == null ? null : e.getValue().toString());
                    }
                    json.writeEndObject();
                    return;

                case DataType.TUPLE:
                    json.writeFieldName(field.getName());
                    json.writeStartObject();

                    ResourceSchema s = field.getSchema();
                    if (s == null) {
                        throw new IOException("Schemas must be fully specified to use "
                                + "this storage function.  No schema found for field " +
                                field.getName());
                    }
                    ResourceSchema.ResourceFieldSchema[] fs = s.getFields();

                    for (int j = 0; j < fs.length; j++) {
                        writeField(json, fs[j], ((Tuple)d).get(j));
                    }
                    json.writeEndObject();
                    return;

                case DataType.BAG:
                    json.writeFieldName(field.getName());
                    json.writeStartArray();
                    s = field.getSchema();
                    if (s == null) {
                        throw new IOException("Schemas must be fully specified to use "
                                + "this storage function.  No schema found for field " +
                                field.getName());
                    }
                    fs = s.getFields();
                    if (fs.length != 1 || fs[0].getType() != DataType.TUPLE) {
                        throw new IOException("Found a bag without a tuple "
                                + "inside!");
                    }
                    // Drill down the next level to the tuple's schema.
                    s = fs[0].getSchema();
                    if (s == null) {
                        throw new IOException("Schemas must be fully specified to use "
                                + "this storage function.  No schema found for field " +
                                field.getName());
                    }
                    fs = s.getFields();
                    for (Tuple t : (DataBag)d) {
                        json.writeStartObject();
                        for (int j = 0; j < fs.length; j++) {
                            writeField(json, fs[j], t.get(j));
                        }
                        json.writeEndObject();
                    }
                    json.writeEndArray();
                    return;
            }
        }
    }
}