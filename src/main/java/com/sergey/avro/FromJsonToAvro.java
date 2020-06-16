package com.sergey.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class FromJsonToAvro {

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        String json = "{\"username\":\"miguno\",\"tweet\":\"Rock: Nerf paper, scissors is fine.\",\"timestamp\": 1366150681 }";
        String schemaStr = "{ \"type\" : \"record\", \"name\" : \"twitter_schema\", \"namespace\" : \"com.miguno.avro\", \"fields\" : [ { \"name\" : \"username\", \"type\" : \"string\", \"doc\"  : \"Name of the user account on Twitter.com\" }, { \"name\" : \"tweet\", \"type\" : \"string\", \"doc\"  : \"The content of the user's Twitter message\" }, { \"name\" : \"timestamp\", \"type\" : \"long\", \"doc\"  : \"Unix epoch time in seconds\" } ], \"doc:\" : \"A basic schema for storing Twitter messages\" }";
        byte[] avroByteArray = fromJsonToAvro(json, schemaStr);
        String str = new String(avroByteArray, StandardCharsets.UTF_8);
        System.out.println(str);
    }

    /**
     * @param json
     * @param schemaStr
     * @throws Exception
     */
    static byte[] fromJsonToAvro(String json, String schemaStr) throws Exception {

        InputStream input = new ByteArrayInputStream(json.getBytes());
        DataInputStream din = new DataInputStream(input);

        Schema schema = Schema.parse(schemaStr);

        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        Object datum = reader.read(null, decoder);


        GenericDatumWriter<Object> w = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);

        w.write(datum, e);
        e.flush();

        return outputStream.toByteArray();
    }
}
