package com.sergey.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SerializeAndDeserializeAvro {

    public static void main(String[] args) {

        BdPerson person1 = new BdPerson();
        person1.setId(1);
        person1.setUsername("mrscarter");
        person1.setFirstName("Beyonce");
        person1.setLastName("Knowles-Carter");
        person1.setBirthdate("1981-09-04");
        person1.setJoinDate("2016-01-01");
        person1.setPreviousLogins(10000);

        BdPerson person2 = new BdPerson();
        person2.setId(2);
        person2.setUsername("jayz");
        person2.setFirstName("Shawn");
        person2.setMiddleName("Corey");
        person2.setLastName("Carter");
        person2.setBirthdate("1969-12-04");
        person2.setJoinDate("2016-01-01");
        person2.setPreviousLogins(20000);

        //Serialize sample BdPerson
        File avroOutput = new File("bdperson-test.avro");
        try {
            DatumWriter<BdPerson> bdPersonDatumWriter = new SpecificDatumWriter<>(BdPerson.class);
            DataFileWriter<BdPerson> dataFileWriter = new DataFileWriter<>(bdPersonDatumWriter);
            dataFileWriter.create(person1.getSchema(), avroOutput);
            dataFileWriter.append(person1);
            dataFileWriter.append(person2);
            dataFileWriter.close();
        } catch (IOException e) {
            System.out.println("Error writing Avro");
        }


        //Deserialize sample avro file
        try {
            DatumReader<BdPerson> bdPersonDatumReader = new SpecificDatumReader(BdPerson.class);
            DataFileReader<BdPerson> dataFileReader = new DataFileReader<>(avroOutput, bdPersonDatumReader);
            BdPerson bdPerson = null;
            while (dataFileReader.hasNext()) {
                bdPerson = dataFileReader.next(bdPerson);
                System.out.println(bdPerson);
            }
        } catch (IOException e) {
            System.out.println("Error reading Avro");
        }
    }


}
