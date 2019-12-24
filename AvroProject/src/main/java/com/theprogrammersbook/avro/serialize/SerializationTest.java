package com.theprogrammersbook.avro.serialize;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import com.theprogrammersbook.avro.ThePerson;

public class SerializationTest {
	public static void main(String[] args) {

		ThePerson p1 = new ThePerson();
		p1.setId(1);
		p1.setUsername("mrscarter");
		p1.setFirstName("Beyonce");
		p1.setLastName("Knowles-Carter");
		p1.setBirthdate("1981-09-04");
		p1.setJoinDate("2016-01-01");
		p1.setPreviousLogins(10000);

		ThePerson p2 = new ThePerson();
		p2.setId(2);
		p2.setUsername("jayz");
		p2.setFirstName("Shawn");
		p2.setMiddleName("Corey");
		p2.setLastName("Carter");
		p2.setBirthdate("1969-12-04");
		p2.setJoinDate("2016-01-01");
		p2.setPreviousLogins(20000);
		// Serialize sample BdPerson
		File avroOutput = new File("theperson-test.avro");
		try {
			DatumWriter<ThePerson> bdPersonDatumWriter = new SpecificDatumWriter<ThePerson>(ThePerson.class);
			DataFileWriter<ThePerson> dataFileWriter = new DataFileWriter<ThePerson>(bdPersonDatumWriter);
			dataFileWriter.create(p1.getSchema(), avroOutput);
			dataFileWriter.append(p1);
			dataFileWriter.append(p2);
			dataFileWriter.close();
			System.out.println("Writing Complleted.....");
		} catch (IOException e) {
			System.out.println("Error writing Avro");
		}
	}
}
