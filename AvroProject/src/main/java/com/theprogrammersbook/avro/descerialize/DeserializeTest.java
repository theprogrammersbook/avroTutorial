package com.theprogrammersbook.avro.descerialize;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import com.theprogrammersbook.avro.ThePerson;

public class DeserializeTest {
	public static void main(String[] args) {
		// Deserialize sample avro file
		try {
			DatumReader bdPersonDatumReader = new SpecificDatumReader(ThePerson.class);
			DataFileReader dataFileReader = new DataFileReader(new File("theperson-test.avro"),
					bdPersonDatumReader);
			ThePerson p = null;
			while (dataFileReader.hasNext()) {
				p = (ThePerson) dataFileReader.next(p);
				System.out.println(p);
			}
		} catch (IOException e) {
			System.out.println("Error reading Avro");
		}
	}
}
