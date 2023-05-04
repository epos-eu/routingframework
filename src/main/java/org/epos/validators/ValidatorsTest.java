/*******************************************************************************
 * Copyright 2021 valerio
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package org.epos.validators;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidatorsTest {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ValidatorsTest.class);
	
	public static ArrayList<String> mimetypesBinary = new ArrayList<>();
	static {
		mimetypesBinary.add("application/octet-stream");
		mimetypesBinary.add("application/vnd.fdsn.mseed");
	}
	
	public static ArrayList<String> mimetypesWMS = new ArrayList<>();
	static {
		mimetypesBinary.add("image/png");
	}

	public static void main(String[] args) {
		String test1 = "https://www.emidius.eu/services/macroseismic/query?starttime=1890-01-01T00:00:00&endtime=1899-12-31T23:59:59&minlatitude=33.0000&maxlatitude=74.0000&minlongitude=-34.0000&maxlongitude=34.0000&minmagnitude=6.5&format=xml&orderby=time&includeallorigins=false&includeallmagnitudes=false&includeallmdpsets=false&includemdps=true&limit=300&nodata=204";
		String test2 = "https://tcs.ah-epos.eu/api/epos/episode-elements?episode=OK&dataType=Catalog&before=2018-09-14&after=2000-10-31";
		String test3 = "https://tcs.ah-epos.eu/api/epos/episodes/GRONINGEN_FIELD";

		ArrayList<String>tests = new ArrayList<>();
		tests.add(test1);
		tests.add(test2);
		tests.add(test3);

		for(String test : tests) {
			LOGGER.info("Check: " + test);
			String[] response = TCSResponse(test);

			if (response[1].contains("json")) {
				LOGGER.info("Validating using wp09");
				try {
					if (JSONValidator.isJsonValid(new File("wp09-radon-schema.json"), response[0])) {
						LOGGER.info("Is validated with wp09-radon-schema.json");
					}

					LOGGER.info("Validating using wp10");
					if (JSONValidator.isJsonValid(new File("wp10-station-schema.json"), response[0])) {
						LOGGER.info("Is validated with wp10-station-schema.json");
					}
					LOGGER.info("Validating using wp11");
					if (JSONValidator.isJsonValid(new File("wp11-schema.json"), response[0])) {
						LOGGER.info("Is validated with wp11-schema.json");
					}
					LOGGER.info("Validating using wp12");
					if (JSONValidator.isJsonValid(new File("wp12-schema.json"), response[0])) {
						LOGGER.info("Is validated with wp12-schema.json");
					}
					LOGGER.info("Validating using wp14 episodes");
					if (JSONValidator.isJsonValid(new File("wp14-schema.json"), response[0])) {
						LOGGER.info("Is validated with wp14-schema.json");
					}
					LOGGER.info("Validating using wp14 episodes elements");
					if (JSONValidator.isJsonValid(new File("wp14-episodes-elements.json"), response[0])) {
						LOGGER.info("Is validated with wp14-episodes-elements.json");
					}
				} catch (IOException e) {
					LOGGER.error("JSON validation tests ", e);
				}
			}
			if (response[1].contains("xml")) {
				LOGGER.info("Validating using FDSNStationXML");
				if (XMLValidator.validate(response[0], new File("stationxml.xsd"))) {
					LOGGER.info("Is validated with FDSNStationXML");
				}
				LOGGER.info("Validating using QuakeML xsd");
				if (XMLValidator.validate(response[0], new File("quakeml.xsd"))) {
					LOGGER.info("Is validated with Quakeml");
				}
			}
		}

	}

	public static String[] TCSResponse(String webservice){
		LOGGER.debug("TCS Request");
		webservice=webservice.replace(" ", "");
		HttpURLConnection connection = null;
		String response = "";
		String format = null;
		System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
		try {
			URL url = new URL(webservice);
			connection = (HttpURLConnection) url.openConnection();
			format = connection.getContentType();
			URL redirectUrl = null;
			try {
				redirectUrl = new URL(connection.getHeaderField("Location"));
				connection = (HttpURLConnection) redirectUrl.openConnection();
				format = connection.getContentType();	
			}catch(Exception e) {}
			if(mimetypesBinary.contains(format)) {
				response = "file";
			}else if(format.toLowerCase().contains("xml") || format.toLowerCase().contains("json")){
				try {
					BufferedReader read = new BufferedReader(new InputStreamReader(connection.getInputStream()));

					if(format.toLowerCase().contains("xml") || format.toLowerCase().contains("json")) {
						String inputLine;
						while ((inputLine = read.readLine()) != null){
							response += inputLine;	        	 	
						}
					}
					read.close();
				}catch(Exception e) {}
			} 
			return new String[] {response, format};
		 
		} catch (IOException e) { 
			LOGGER.error("Problem connecting to TCS",e); 
			format = connection.getContentType();
			return  new String[] {response, format};
		}	    
	}

}
