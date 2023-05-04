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


import org.epos.mq.Manager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class XMLValidator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(XMLValidator.class);
  
    public static boolean validate(String xmlFile, File schemaFile) {
    	
    	try {
            Source xmlInputPaylod = new StreamSource(new ByteArrayInputStream(xmlFile.getBytes()));
            SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            schemaFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            schemaFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            Schema schema = schemaFactory.newSchema(schemaFile);
            Validator validator = schema.newValidator();
            final List<SAXParseException> exceptions = new LinkedList<SAXParseException>();
            validator.setErrorHandler(new ErrorHandler()
             {
             @Override
             public void warning(SAXParseException exception) throws SAXException
             {
              exceptions.add(exception);
             }
             @Override
             public void fatalError(SAXParseException exception) throws SAXException
             {
              exceptions.add(exception);
             }
             @Override
             public void error(SAXParseException exception) throws SAXException
             {
              exceptions.add(exception);
             }
             });

            validator.validate(xmlInputPaylod);

             } catch (SAXException ex) {
            		LOGGER.error("Validating XML payload", ex);
                 return false;

             } catch (IOException e) {
            		LOGGER.error("Validating XML payload", e);
             return false;
            }
    	
      
    	return true;
    }
}
