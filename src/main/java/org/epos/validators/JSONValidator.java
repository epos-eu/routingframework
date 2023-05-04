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


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/*import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;*/


public class JSONValidator {

	public static boolean isJsonValid(File inputSchema, String payload) throws IOException {

		try (InputStream inputStream = new FileInputStream(inputSchema)) {
			JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
			Schema schema = SchemaLoader.load(rawSchema);
			
			Object jsonPayload = new JSONTokener(payload).nextValue();
			
			try {
				if(jsonPayload instanceof JSONObject) schema.validate(new JSONObject(payload));
				if(jsonPayload instanceof JSONArray) schema.validate(new JSONArray(payload));
			}catch(ValidationException ve) {
				return false;
			}
		}

		return true;
	}
	/*
	public static final String JSON_V7_SCHEMA_IDENTIFIER = "http://json-schema.org/draft-07/schema#";
	public static final String JSON_SCHEMA_IDENTIFIER_ELEMENT = "$schema";

	public static JsonNode getJsonNode(String jsonText) 
			throws IOException
	{
		return JsonLoader.fromString(jsonText);
	} // getJsonNode(text) ends

	public static JsonNode getJsonNode(File jsonFile) 
			throws IOException 
	{
		return JsonLoader.fromFile(jsonFile);
	} // getJsonNode(File) ends

	public static JsonNode getJsonNode(URL url) 
			throws IOException 
	{
		return JsonLoader.fromURL(url);
	} // getJsonNode(URL) ends

	public static JsonNode getJsonNodeFromResource(String resource) 
			throws IOException
	{
		return JsonLoader.fromResource(resource);
	} // getJsonNode(Resource) ends

	public static JsonSchema getSchemaNode(String schemaText)
			throws IOException, ProcessingException 
	{
		final JsonNode schemaNode = getJsonNode(schemaText);
		return _getSchemaNode(schemaNode);
	} // getSchemaNode(text) ends

	public static JsonSchema getSchemaNode(File schemaFile)
			throws IOException, ProcessingException
	{
		final JsonNode schemaNode = getJsonNode(schemaFile);
		return _getSchemaNode(schemaNode);
	} // getSchemaNode(File) ends

	public static JsonSchema getSchemaNode(URL schemaFile)
			throws IOException, ProcessingException
	{
		final JsonNode schemaNode = getJsonNode(schemaFile);
		return _getSchemaNode(schemaNode);
	} // getSchemaNode(URL) ends

	public static JsonSchema getSchemaNodeFromResource(String resource)
			throws IOException, ProcessingException 
	{
		final JsonNode schemaNode = getJsonNodeFromResource(resource);
		return _getSchemaNode(schemaNode);
	} // getSchemaNode() ends

	public static void validateJson(JsonSchema jsonSchemaNode, JsonNode jsonNode)
			throws ProcessingException 
	{
		ProcessingReport report = jsonSchemaNode.validate(jsonNode);
		if (!report.isSuccess()) {
			for (ProcessingMessage processingMessage : report) {
				throw new ProcessingException(processingMessage);
			}
		}
	} // validateJson(Node) ends

	public static boolean isJsonValid(JsonSchema jsonSchemaNode, JsonNode jsonNode) throws ProcessingException
	{
		ProcessingReport report = jsonSchemaNode.validate(jsonNode);
		return report.isSuccess();
	} // validateJson(Node) ends

	public static boolean isJsonValid(String schemaText, String jsonText) throws ProcessingException, IOException
	{   
		final JsonSchema schemaNode = getSchemaNode(schemaText);
		final JsonNode jsonNode = getJsonNode(jsonText);
		return isJsonValid(schemaNode, jsonNode);
	} // validateJson(Node) ends

	public static boolean isJsonValid(File schemaFile, File jsonFile) throws ProcessingException, IOException
	{   
		final JsonSchema schemaNode = getSchemaNode(schemaFile);
		final JsonNode jsonNode = getJsonNode(jsonFile);
		return isJsonValid(schemaNode, jsonNode);
	} // validateJson(Node) ends

	public static boolean isJsonValid(URL schemaURL, URL jsonURL) throws ProcessingException, IOException
	{   
		final JsonSchema schemaNode = getSchemaNode(schemaURL);
		final JsonNode jsonNode = getJsonNode(jsonURL);
		return isJsonValid(schemaNode, jsonNode);
	} // validateJson(Node) ends    

	public static void validateJson(String schemaText, String jsonText) throws IOException, ProcessingException{
		final JsonSchema schemaNode = getSchemaNode(schemaText);
		final JsonNode jsonNode = getJsonNode(jsonText);
		validateJson(schemaNode, jsonNode);
	} // validateJson(text) ends

	public static void validateJson(File schemaFile, File jsonFile) throws IOException, ProcessingException{
		final JsonSchema schemaNode = getSchemaNode(schemaFile);
		final JsonNode jsonNode = getJsonNode(jsonFile);
		validateJson(schemaNode, jsonNode);
	} // validateJson(File) ends

	public static void validateJson(URL schemaDocument, URL jsonDocument) throws IOException, ProcessingException{
		final JsonSchema schemaNode = getSchemaNode(schemaDocument);
		final JsonNode jsonNode = getJsonNode(jsonDocument);
		validateJson(schemaNode, jsonNode);
	} // validateJson(URL) ends

	public static void validateJsonResource(String schemaResource, String jsonResource) throws IOException, ProcessingException{
		final JsonSchema schemaNode = getSchemaNode(schemaResource);
		final JsonNode jsonNode = getJsonNodeFromResource(jsonResource);
		validateJson(schemaNode, jsonNode);
	} // validateJsonResource() ends

	private static JsonSchema _getSchemaNode(JsonNode jsonNode)
			throws ProcessingException
	{
		final JsonNode schemaIdentifier = jsonNode.get(JSON_SCHEMA_IDENTIFIER_ELEMENT);
		if (null == schemaIdentifier){
			((ObjectNode) jsonNode).put(JSON_SCHEMA_IDENTIFIER_ELEMENT, JSON_V7_SCHEMA_IDENTIFIER);
		}

		final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
		return factory.getJsonSchema(jsonNode);
	} // _getSchemaNode() ends

	public static boolean isJsonValid(File schemaFile, String jsonText) throws ProcessingException, IOException {
		final JsonSchema schemaNode = getSchemaNode(schemaFile);
		final JsonNode jsonNode = getJsonNode(jsonText);
		return isJsonValid(schemaNode, jsonNode);
	}
	 */
}
