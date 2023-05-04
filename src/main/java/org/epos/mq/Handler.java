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
package org.epos.mq;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * @deprecated use router-framework instead
 */
public class Handler 
{
    private String outQueue;
    	
	public JsonObject jsonParser(String jsonString) {
		JsonParser parser = new JsonParser();
		return parser.parse(jsonString).getAsJsonObject();
	}

	public String handle(String message){return message;}
	
	public void setSendQueue(String nameQueue) { this.outQueue = nameQueue; }
	
	public String getSendQueue(){return outQueue;}

	public void handlerExecution(){}

	public void jsonRefiner(){}

	public void updateSourceTarget(){}
	

}
