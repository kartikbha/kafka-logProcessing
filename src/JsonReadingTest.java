
import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class JsonReadingTest {

	private static JsonFactory factory;
	
	public static void main(String args[]) throws JsonParseException, JsonMappingException, IOException {

		/**
		 * 
		 * [{ "timestamp":1407394935908, "publisher":"publisher_0",
		 * "advertiser":"advertiser_2" ,"website":"website_7689.com",
		 * "geo":"MI", "bid":0.14403514698631104, "cookie":"cookie_7447"}]
		 * 
		 */

		//JSONObject parser = new JSONObject();
	
		 ObjectMapper mapper = new ObjectMapper(factory);

	
		String input = "[{ \"timestamp\":1407394935908, \"publisher\":\"publisher_0\",\"advertiser\":\"advertiser_2\" ,\"website\":\"website_7689.com\",\"geo\":\"MI\", \"bid\":0.14403514698631104, \"cookie\":\"cookie_7447\"}]";

		 ArrayNode arrayNode = mapper.readValue(input, ArrayNode.class);
		 ObjectNode objectnode =  (ObjectNode) arrayNode.get(0);
			
	
		System.out.println(objectnode.get("timestamp").toString());
		
		String str = objectnode.get("timestamp").toString();
		
		System.out.println("f...."+str);
		
		
	}
}
