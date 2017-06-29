package edu.illinois.elasticsearch;

import java.io.IOException;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import edu.gslis.textrepresentation.FeatureVector;

public class RocchioRestAction extends BaseRestHandler {

    @Inject
    public RocchioRestAction(Settings settings, RestController controller) {
        super(settings);
                
        // Register your handlers here
        controller.registerHandler(RestRequest.Method.GET, "/{index}/_expand", this);
    }

    
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
    	
    	String index = request.param("index");
    	String query = request.param("query");
    	
    	String type = request.param("type", "dataset");
    	String field = request.param("field", "_all");
    	double alpha = Double.parseDouble(request.param("alpha", "0.5"));
    	double beta = Double.parseDouble(request.param("beta", "0.5"));
    	double k1 = Double.parseDouble(request.param("k1", "1.2"));
    	double b = Double.parseDouble(request.param("b", "0.75"));
    	int fbDocs = Integer.parseInt(request.param("fbDocs", "10"));
    	int fbTerms = Integer.parseInt(request.param("fbTerms", "10"));
    	
    	
		ESRocchioExample rocchio = new ESRocchioExample(client, index, type, field, alpha, beta, k1, b);
		
		// Expand the query
		FeatureVector feedbackQuery = rocchio.expandQuery(query, fbDocs, fbTerms);
		
		StringBuffer expandedQuery = new StringBuffer();
		for (String term : feedbackQuery.getFeatures()) {
			expandedQuery.append(term + "^" + feedbackQuery.getFeatureWeight(term) + " ");
		}
				
	    return channel -> {
	    	XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            builder.field("query", expandedQuery.toString());
            builder.endObject();
	        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
	    };
    }
}