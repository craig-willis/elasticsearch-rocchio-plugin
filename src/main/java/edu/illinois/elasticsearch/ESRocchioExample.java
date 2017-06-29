package edu.illinois.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsItemResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsRequestBuilder;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;

import edu.gslis.textrepresentation.FeatureVector;


/**
 * Rocchio implementation for Lucene based on: 
 *   https://github.com/gtsherman/lucene/blob/master/src/main/java/org/retrievable/lucene/searching/expansion/Rocchio.java
 * 
 * 
 * 
 * Todo: 
 * 		Specify stoplist
 */
public class ESRocchioExample 
{
	private Client client;  // ElasticSearch client
	private String index;   // ElasticSearch index name
	private String type;    // Document type
	private String field;   // Field
	
	private double alpha;   // Rocchio alpha
	private double beta;    // Rocchio beta
	private double k1;      // BM25 k1	
	private double b;       // BM25 b
		
	// Global statistics (there's certainly a better way to handle this)
	long docCount = 0;      // Number of documents in index
	double avgDocLen = 0;   // Average document length, needed by BM25
	Map<String, Long> dfStats = new HashMap<String, Long>(); // Cached doc frequency stats
	

	public ESRocchioExample(Client client, String index, String type, String field, double alpha, double beta, double k1, double b) {
		this.client = client;
		this.index = index;
		this.type = type;
		this.field = field;
		this.alpha = alpha;
		this.beta = beta;
		this.k1 = k1;
		this.b = b;
	}
	
	/**
	 * Run the query 
	 * 
	 * @param query	Query string
	 * @param numDocs Number of results to return
	 * @return SearchHits object
	 */
	private SearchHits runQuery(String query, int numDocs) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);	        
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder(query);
        searchRequestBuilder.setQuery(queryStringQueryBuilder).setSize(numDocs);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        SearchHits hits = response.getHits();
        return hits;
	}
	
	/**
	 * Given a set of SearchHits, construct the feedback vector
	 * 
	 * @param hits  SearchHits
	 * @param fbDocs  Number of feedback documents
	 * @return  FeatureVector based on feedback documents
	 * @throws IOException
	 */
	private FeatureVector getFeedbackVector(SearchHits hits, int fbDocs) throws IOException 
	{
		FeatureVector summedDocVec = new FeatureVector(null);	

		// Use the multi termvector request to get vectors for all documents at once
	    MultiTermVectorsRequestBuilder mtbuilder = client.prepareMultiTermVectors();
		for (SearchHit hit: hits) {
			String id = hit.getId();
			TermVectorsRequest termVectorsRequest = new TermVectorsRequest();					
			termVectorsRequest.index(index).id(id).type(this.type).termStatistics(true).
				offsets(false).positions(false).payloads(false);
			
			mtbuilder.add(termVectorsRequest);
		}			
	    MultiTermVectorsResponse mtvresponse = mtbuilder.execute().actionGet();

		// Iterate over the returned document vectors. Construct the feedback vector.
	    // Store the global document count and calculate the global average document length
		// Store document frequencies for encountered terms in dfStats map.
	    for (MultiTermVectorsItemResponse item: mtvresponse.getResponses()) {
		    FeatureVector docVec = new FeatureVector(null);
		    
	    	TermVectorsResponse tv = item.getResponse();	
	    	Fields fields =  tv.getFields();
		    Terms terms = fields.terms(this.field);
		    
		    // These are global settings and will be the same for all TermVectorResponses.
		    // There's a better way to handle this.
		    long sumTotalTermFreq = terms.getSumTotalTermFreq(); // Total number of terms in index
		    docCount = terms.getDocCount();  // Total number of documents in index
		    avgDocLen = sumTotalTermFreq/(double)docCount;
		    
		    // Get the term frequency and document frequency for each term
		    TermsEnum termsEnum = terms.iterator();
		    while (termsEnum.next() != null) {
		    	String term = termsEnum.term().utf8ToString();
		    	long freq = termsEnum.totalTermFreq();  // Frequency for term t in this document
		    	long df = termsEnum.docFreq(); // Frequency for term t in all documents (document frequency) -- a global statistic
		    	dfStats.put(term, df); // Map storing global document frequencies for seen terms, used by BM25
		    	docVec.addTerm(term, freq); // Current document vector
		    }
		    
		    // Add this document to the feedback document vector with BM25 weights
		    computeBM25Weights(docVec, summedDocVec);			    
	    }
        		    
		// Multiply the summed term vector by beta / |Dr|
		FeatureVector relDocTermVec = new FeatureVector(null);
		for (String term : summedDocVec.getFeatures()) {
			relDocTermVec.addTerm(term, summedDocVec.getFeatureWeight(term) * beta / fbDocs);
		}
		
		return relDocTermVec;
	}
	
	/**
	 * Construct the query vector with BM25 weights
	 * @param query Query string
	 * @return FeatureVector
	 */
	public FeatureVector getQueryVector(String query) {
		// Create a query vector and scale by alpha
		FeatureVector rawQueryVec = new FeatureVector(null);
		rawQueryVec.addText(query);
		
		FeatureVector summedQueryVec = new FeatureVector(null);
	    computeBM25Weights(rawQueryVec, summedQueryVec);
		
		FeatureVector queryTermVec = new FeatureVector(null);
		for (String term : rawQueryVec.getFeatures()) {
			queryTermVec.addTerm(term, summedQueryVec.getFeatureWeight(term) * alpha);
		}
		
		return queryTermVec;
	}
	
	/**
	 * Expand the query.
	 * 
	 * @param query Query string
	 * @param fbDocs Number of feedback documents
	 * @param fbTerms Number of feedback terms
	 * @return Expanded feature vector
	 * @throws IOException
	 */
	public FeatureVector expandQuery(String query, int fbDocs, int fbTerms) throws IOException 
	{
		// Run the initial query
        SearchHits hits = runQuery(query, fbDocs);
       
        // Get the feedback document vector, weighted by beta
        FeatureVector feedbackVector = getFeedbackVector(hits, fbDocs);
        
        // Get the original query vector, weighted by alpha
        // Note, this is called after getFeedbackVector because it relies on dfStats
        FeatureVector queryVector = getQueryVector(query);
        		    			
		// Combine query and feedbackvectors
		for (String term : queryVector.getFeatures()) {
			feedbackVector.addTerm(term, queryVector.getFeatureWeight(term));
		}
		
		// Get top terms -- aka head
		feedbackVector.clip(fbTerms);

		return feedbackVector;
	}

	
	/**
	 * Compute BM25 weights for the input vector and add to the output vector
	 * @param inputVector  
	 * @param outputVector
	 */
	private void computeBM25Weights(FeatureVector inputVector,  FeatureVector outputVector) 
	{
		for (String term : inputVector.getFeatures()) {
			long docOccur = dfStats.get(term);
			
			double idf = Math.log( (docCount + 1) / (docOccur + 0.5) ); // following Indri
			double tf = inputVector.getFeatureWeight(term);
			
			double weight = (idf * k1 * tf) / (tf + k1 * (1 - b + b * inputVector.getLength() / avgDocLen));
			outputVector.addTerm(term, weight);
		}
	}	
	
	/**
	 * Command line options
	 * @return
	 */
    public static Options createOptions()
    {
        Options options = new Options();
        options.addOption("cluster", true, "ElasticSearch cluster name (default: biocaddie)");
        options.addOption("host", true, "ElasticSearch host (default: localhost)");
        options.addOption("port", true, "ElasticSearch transport port (default: 9300)");
        options.addOption("index", true, "ElasticSearch index name (default: biocaddie)");
        options.addOption("type", true, "ElasticSearch document type  (default: dataset)");
        options.addOption("field", true, "ElasticSearch  field  (default: _all)");
        options.addOption("alpha", true, "Rocchio alpha (default: 0.5)");
        options.addOption("beta", true, "Rocchio beta (default: 0.5)");
        options.addOption("k1", true, "BM25 k1 (default: 1.2)");
        options.addOption("b", true, "BM25 b (default: 0.75)");
        options.addOption("query", true, "Query string");
        options.addOption("auth", true, "Basic authentication string (default: elastic:biocaddie)");
        return options;
    }
      
	
    /*
	public static void main(String[] args) throws IOException, ParseException
	{
	
        Options options = createOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine cl = parser.parse( options, args);
        if (cl.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( ESRocchioExample.class.getCanonicalName(), options );
            return;
        }
        
        // Get the many command line parameters
        String cluster = cl.getOptionValue("cluster", "elasticsearch");
        String host = cl.getOptionValue("host", "localhost");
        int port = Integer.parseInt(cl.getOptionValue("port", "9300"));
        double alpha = Double.parseDouble(cl.getOptionValue("alpha", "0.5"));
        double beta = Double.parseDouble(cl.getOptionValue("beta", "0.5"));
        double k1 = Double.parseDouble(cl.getOptionValue("k1", "1.2"));
        double b = Double.parseDouble(cl.getOptionValue("b", "0.75"));
        int fbTerms = Integer.parseInt(cl.getOptionValue("fbTerms", "10"));
        int fbDocs = Integer.parseInt(cl.getOptionValue("fbDocs", "10"));
        String index = cl.getOptionValue("index", "biocaddie");
        String type = cl.getOptionValue("type", "dataset");
        String field = cl.getOptionValue("field", "_all");

        String auth = cl.getOptionValue("auth", "elastic:biocaddie");
        String query = cl.getOptionValue("query", "multiple sclerosis");
        
        
        // Connect to ElasticSearch
		Settings settings = Settings.builder().put("cluster.name", cluster).build();
		@SuppressWarnings("unchecked")
		TransportClient transportClient = new PreBuiltTransportClient(settings);
		transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
	
		Client client = transportClient.filterWithHeader(Collections.singletonMap("Authorization", auth));
		
		// Construct Rocchio
		ESRocchioExample rocchio = new ESRocchioExample(client, index, type, field, alpha, beta, k1, b);
		
		// Expand the query
		FeatureVector feedbackQuery = rocchio.expandQuery(query, fbDocs, fbTerms);
		
		// Dump the expanded query
		StringBuffer esQuery = new StringBuffer();
		for (String term : feedbackQuery.getFeatures()) {
			esQuery.append(term + "^" + feedbackQuery.getFeatureWeight(term) + " ");
		}	    
		System.out.println(esQuery);
		
		transportClient.close();

	}
	*/
}
