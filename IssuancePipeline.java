package com.syw.ors.pipelines.dataflow;

import java.util.List;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.syw.ors.common.OrsErrorIssuanceSchemaCreate;
import com.syw.ors.common.OrsIssuanceErrorFilterPredicate;
import com.syw.ors.common.OrsIssuanceFilterPredicate;
import com.syw.ors.common.OrsIssuanceSchemaCreate;
import com.syw.ors.common.OrsRequestParserTableRowDoFn;
import com.syw.ors.common.ParseIssuanceDoFn;
import com.syw.ors.common.Constants;


public class IssuancePipeline  implements Constants{
	@SuppressWarnings("unused")
	private interface StreamingExtractOptions 
		extends BigQueryOptions, DataflowPipelineOptions{		
	}

	public static void main(String[] args) {
		
		//create Data flow Pipeline Options 
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		
		options.setProject(PROJECT_ID_PROD);
		options.setStagingLocation(STAGING_LOCATION_PROD);		
		options.setTempLocation(TEMP_LOCATION_PROD);
		options.setStreaming(true);
		
	    // Create the Pipeline object with the options we defined above.
	    Pipeline p = Pipeline.create(options);
	    
	    	    
	    PCollection<String> rawLines = p.apply(
	    		PubsubIO.Read.named("ReadFromPubSub").topic("projects" + "/" + PROJECT_ID_PROD + "/" + "topics" + "/" + PUBSUB_REPOSITORY_TOPIC));
	    
	    //apply Pipeline transforms to parse raw lines
	    PCollection<List<KV<String, String>>> parsedRecordCollection = rawLines.apply(
	    		ParDo.named("ParseIssuance").of(new ParseIssuanceDoFn()));
	    	    
		//filter good records
		PCollection<List<KV<String, String>>> filteredRecordCollections = parsedRecordCollection.apply(
				Filter.byPredicate(new OrsIssuanceFilterPredicate()));

		//filter error records
		PCollection<List<KV<String, String>>> errorRecordCollections = parsedRecordCollection.apply(
				Filter.byPredicate(new OrsIssuanceErrorFilterPredicate()));
		
		//convert good records to table rows
	    PCollection<TableRow> tableRowCollection = filteredRecordCollections.apply(
	    		ParDo.named("convertKVtoTableRow").of(new OrsRequestParserTableRowDoFn()));
	    
	    //convert error records to table rows
	    PCollection<TableRow> errortableRowCollection = errorRecordCollections.apply(
	    		ParDo.named("convertKVtoTableRow").of(new OrsRequestParserTableRowDoFn()));

	    
	   //insert good records into big query	
	    tableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WritetoBigQuery")
	    		 .to(ISSUANCE_TABLE_PATH)
	    		 .withSchema(OrsIssuanceSchemaCreate.createSchema())
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
	    
	  //insert bad records into big query	
	    errortableRowCollection.apply(
	    		BigQueryIO.Write
	    		 .named("WritetoBigQuery")
	    		 .to(ISSUANCE_ERROR_TABLE_PATH)
	    		 .withSchema(OrsErrorIssuanceSchemaCreate.createSchema())
	    		 .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
	    		 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
	    
	    p.run(); 
				
	}

}
