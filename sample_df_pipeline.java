package analytics.gcp.sample.pipeline;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.gson.JsonObject;

import analytics.gcp.sample.config.AppConfiguration;
import analytics.gcp.sample.config.Assemble;
import analytics.gcp.sample.dao.dbmanager.BigTableConnManager;
import analytics.gcp.sample.enums.BigTableFamilyEnum;
import analytics.gcp.sample.util.AppConfigReader;
import analytics.gcp.sample.util.Constants;



public class SamplePipeline {

	private static final Logger LOG = LoggerFactory.getLogger(ModelMigrationPipeline.class);	
	private static final String queryFileName = "src/main/resources/sample.sql"; 
	
	
		
	public static String getQuery(String queryFileName) throws IOException {
		FileReader fr = new FileReader(queryFileName);
		BufferedReader br = new BufferedReader(fr);
		String line = br.readLine();
		StringBuilder sb = new StringBuilder();
		
		while((line != null)){
			sb.append(line).append(" ");
			line = br.readLine();
		}
		
		br.close();
			
		return sb.toString();				
	}
	
	public static class SampleTableRowToMutationDoFn extends DoFn<KV<String, Iterable<TableRow>>, Mutation> {
		private static final long serialVersionUID = 1L;
		
		
		@Override
		public void processElement(ProcessContext c) throws Exception{			
			KV<String, Iterable<TableRow>> kvTableRowList = c.element();			
			
			String memberId = kvTableRowList.getKey();//id_no is the row key for the BT. It is coming from BQ query.
			if(memberId != null){
			    Iterable<TableRow> TableRowList = kvTableRowList.getValue();
			    JsonObject jsonObject = new JsonObject();

                for(TableRow row : TableRowList){
                    if(row.containsKey("modelCode")){
                        String key = (String) row.get("modelCode");
                        if(row.containsKey("R_BB")){
                            String value = (String) row.get("R_BB");
                            jsonObject.addProperty(key, value);
                        }
                    }
                }

				byte[] rowKey = Bytes.toBytes(memberId);//making it ready for BT insert
				Put put = new Put(rowKey);
				put.addColumn(Bytes.toBytes(BigTableFamilyEnum.OFFER_CATEGORY_FAMILY.toString()), 
						Bytes.toBytes(BigTableFamilyEnum.OFFER_CATEGORY_QUALIFIER.toString()), 
						Bytes.toBytes(jsonObject.toString()));
				c.output(put);//BT insert
			}				
		}
	}
	
	public static class ConvertTableRowToKeyTableRow extends DoFn<TableRow, KV<String, TableRow>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(ProcessContext c) throws Exception{
			TableRow row = c.element();
			String memberId = (String) row.get("id_no");

			if(memberId != null){
				KV<String, TableRow> tableRowKv = KV.of(memberId, row);
				c.output(tableRowKv);
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		//Setting up Big Table Configurations		
		String env = Constants.DEV_ENV;
		if(args.length>0 && Constants.PROD_ENV.equalsIgnoreCase(args[0])){
			env = Constants.PROD_ENV;
		}
		
		Assemble assemble = new Assemble();
		assemble.setEnv(env);
		assemble.setPipelineName("SamplePipeline");
		assemble.setBqTable("SampleTable");	
		
		//String modelTable = "offercategory-test-msaha";
		//CloudBigtableScanConfiguration cloudBigtableConfig = BigTableConnManager.getCloudBigtableScanConfig(assemble, modelTable);
		
		
		CloudBigtableScanConfiguration cloudBigtableConfig = new CloudBigtableScanConfiguration.Builder()
				.withProjectId("msaha")
				.withInstanceId("msaha")
				.withTableId("msaha")
				.build();
				
			
		
		
		//Setting up Dataflow options
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
				
		AppConfiguration appConfig = AppConfigReader.getAppConfiguration(env);
		options.setProject(appConfig.getProjectId());
		
		options.setTempLocation(appConfig.getDataflow().getTempLocation()); // "gs://dataflow-tmp/sample-migration")
		
		Pipeline p = Pipeline.create(options);
		CloudBigtableIO.initializeForWrite(p);
		
		PCollection<TableRow> offerCategory = p.apply(
				BigQueryIO.Read
					.named("ReadOfferCategory")
					.fromQuery(getQuery(queryFileName))
					.usingStandardSql());
		
		PCollection<KV<String, TableRow>> offerCategoryKeyAndRows = offerCategory.apply(
	            ParDo.named("ConvertTableRowToKeyTableRow").of(new ConvertTableRowToKeyTableRow()
	                )
	            );

	    PCollection<KV<String, Iterable<TableRow>>> groupedOfferCategory =  offerCategoryKeyAndRows.apply(GroupByKey.<String, TableRow>create());

		
		PCollection<Mutation> offerCategoryBTPut = groupedOfferCategory.apply(
				ParDo.named("ConvertOfferCategoryTableRowToMutationDoFn").of(new ConvertOfferCategoryTableRowToMutationDoFn()));
		
		
		offerCategoryBTPut.apply("BigTable Write",
								CloudBigtableIO.writeToTable(cloudBigtableConfig));
		p.run();
				
	}
	
	
	
	
}
