package com.google.cloud.pso.bigquery;

import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.WaitForOption;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.cloud.bigquery.BigQuery.JobField.STATUS;
import static java.util.Arrays.asList;

/**
 * Created by roderickyao on 8/2/17.
 */
public class RenameBQSchemaFields {
    private static final Logger LOG = LoggerFactory.getLogger(RenameBQSchemaFields.class);

    //Parameter: property file that contains the schema matching. Example is schema.properties file.
    public static void main(String[] args) throws InterruptedException {
        // Instantiates a client
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        // The name for the new dataset
        String datasetName = "DAT01lLab";
        String tableName = "accounts";
        String newTableName = "NEWaccounts";



        String oldFieldsString="";
        String newFieldsString="";

        //Validate source dataset exist
        Dataset dataset = bigquery.getDataset(datasetName);
        if (dataset == null) {
            LOG.error("Dataset " + datasetName + " does not exist!");
            return;
        }

        //Constructing a new table with a new Schema
        TableId newTableId=TableId.of(datasetName, newTableName);
        Table table=bigquery.getTable(TableId.of(datasetName,tableName));

        Schema schema=table.getDefinition().getSchema();
        Schema.Builder newSchemaBuilder=Schema.newBuilder();
        for (Field field: schema.getFields()
                ){
            //Rename the fields. Also, you should pay attention to the order of the fields.
            oldFieldsString += field.getName() +",";
            newFieldsString += field.getName()+"AAA,";

            Field.Builder fieldBuilder=Field.newBuilder(field.getName(),field.getType());
            fieldBuilder.setDescription(field.getDescription());
            fieldBuilder.setMode(field.getMode());
            fieldBuilder.setName(field.getName()+"AAA");
            newSchemaBuilder.addField(fieldBuilder.build());
        }
        oldFieldsString=oldFieldsString.substring(0,oldFieldsString.lastIndexOf(","));
        newFieldsString=newFieldsString.substring(0,newFieldsString.lastIndexOf(","));

        Schema newSchema=newSchemaBuilder.build();
        TableDefinition newTableDefinition=StandardTableDefinition.of(newSchema);

        // Copy data from source to new table by using INSERT query
        QueryRequest queryRequest =
                QueryRequest.newBuilder("INSERT " + datasetName+"." + newTableName +
                        " (" + newFieldsString + ") " +
                        "SELECT * FROM " + datasetName + "." + tableName)
                        .setMaxWaitTime(60000L)
                        .setPageSize(1000L)
                        .setUseLegacySql(false)
                        .build();

        QueryResponse queryResponse = bigquery.query(queryRequest);
        while (!queryResponse.jobCompleted()) {
            Thread.sleep(1000L);
            queryResponse = bigquery.getQueryResults(queryResponse.getJobId());
        }

        if(queryResponse.hasErrors()){
            LOG.error(queryResponse.getExecutionErrors().toString());
        }else
            LOG.info("Table rename completed!");

    }

    //Load schema mapping from a properties file. We need to preserve insert order the schema fields
    //Output: LinkedHashMap<String, String>
    private static LinkedHashMap<String, String> loadSchema(String propertyFileName) throws IOException{
        LinkedHashMap<String, String> propertyMap=new LinkedHashMap<>();
        FileReader reader=new FileReader(propertyFileName);
        Properties properties=new Properties();
        properties.load(reader);

        Enumeration keys=properties.keys();
        while(keys.hasMoreElements()){
            String key=(String)keys.nextElement();
            propertyMap.put(key, properties.getProperty(key));
        }

        return  propertyMap;
    }

    //Get each field's information from BigQuery and copy metadata to new schema
    private static LinkedHashMap<Field, Field> enrichSchema(LinkedHashMap<String, String> schemaMap, Table oldTable, BigQuery bigQueryClient) throws Exception{
        Schema schema=oldTable.getDefinition().getSchema();
        List<Field> fields=schema.getFields();

        List<String> newFields = Arrays.asList(schemaMap.values().toArray(new String[0]));

        for (Field field: schema.getFields()){
            Field.Builder fieldBuilder=Field.newBuilder(field.getName(),field.getType());
            fieldBuilder.setDescription(field.getDescription());
            fieldBuilder.setMode(field.getMode());
            fieldBuilder.setName(field.getName()+"AAA");
            newSchemaBuilder.addField(fieldBuilder.build());
        }
    }

    //Validate input fields match all fields from BigQuery to avoid user error
    private static boolean validateOldSchema(Schema oldSchema, List<String> fields){
        if(oldSchema == null){
            return false;
        }

        List<Field> bqFields = oldSchema.getFields();

        if(bqFields.size() != fields.size())
            return false;

        for(int i=0;i<bqFields.size();i++){
            if (!bqFields.get(i).getName().contentEquals(fields.get(i))){
                return false;
            }
        }

        return true;
    }

    private static boolean copyTable(Table sourceTable, String destDataset, String destTable, int timeOutMinutes) {
        TableId destinationId = TableId.of(destDataset, destTable);

        BigQuery.JobOption options = BigQuery.JobOption.fields(STATUS, BigQuery.JobField.USER_EMAIL);
        Job job = sourceTable.copy(destinationId, options);
        // Wait for the job to complete.
        try {
            Job completedJob = job.waitFor(WaitForOption.checkEvery(1, TimeUnit.SECONDS),
                    WaitForOption.timeout(timeOutMinutes, TimeUnit.MINUTES));
            if (completedJob != null && completedJob.getStatus().getError() == null) {
                // Job completed successfully.
                return true;
            } else {
                // Handle error case.
                LOG.error(completedJob.getStatus().getError().toString());
                return false;
            }
        } catch (InterruptedException | TimeoutException e) {
            // Handle interrupted wait
            LOG.error(e.getMessage());
            return false;
        }
    }
}
