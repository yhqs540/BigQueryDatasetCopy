package com.google.cloud.pso.bigquery;

import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.cloud.WaitForOption;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.awt.image.ImageWatched;

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
    //
    public static void main(String[] args) throws Exception {
        if(args.length < 4){
            System.out.println("Missing parameters.");
            return;
        }

        // Instantiates a client
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        String propertyFileName=args[0];
        String datasetName = args[1];
        String tableName = args[2];
        String newTableName = args[3];

        //Validate source dataset exist
        Dataset dataset = bigquery.getDataset(datasetName);
        if (dataset == null) {
            LOG.error("Dataset " + datasetName + " does not exist!");
            return;
        }
        
        Table sourceTable=bigquery.getTable(TableId.of(datasetName,tableName));
        LinkedHashMap<Field, Field> fieldMap=enrichSchema(loadSchema(propertyFileName), sourceTable, bigquery);
        List<Field> newFieldList = Arrays.asList(fieldMap.values().toArray(new Field[0]));
        List<Field> sourceFieldList = Arrays.asList(fieldMap.values().toArray(new Field[0]));

        //New Schema
        Schema newSchema=buildNewSchema(newFieldList);

        //Create the new table with a new Schema
        TableId newTableId=TableId.of(datasetName, newTableName);
        TableDefinition newTableDefinition=StandardTableDefinition.of(newSchema);TimePartitioning tp=TimePartitioning.of(TimePartitioning.Type.DAY);
        if (bigquery.getTable(newTableId) == null){
            Table newTable = bigquery.create(TableInfo.of(newTableId, newTableDefinition));
            LOG.info("Created new table "+ newTableName);
        }else{
            LOG.error("Destination table is already exist.");
            return;
        }
        
        //Copy data from source to new table by using INSERT query
        String copyQuery=makeCopyQuery(sourceFieldList, newFieldList, datasetName, newTableName, tableName);
        QueryRequest queryRequest =
                QueryRequest.newBuilder(copyQuery)
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
    
    //Make INSERT query to new table by selecting all fields from source table
    private static String makeCopyQuery(List<Field> sourceFields, List<Field> newFields, String datasetName, String newTableName, String sourceTableName){
        StringBuilder strBuilder=new StringBuilder();
        strBuilder.append("INSERT ").append(datasetName).append(".").append(newTableName).append(" (");

        for(Field field : sourceFields){
            strBuilder.append(field.getName()).append(",");
        }
        strBuilder.deleteCharAt(strBuilder.length()-1);

        strBuilder.append(") ").append("SELECT ");
        for(Field field : newFields){
            strBuilder.append(field.getName()).append(",");
        }
        strBuilder.deleteCharAt(strBuilder.length()-1);
        strBuilder.append(" ").append(datasetName).append(".").append(sourceTableName);

        return strBuilder.toString();
    }

    //Build new schema
    private static Schema buildNewSchema(List<Field> fields){
        Schema.Builder newSchemaBuilder=Schema.newBuilder();
        for (Field field: fields){
            newSchemaBuilder.addField(field);
        }

        return newSchemaBuilder.build();
    }

    //Load schema mapping from a properties file. We need to preserve insert order the schema fields
    //Output: LinkedHashMap<String, String>
    private static LinkedHashMap<String, String> loadSchema(String propertyFileName) throws IOException{
        LinkedHashMap<String, String> propertyMap=new LinkedHashMap<>();
        FileReader reader=new FileReader(propertyFileName);
        Properties properties=new OrderedProperties();
        properties.load(reader);

        Enumeration keys=properties.propertyNames();
        while(keys.hasMoreElements()){
            String key=(String)keys.nextElement();
            propertyMap.put(key, properties.getProperty(key));
        }

        return  propertyMap;
    }

    //Get each field's information from BigQuery and copy metadata to new schema
    //The given list of fields should match exactly what they are in BigQuery
    private static LinkedHashMap<Field, Field> enrichSchema(LinkedHashMap<String, String> schemaMap, Table sourceTable, BigQuery bigQueryClient) throws Exception{
        if(sourceTable == null){
            throw new Exception("Original Table is not valid or exist.");
        }

        if(bigQueryClient == null){
            throw new Exception("No BigQuery client found.");
        }

        if(schemaMap.size() == 0){
            throw new Exception("No schema was found from the input.");
        }

        Schema schema=sourceTable.getDefinition().getSchema();
        List<String> sourceFields = Arrays.asList(schemaMap.keySet().toArray(new String[0]));

        if(!validateSourceSchema(schema, sourceFields)){
            throw new Exception("Input schema does not match schema from BigQuery.");
        }

        LinkedHashMap<Field, Field> result=new LinkedHashMap<>();
        for (Field field: schema.getFields()){
            Field.Builder fieldBuilder=Field.newBuilder(field.getName(),field.getType());
            fieldBuilder.setDescription(field.getDescription());
            fieldBuilder.setMode(field.getMode());
            fieldBuilder.setName(field.getName());
            result.put(field, fieldBuilder.build());
        }

        return result;
    }

    //Validate input fields match all fields from BigQuery to avoid user error
    private static boolean validateSourceSchema(Schema sourceSchema, List<String> fields){
        if(sourceSchema == null){
            return false;
        }

        List<Field> bqFields = sourceSchema.getFields();
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

class OrderedProperties extends Properties {
    private final LinkedHashMap<Object, Object> values = new LinkedHashMap<Object, Object>();

    public OrderedProperties() {
        super();
    }

    public Enumeration<?> propertyNames() {
        return Collections.enumeration(keySet());
    }

    public String getProperty(String name) {
        return (String) get(name);
    }

    @Override public Object put(Object key, Object value) {
        return values.put(key, value);
    }

    @Override public Set<Object> keySet() {
        return values.keySet();
    }
}