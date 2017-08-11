package com.google.cloud.pso.bigquery;

import com.google.cloud.WaitForOption;
import com.google.cloud.bigquery.*;
import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.cloud.bigquery.BigQuery.JobField.STATUS;

/**
 * Created by roderickyao on 8/2/17.
 */
public class RenameBQSchemaFields {
    private static final Logger LOG = LoggerFactory.getLogger(RenameBQSchemaFields.class);

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
