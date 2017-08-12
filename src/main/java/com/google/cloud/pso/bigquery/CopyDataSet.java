/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.pso.bigquery;

import com.google.api.gax.paging.Page;

import com.google.cloud.WaitForOption;
import com.google.cloud.bigquery.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.cloud.bigquery.BigQuery.JobField.STATUS;

/**
 * A simple piece of code to replicate a BigQuery dataset to a different dataset in the same project.
 * All content and structure of tables inside of the dataset will be copied.
 */
public class CopyDataSet {
    private static final Logger LOG = LoggerFactory.getLogger(CopyDataSet.class);

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


    public static void main(String[] args) {
        // Instantiates a client
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        // The name for the new dataset
        String datasetName = "pso_data_demos";
        String newDatasetName = "pso_data_demos_copy";

        //Validate source dataset exist
        Dataset dataset = bigquery.getDataset(datasetName);
        if (dataset == null) {
            LOG.error("Dataset " + datasetName + " does not exist!");
            return;
        }

        //Validate destination dataset or to create
        Dataset newDataSet = bigquery.getDataset(newDatasetName);
        if (newDataSet == null) {
            bigquery.create(DatasetInfo.newBuilder(newDatasetName).build());
            LOG.info("Destination dataset " + newDatasetName + " does not exist. Create new dataset.");
        }

        Page<Table> tables = bigquery.listTables(datasetName, BigQuery.TableListOption.pageSize(100));
        Iterator<Table> tableIterator = tables.iterateAll().iterator();
        while (tableIterator.hasNext()) {
            Table table = tableIterator.next();

            //Check if the table exists in the destination dataset. Do not copy or overwrite then.
            if (bigquery.getTable(TableId.of(newDatasetName,table.getTableId().getTable())) == null){
                if (copyTable(table, newDatasetName, table.getTableId().getTable(),3) == true){
                    LOG.info("Table "+table.getTableId().getTable()+" copied.");
                }
                else{
                    LOG.error("Table "+table.getTableId().getTable()+" failed to copy.");
                }
            }else{
                LOG.error("Table "+table.getTableId().getTable()+" exists in the destination dataset. Ignored!");
            }

        }

    }

}
