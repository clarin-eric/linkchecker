/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * NOTICE: This code was modified in ACDH - Austrian Academy of Sciences.
 */

package at.ac.oeaw.acdh;

public class Constants {
    public static final String SQL_STATUS_TABLE_PARAM_NAME = "sql.status.table";
    public static final String SQL_MAX_DOCS_BUCKET_PARAM_NAME = "sql.max.urls.per.bucket";
    public static final String SQL_MAXRESULTS_PARAM_NAME = "sql.spout.max.results";
    public static final String SQL_UPDATE_BATCH_SIZE_PARAM_NAME = "sql.update.batch.size";
    public static final String SQL_METRICS_TABLE_PARAM_NAME = "sql.metrics.table";
    public static final String SQL_STATUS_RESULT_TABLE_PARAM_NAME = "sql.status.resultTable";
    public static final String SQL_STATUS_HISTORY_TABLE_PARAM_NAME = "sql.status.historyTable";
    public static final String HTTP_REDIRECT_LIMIT = "http.redirectLimit";

    private Constants() {
    }
}
