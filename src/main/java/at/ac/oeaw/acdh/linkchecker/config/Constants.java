/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * NOTICE: This code was modified in ACDH - Austrian Academy of Sciences.
 */

package at.ac.oeaw.acdh.linkchecker.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Constants {
    public static final String SQL_STATUS_TABLE_PARAM_NAME = "sql.status.table";
    public static final String SQL_MAX_DOCS_BUCKET_PARAM_NAME = "sql.max.urls.per.bucket";
    public static final String SQL_MAXRESULTS_PARAM_NAME = "sql.spout.max.results";
    public static final String SQL_UPDATE_BATCH_SIZE_PARAM_NAME = "sql.update.batch.size";
    public static final String SQL_METRICS_TABLE_PARAM_NAME = "sql.metrics.table";
    public static final String SQL_STATUS_RESULT_TABLE_PARAM_NAME = "sql.status.resultTable";
    public static final String SQL_STATUS_HISTORY_TABLE_PARAM_NAME = "sql.status.historyTable";
    public static final String HTTP_REDIRECT_LIMIT = "http.redirectLimit";

    public static final String PARTITION_MODEParamName = "partition.url.mode";
    public static final String PARTITION_MODE_HOST = "byHost";
    public static final String PARTITION_MODE_DOMAIN = "byDomain";
    public static final String PARTITION_MODE_IP = "byIP";
    public static final String STATUS_ERROR_MESSAGE = "error.message";
    public static final String STATUS_ERROR_SOURCE = "error.source";
    public static final String STATUS_ERROR_CAUSE = "error.cause";
    public static final String StatusStreamName = "status";
    public static final String RedirectStreamName = "redirect";
    public static final String DELETION_STREAM_NAME = "deletion";
    public static final String AllowRedirParamName = "redirections.allowed";
    public static final String fetchErrorFetchIntervalParamName = "fetchInterval.fetch.error";
    public static final String errorFetchIntervalParamName = "fetchInterval.error";
    public static final String defaultFetchIntervalParamName = "fetchInterval.default";
    public static final String fetchErrorCountParamName = "fetch.error.count";
    
    public static final String LOGIN_LIST_URL = "login.list.url";
    public static final String OK_STATUS_CODES = "ok.status.codes";
    public static final String REDIRECT_STATUS_CODES = "redirect.status.codes";
    public static final String UNDETERMINED_STATUS_CODES = "undeterminded.status.codes";

    private Constants() {
    }
}
