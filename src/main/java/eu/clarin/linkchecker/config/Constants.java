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

package eu.clarin.linkchecker.config;

public class Constants {
    public static final String SPOUT_MAXRESULTS_PARAM_NAME = "spout.max.results";
    public static final String SPOUT_GROUP_MAXRESULTS_PARAM_NAME = "spout.group.max.results";
    public static final String HTTP_REDIRECT_LIMIT = "http.redirectLimit";
    
    public static final String RedirectStreamName = "redirect";
    
    public static final String LOGIN_LIST_URL = "login.list.url";
    public static final String OK_STATUS_CODES = "ok.status.codes";
    public static final String REDIRECT_STATUS_CODES = "redirect.status.codes";
    public static final String UNDETERMINED_STATUS_CODES = "undeterminded.status.codes";
    public static final String RESTRICTED_ACCESS_STATUS_CODES = "restricted.access.status.codes";

    private Constants() {
    }
}
