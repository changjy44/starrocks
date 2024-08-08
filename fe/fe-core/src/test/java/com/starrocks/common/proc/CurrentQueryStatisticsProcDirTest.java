// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.common.proc;

import com.starrocks.common.AnalysisException;
import com.starrocks.qe.QueryStatisticsInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

public class CurrentQueryStatisticsProcDirTest {
    public static final QueryStatisticsInfo QUERY_ONE = new QueryStatisticsInfo()
            .withQueryStartTime(1721866303)
            .withFeIp("172.17.0.3")
            .withQueryId("870d14e0-4a5d-11ef-a58a-0242ac110003")
            .withConnId("33")
            .withDb("example_db")
            .withUser("root")
            .withScanBytes(2676000)
            .withScanRows(396288)
            .withMemUsageBytes(610900000)
            .withSpillBytes(0)
            .withCpuCostNs(97323000)
            .withExecTime(3533000)
            .withWareHouseName("default_warehouse")
            .withCustomQueryId("");


    public static final QueryStatisticsInfo QUERY_TWO = new QueryStatisticsInfo()
            .withQueryStartTime(1721866304)
            .withFeIp("172.17.0.6")
            .withQueryId("87e23f47-4a5d-11ef-b91e-0242ac110006")
            .withConnId("0")
            .withDb("example_db")
            .withUser("root")
            .withScanBytes(2676000)
            .withScanRows(396288)
            .withMemUsageBytes(613300000)
            .withSpillBytes(0)
            .withCpuCostNs(96576000)
            .withExecTime(2086000)
            .withWareHouseName("default_warehouse")
            .withCustomQueryId("");

    public static List<QueryStatisticsInfo> LOCAL_TEST_QUERIES = new ArrayList<>(List.of(QUERY_ONE, QUERY_TWO));

    @Test
    public void testFetchResult() throws AnalysisException {
        try (MockedStatic<QueryStatisticsInfo> queryStatisticsInfo = mockStatic(QueryStatisticsInfo.class)) {
            queryStatisticsInfo.when(QueryStatisticsInfo::makeListFromMetricsAndMgrs)
                    .thenReturn(LOCAL_TEST_QUERIES);

            BaseProcResult result = (BaseProcResult) new CurrentQueryStatisticsProcDir().fetchResult();
            Assert.assertEquals(LOCAL_TEST_QUERIES.size(), result.getRows().size());

            List<List<String>> expectedQueryStatisticsInfo =
                    LOCAL_TEST_QUERIES.stream()
                            .map(QueryStatisticsInfo::formatToList)
                            .collect(Collectors.toList());

            assertThat(result.getRows()).containsExactlyInAnyOrderElementsOf(expectedQueryStatisticsInfo);
        }
    }

}