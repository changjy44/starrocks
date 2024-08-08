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
import com.starrocks.server.NodeMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.starrocks.common.proc.CurrentQueryStatisticsProcDirTest.LOCAL_TEST_QUERIES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

public class CurrentGlobalQueryStatisticsProcDirTest {
    public static final QueryStatisticsInfo QUERY_ONE_REMOTE = new QueryStatisticsInfo()
            .withQueryStartTime(1721866428)
            .withFeIp("192.168.0.4")
            .withQueryId("84192bde-7af4-4707-83e2-52b1a8653353")
            .withConnId("33")
            .withDb("example_db")
            .withUser("root")
            .withScanBytes(2676000)
            .withScanRows(396288)
            .withMemUsageBytes(610900000)
            .withSpillBytes(0)
            .withCpuCostNs(97456000)
            .withExecTime(3687000)
            .withWareHouseName("default_warehouse")
            .withCustomQueryId("");


    public static final QueryStatisticsInfo QUERY_TWO_REMOTE = new QueryStatisticsInfo()
            .withQueryStartTime(1721866430)
            .withFeIp("192.168.0.5")
            .withQueryId("746f0274-6252-4f9a-a07e-ddacbbf71ee2")
            .withConnId("0")
            .withDb("example_db")
            .withUser("root")
            .withScanBytes(2689000)
            .withScanRows(398988)
            .withMemUsageBytes(723300000)
            .withSpillBytes(0)
            .withCpuCostNs(96686000)
            .withExecTime(2196000)
            .withWareHouseName("default_warehouse")
            .withCustomQueryId("");

    public static List<QueryStatisticsInfo> REMOTE_TEST_QUERIES =
            new ArrayList<>(List.of(QUERY_ONE_REMOTE, QUERY_TWO_REMOTE));

    @Test
    public void testFetchResult() throws AnalysisException {
        try (MockedStatic<QueryStatisticsInfo> queryStatisticsInfo = mockStatic(QueryStatisticsInfo.class)) {
            queryStatisticsInfo.when(QueryStatisticsInfo::makeListFromMetricsAndMgrs)
                    .thenReturn(LOCAL_TEST_QUERIES);

            new MockUp<NodeMgr>() {
                @Mock
                public List<QueryStatisticsInfo> getQueryStatisticsInfoFromOtherFEs() {
                    return REMOTE_TEST_QUERIES;
                }
            };

            BaseProcResult result = (BaseProcResult) new CurrentGlobalQueryStatisticsProcDir().fetchResult();
            Assert.assertEquals(LOCAL_TEST_QUERIES.size() + REMOTE_TEST_QUERIES.size(),
                    result.getRows().size());

            List<List<String>> expectedQueryStatisticsInfo =
                    Stream.concat(LOCAL_TEST_QUERIES.stream(), REMOTE_TEST_QUERIES.stream())
                            .map(QueryStatisticsInfo::formatToList)
                            .collect(Collectors.toList());

            assertThat(result.getRows()).containsExactlyInAnyOrderElementsOf(expectedQueryStatisticsInfo);
        }
    }
}