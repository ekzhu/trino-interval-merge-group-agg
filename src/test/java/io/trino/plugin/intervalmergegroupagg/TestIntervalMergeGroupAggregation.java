/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.intervalmergegroupagg;

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestIntervalMergeGroupAggregation
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder().build();

        LocalQueryRunner queryRunner = LocalQueryRunner.builder(session).build();

        try {
            queryRunner.installPlugin(new IntervalMergeGroupAggPlugin());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    @Test
    public void testMergeGroupWindowFunctionLong()
    {
        assertQuery("SELECT pid, x, y, merge_group(x, y) OVER (partition by pid order by x) FROM " +
                        "(VALUES " +
                        "('a', cast(1 as bigint), cast(4 as bigint)), " +
                        "('a', 2, 3), " +
                        "('b', 3, 5), " +
                        "('c', 2, 4), " +
                        "('b', 4, 6), " +
                        "('a', 5, 7)) AS t(pid, x, y)",
                "VALUES " +
                        "('a', cast(1 as bigint), cast(4 as bigint), 1), " +
                        "('a', 2, 3, 1), " +
                        "('a', 5, 7, 2), " +
                        "('b', 3, 5, 1), " +
                        "('b', 4, 6, 1), " +
                        "('c', 2, 4, 1)");
    }

    @Test
    public void testMergeGroupWindowFunctionDate()
    {
        assertQuery("SELECT pid, x, y, merge_group(x, y) OVER (partition by pid order by x) FROM " +
                        "(VALUES " +
                        "('a', date '2012-01-01', date '2012-01-02'), " +
                        "('a', date '2012-01-02', date '2012-01-03'), " +
                        "('b', date '2012-01-03', date '2012-01-05'), " +
                        "('c', date '2012-01-02', date '2012-01-04'), " +
                        "('b', date '2012-01-04', date '2012-01-06'), " +
                        "('a', date '2012-01-05', date '2012-01-07')) " +
                        "AS t(pid, x, y)",
                "VALUES " +
                        "('a', date '2012-01-01', date '2012-01-02', 1), " +
                        "('a', date '2012-01-02', date '2012-01-03', 1), " +
                        "('b', date '2012-01-03', date '2012-01-05', 1), " +
                        "('c', date '2012-01-02', date '2012-01-04', 1), " +
                        "('b', date '2012-01-04', date '2012-01-06', 1), " +
                        "('a', date '2012-01-05', date '2012-01-07', 2)");
    }

    @Test
    public void testMergeGroupWindowFunctionTimestamp()
    {
        assertQuery("SELECT pid, x, y, merge_group(x, y) OVER (partition by pid order by x) FROM " +
                        "(VALUES " +
                        "('a', timestamp '2012-01-01', timestamp '2012-01-02'), " +
                        "('a', timestamp '2012-01-02', timestamp '2012-01-03'), " +
                        "('b', timestamp '2012-01-03', timestamp '2012-01-05'), " +
                        "('c', timestamp '2012-01-02', timestamp '2012-01-04'), " +
                        "('b', timestamp '2012-01-04', timestamp '2012-01-06'), " +
                        "('a', timestamp '2012-01-05', timestamp '2012-01-07')) " +
                        "AS t(pid, x, y)",
                "VALUES " +
                        "('a', timestamp '2012-01-01', timestamp '2012-01-02', 1), " +
                        "('a', timestamp '2012-01-02', timestamp '2012-01-03', 1), " +
                        "('b', timestamp '2012-01-03', timestamp '2012-01-05', 1), " +
                        "('c', timestamp '2012-01-02', timestamp '2012-01-04', 1), " +
                        "('b', timestamp '2012-01-04', timestamp '2012-01-06', 1), " +
                        "('a', timestamp '2012-01-05', timestamp '2012-01-07', 2)");
    }

    @Test
    public void testMergeGroupWindowFunctionDouble()
    {
        assertQuery("SELECT pid, x, y, merge_group(x, y) OVER (partition by pid order by x) FROM " +
                        "(VALUES " +
                        "('a', 1.0, 4.0), " +
                        "('a', 2.0, 3.0), " +
                        "('b', 3.0, 5.0), " +
                        "('c', 2.0, 4.0), " +
                        "('b', 4.0, 6.0), " +
                        "('a', 5.0, 7.0)) AS t(pid, x, y)",
                "VALUES " +
                        "('a', 1.0, 4.0, 1), " +
                        "('a', 2.0, 3.0, 1), " +
                        "('a', 5.0, 7.0, 2), " +
                        "('b', 3.0, 5.0, 1), " +
                        "('b', 4.0, 6.0, 1), " +
                        "('c', 2.0, 4.0, 1)");
    }
}
