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
package com.facebook.presto.tests;

import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static com.facebook.presto.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static com.facebook.presto.testing.TestingAccessControlManager.privilege;

public class TestLocalQueryRunner
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
    {
        return TestLocalQueries.createLocalQueryRunner();
    }

    @Test
    public void testSimpleQuery()
    {
        assertQuery("SELECT * FROM nation");
    }

    @Test
    public void testAnalyzeAccessControl()
    {
        assertAccessAllowed("ANALYZE nation");
        assertAccessDenied("ANALYZE nation", "Cannot insert into table .*.nation.*", privilege("nation", INSERT_TABLE));
        assertAccessDenied("ANALYZE nation", "Cannot select from columns \\[.*] in table or view .*.nation", privilege("nation", SELECT_COLUMN));
        assertAccessDenied("ANALYZE nation", "Cannot select from columns \\[.*nationkey.*] in table or view .*.nation", privilege("nationkey", SELECT_COLUMN));
    }

    @Test
    public void testExplain() {
        String q15 = "with revenue_view as (\n" +
                "  select\n" +
                "    l.suppkey as supplier_no,\n" +
                "    sum(l.extendedprice * (1 - l.discount)) as total_revenue\n" +
                "  from\n" +
                "    lineitem l \n" +
                "  where\n" +
                "    DATE(l.shipdate) >= DATE('1996-01-01')\n" +
                "    and DATE(l.shipdate) < DATE('1996-04-01')\n" +
                "  group by\n" +
                "    l.suppkey)\n" +
                "select\n" +
                "  s.suppkey,\n" +
                "  s.name,\n" +
                "  s.address,\n" +
                "  s.phone,\n" +
                "  total_revenue\n" +
                "from\n" +
                "  supplier s,\n" +
                "  revenue_view r\n" +
                "where\n" +
                "  s.suppkey = supplier_no\n" +
                "  and total_revenue = (\n" +
                "    select\n" +
                "      max(total_revenue)\n" +
                "    from\n" +
                "      revenue_view\n" +
                "    )\n" +
                "order by\n" +
                "  s.suppkey";

        assertQuery(q15);
    }
}
