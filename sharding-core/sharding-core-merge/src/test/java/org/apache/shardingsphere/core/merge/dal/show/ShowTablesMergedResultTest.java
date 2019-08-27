/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
 */

package org.apache.shardingsphere.core.merge.dal.show;

import com.google.common.collect.Lists;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetas;
import org.apache.shardingsphere.core.parse.sql.statement.dal.dialect.mysql.ShowTablesStatement;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class ShowTablesMergedResultTest {
    
    @Test
    public void assertResult() {
        ShowTablesMergedResult showTablesMergedResult = new ShowTablesMergedResult(getShardingRule(), getShowTablesStatement(), getTableMetas());
        assertTrue(showTablesMergedResult.next());
        assertThat((String) showTablesMergedResult.getValue(1, String.class), is("t_order"));
        assertThat((String) showTablesMergedResult.getValue(2, String.class), is("BASE TABLE"));
        assertTrue(showTablesMergedResult.next());
        assertThat((String) showTablesMergedResult.getValue(1, String.class), is("t_order_item"));
        assertThat((String) showTablesMergedResult.getValue(2, String.class), is("VIEW"));
        assertFalse(showTablesMergedResult.next());
    }
    
    private ShardingRule getShardingRule() {
        ShardingRule result = mock(ShardingRule.class);
        List<String> logicTableNames = Lists.newArrayList("t_order", "t_order_item", "t_user", "t_unknown");
        when(result.getLogicTableNames()).thenReturn(logicTableNames);
        return result;
    }
    
    private ShowTablesStatement getShowTablesStatement() {
        final ShowTablesStatement result = new ShowTablesStatement();
        result.setFull(true);
        result.setSchema("sharding_db");
        result.setPattern("t_order%");
        return result;
    }
    
    private TableMetas getTableMetas() {
        final TableMetas result = mock(TableMetas.class);
        TableMetaData orderTableMetaData = mock(TableMetaData.class);
        when(orderTableMetaData.getType()).thenReturn("TABLE");
        TableMetaData orderItemTableMetaData = mock(TableMetaData.class);
        when(orderItemTableMetaData.getType()).thenReturn("VIEW");
        TableMetaData userTableMetaData = mock(TableMetaData.class);
        when(userTableMetaData.getType()).thenReturn("INFORMATION_SCHEMA");
        TableMetaData unknownTableMetaData = mock(TableMetaData.class);
        when(unknownTableMetaData.getType()).thenReturn("UNKNOWN");
        
        when(result.get("t_order")).thenReturn(orderTableMetaData);
        when(result.get("t_order_item")).thenReturn(orderItemTableMetaData);
        when(result.get("t_user")).thenReturn(userTableMetaData);
        when(result.get("t_unknown")).thenReturn(unknownTableMetaData);
        return result;
    }
    
    @Test
    public void assertAllResult() {
        ShowTablesMergedResult showTablesMergedResult = new ShowTablesMergedResult(getShardingRule(), getShowTablesStatementWithEmptyPattern(), getTableMetas());
        assertTrue(showTablesMergedResult.next());
        assertThat((String) showTablesMergedResult.getValue(1, String.class), is("t_order"));
        assertThat((String) showTablesMergedResult.getValue(2, String.class), is("BASE TABLE"));
        assertTrue(showTablesMergedResult.next());
        assertThat((String) showTablesMergedResult.getValue(1, String.class), is("t_order_item"));
        assertThat((String) showTablesMergedResult.getValue(2, String.class), is("VIEW"));
        assertTrue(showTablesMergedResult.next());
        assertThat((String) showTablesMergedResult.getValue(1, String.class), is("t_user"));
        assertThat((String) showTablesMergedResult.getValue(2, String.class), is("SYSTEM VIEW"));
        assertTrue(showTablesMergedResult.next());
        assertThat((String) showTablesMergedResult.getValue(1, String.class), is("t_unknown"));
        assertThat((String) showTablesMergedResult.getValue(2, String.class), is("UNKNOWN TABLE TYPE"));
        assertFalse(showTablesMergedResult.next());
    }
    
    private ShowTablesStatement getShowTablesStatementWithEmptyPattern() {
        final ShowTablesStatement result = new ShowTablesStatement();
        result.setFull(true);
        result.setSchema("sharding_db");
        result.setPattern(null);
        return result;
    }
    
    @Test
    public void assertEmptyResult() {
        ShowTablesMergedResult showTablesMergedResult = new ShowTablesMergedResult(getShardingRule(), getShowTablesStatementWithAbsentSchema(), getTableMetas());
        assertFalse(showTablesMergedResult.next());
    }
    
    private ShowTablesStatement getShowTablesStatementWithAbsentSchema() {
        final ShowTablesStatement result = new ShowTablesStatement();
        result.setFull(true);
        result.setSchema("sharding_db");
        result.setPattern("PATTERN");
        return result;
    }
    
    @Test
    public void assertResultWithOutTableType() {
        ShowTablesMergedResult showTablesMergedResult = new ShowTablesMergedResult(getShardingRule(), getShowTablesStatementWithFalseFull(), getTableMetas());
        assertTrue(showTablesMergedResult.next());
        assertThat((String) showTablesMergedResult.getValue(1, String.class), is("t_order"));
        assertTrue(showTablesMergedResult.next());
        assertThat((String) showTablesMergedResult.getValue(1, String.class), is("t_order_item"));
        assertFalse(showTablesMergedResult.next());
    }
    
    private ShowTablesStatement getShowTablesStatementWithFalseFull() {
        final ShowTablesStatement result = new ShowTablesStatement();
        result.setFull(false);
        result.setSchema("sharding_db");
        result.setPattern("t_order%");
        return result;
    }
}
