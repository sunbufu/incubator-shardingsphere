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

import com.google.common.base.Strings;
import org.apache.shardingsphere.core.metadata.table.TableMetas;
import org.apache.shardingsphere.core.parse.sql.statement.dal.dialect.mysql.ShowTablesStatement;
import org.apache.shardingsphere.core.parse.util.SQLUtil;
import org.apache.shardingsphere.core.rule.ShardingRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Merged result for show tables.
 *
 * @author zhangliang
 * @author panjuan
 * @author sunbufu
 */
public final class ShowTablesMergedResult extends LocalMergedResultAdapter {
    
    private final ShardingRule shardingRule;
    
    private final ShowTablesStatement showTablesStatement;
    
    private final TableMetas tableMetas;
    
    private Iterator<Map.Entry<String, String>> result;
    
    private Map.Entry<String, String> entry;
    
    public ShowTablesMergedResult(final ShardingRule shardingRule, final ShowTablesStatement showTablesStatement, final TableMetas tableMetas) {
        this.shardingRule = shardingRule;
        this.showTablesStatement = showTablesStatement;
        this.tableMetas = tableMetas;
        merge();
    }
    
    private void merge() {
        String pattern = SQLUtil.getExactlyValue(showTablesStatement.getPattern());
        List<String> logicTableNameList = filterWithPattern(shardingRule.getLogicTableNames(), pattern);
        Map<String, String> logicTableNameTableTypeMap = new LinkedHashMap<>();
        for (String each : logicTableNameList) {
            logicTableNameTableTypeMap.put(each, tableMetas.get(each).getType());
        }
        result = logicTableNameTableTypeMap.entrySet().iterator();
    }
    
    private List<String> filterWithPattern(final Collection<String> collection, final String pattern) {
        if (Strings.isNullOrEmpty(pattern)) {
            return new ArrayList<>(collection);
        }
        List<String> result = new LinkedList<>();
        String regex = pattern;
        regex = regex.replace("?", ".");
        regex = regex.replace("%", ".*");
        for (String each : collection) {
            if (!each.matches(regex)) {
                continue;
            }
            result.add(each);
        }
        return result;
    }
    
    @Override
    public boolean next() {
        if (result.hasNext()) {
            entry = result.next();
            return true;
        }
        return false;
    }
    
    @Override
    public Object getValue(final int columnIndex, final Class<?> type) {
        if (1 == columnIndex) {
            return entry.getKey();
        } else {
            return getTableTypeForShowTablesStatement(entry.getValue());
        }
    }
    
    /**
     * Get TableType for ShowTablesStatement.
     * BASE TABLE for a table, VIEW for a view, or SYSTEM VIEW for an INFORMATION_SCHEMA table.
     *
     * @param tableType table type
     * @return tableType for ShowTablesStatement
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/tables-table.html"/>
     */
    private String getTableTypeForShowTablesStatement(final String tableType) {
        if (Strings.isNullOrEmpty(tableType)) {
            return "UNKNOWN TABLE TYPE";
        }
        switch (tableType) {
            case "TABLE":
                return "BASE TABLE";
            case "VIEW":
                return "VIEW";
            case "INFORMATION_SCHEMA":
                return "SYSTEM VIEW";
            default:
                return "UNKNOWN TABLE TYPE";
        }
    }
}
