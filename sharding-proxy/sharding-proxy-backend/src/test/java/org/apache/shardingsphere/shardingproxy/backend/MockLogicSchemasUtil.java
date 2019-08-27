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

package org.apache.shardingsphere.shardingproxy.backend;

import lombok.SneakyThrows;
import org.apache.shardingsphere.core.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetas;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.shardingproxy.backend.schema.LogicSchema;
import org.apache.shardingsphere.shardingproxy.backend.schema.LogicSchemas;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class MockLogicSchemasUtil {
    
    /**
     * set logic schemas for global registry.
     * @param prefix prefix of schema
     * @param size size of schemas
     */
    @SneakyThrows
    public static void setLogicSchemas(final String prefix, final int size) {
        Field field = LogicSchemas.getInstance().getClass().getDeclaredField("logicSchemas");
        field.setAccessible(true);
        field.set(LogicSchemas.getInstance(), mockLogicSchemas(prefix, size));
    }
    
    private static Map<String, LogicSchema> mockLogicSchemas(final String prefix, final int size) {
        Map<String, LogicSchema> result = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            result.put(prefix + "_" + i, mock(LogicSchema.class));
        }
        return result;
    }
    
    /**
     * set logic schemas with tableMetas for global registry.
     * @param prefix prefix of schema
     * @param size size of schemas
     */
    @SneakyThrows
    public static void setLogicSchemasWithTableMetas(final String prefix, final int size) {
        Field field = LogicSchemas.getInstance().getClass().getDeclaredField("logicSchemas");
        field.setAccessible(true);
        field.set(LogicSchemas.getInstance(), mockLogicSchemasWithTableMetas(prefix, size));
    }
    
    private static Map<String, LogicSchema> mockLogicSchemasWithTableMetas(final String prefix, final int size) {
        Map<String, LogicSchema> result = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            result.put(prefix + "_" + i, getLogicSchema());
        }
        return result;
    }
    
    private static LogicSchema getLogicSchema() {
        LogicSchema result = mock(LogicSchema.class);
        doReturn(getShardingSphereMetaData()).when(result).getMetaData();
        return result;
    }
    
    private static ShardingSphereMetaData getShardingSphereMetaData() {
        ShardingSphereMetaData result = mock(ShardingSphereMetaData.class);
        doReturn(getTableMetas()).when(result).getTables();
        return result;
    }
    
    private static TableMetas getTableMetas() {
        final TableMetas result = mock(TableMetas.class);
        TableMetaData orderTableMetaData = mock(TableMetaData.class);
        when(orderTableMetaData.getType()).thenReturn("TABLE");
        TableMetaData orderItemTableMetaData = mock(TableMetaData.class);
        when(orderItemTableMetaData.getType()).thenReturn("VIEW");
        TableMetaData userTableMetaData = mock(TableMetaData.class);
        when(userTableMetaData.getType()).thenReturn("INFORMATION_SCHEMA");
        
        when(result.get("t_order")).thenReturn(orderTableMetaData);
        when(result.get("t_order_item")).thenReturn(orderItemTableMetaData);
        when(result.get("t_user")).thenReturn(userTableMetaData);
        return result;
    }
    
    /**
     * Set logic tables on logic schemas for global registry.
     * @param tables tables
     * @param logicSchemaName logicSchemaName
     */
    public static void setLogicTablesOnLogicSchemas(final Collection<String> tables, final String logicSchemaName) {
        LogicSchema logicSchema = LogicSchemas.getInstance().getLogicSchema(logicSchemaName);
        if (null == logicSchema) {
            return;
        }
        ShardingRule shardingRule = mockLogicTable(tables);
        when(logicSchema.getShardingRule()).thenReturn(shardingRule);
    }
    
    private static ShardingRule mockLogicTable(final Collection<String> tables) {
        ShardingRule shardingRule = mock(ShardingRule.class);
        when(shardingRule.getLogicTableNames()).thenReturn(tables);
        return shardingRule;
    }
}
