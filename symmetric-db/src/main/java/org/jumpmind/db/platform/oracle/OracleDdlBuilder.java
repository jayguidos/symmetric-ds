/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.db.platform.oracle;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.AddPrimaryKeyChange;
import org.jumpmind.db.alter.ColumnAutoIncrementChange;
import org.jumpmind.db.alter.ColumnDataTypeChange;
import org.jumpmind.db.alter.ColumnDefaultValueChange;
import org.jumpmind.db.alter.ColumnRequiredChange;
import org.jumpmind.db.alter.ColumnSizeChange;
import org.jumpmind.db.alter.CopyColumnValueChange;
import org.jumpmind.db.alter.PrimaryKeyChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.RemovePrimaryKeyChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.ForeignKey.ForeignKeyAction;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.PlatformUtils;

/*
 * The SQL Builder for Oracle.
 */
public class OracleDdlBuilder extends AbstractDdlBuilder {
    protected static final String PREFIX_TRIGGER = "TRG";
    protected static final String PREFIX_SEQUENCE = "SEQ";
    protected static final String ROWID_TYPE = "ROWID";
    protected static final String DATE_TYPE = "DATE";

    public OracleDdlBuilder() {
        super(DatabaseNamesConstants.ORACLE);
        databaseInfo.setMaxIdentifierLength(30);
        databaseInfo.setIdentityStatusReadingSupported(false);
        // Note that the back-mappings are partially done by the model reader,
        // not the driver
        databaseInfo.addNativeTypeMapping(Types.ARRAY, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.BIGINT, "NUMBER(38)", Types.NUMERIC);
        databaseInfo.addNativeTypeMapping(Types.BINARY, "RAW", Types.VARBINARY);
        databaseInfo.addNativeTypeMapping(Types.BIT, "NUMBER(1)", Types.NUMERIC);
        databaseInfo.addNativeTypeMapping(Types.DATE, "DATE", Types.TIMESTAMP);
        databaseInfo.addNativeTypeMapping(Types.DECIMAL, "NUMBER", Types.NUMERIC);
        databaseInfo.addNativeTypeMapping(Types.DISTINCT, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        databaseInfo.addNativeTypeMapping(Types.FLOAT, "FLOAT", Types.DOUBLE);
        databaseInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "CLOB", Types.CLOB);
        databaseInfo.addNativeTypeMapping(Types.NULL, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.NUMERIC, "NUMBER", Types.NUMERIC);
        databaseInfo.addNativeTypeMapping(Types.INTEGER, "NUMBER(22)", Types.NUMERIC);
        databaseInfo.addNativeTypeMapping(Types.OTHER, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.REF, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.SMALLINT, "NUMBER(5)", Types.NUMERIC);
        databaseInfo.addNativeTypeMapping(Types.STRUCT, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.TIME, "TIMESTAMP", Types.TIMESTAMP);
        databaseInfo.addNativeTypeMapping(ColumnTypes.TIMETZ, "TIMESTAMP", Types.TIMESTAMP);
        databaseInfo.addNativeTypeMapping(Types.TIMESTAMP, "TIMESTAMP");
        databaseInfo.addNativeTypeMapping(Types.TINYINT, "NUMBER(3)", Types.NUMERIC);
        databaseInfo.addNativeTypeMapping(Types.VARBINARY, "RAW");
        databaseInfo.addNativeTypeMapping(Types.VARCHAR, "VARCHAR2");
        databaseInfo.addNativeTypeMapping("BOOLEAN", "NUMBER(1)", "BIT");
        databaseInfo.addNativeTypeMapping("DATALINK", "BLOB", "BLOB");
        databaseInfo.addNativeTypeMapping(ColumnTypes.NVARCHAR, "NVARCHAR2", Types.VARCHAR);
        databaseInfo.addNativeTypeMapping(ColumnTypes.LONGNVARCHAR, "NVARCHAR2", Types.VARCHAR);
        databaseInfo.addNativeTypeMapping(ColumnTypes.ORACLE_TIMESTAMPTZ, "TIMESTAMP WITH TIME ZONE");
        databaseInfo.addNativeTypeMapping(ColumnTypes.ORACLE_TIMESTAMPLTZ, "TIMESTAMP WITH LOCAL TIME ZONE");
        databaseInfo.setHasSize(Types.TIMESTAMP, true);
        databaseInfo.setHasSize(ColumnTypes.ORACLE_TIMESTAMPTZ, true);
        databaseInfo.setHasSize(ColumnTypes.ORACLE_TIMESTAMPLTZ, true);
        databaseInfo.setHasSize(Types.TIME, true);
        databaseInfo.setHasSize(ColumnTypes.TIMETZ, true);
        databaseInfo.setDefaultSize(Types.TIMESTAMP, 6);
        databaseInfo.setDefaultSize(Types.CHAR, 254);
        databaseInfo.setDefaultSize(Types.VARCHAR, 254);
        databaseInfo.setDefaultSize(Types.BINARY, 254);
        databaseInfo.setDefaultSize(Types.VARBINARY, 254);
        databaseInfo.setMaxSize("TIMESTAMP", 9);
        databaseInfo.setMaxSize("TIMESTAMP WITH TIME ZONE", 9);
        databaseInfo.setMaxSize("TIMESTAMP WITH LOCAL TIME ZONE", 9);
        databaseInfo.setPrimaryKeyEmbedded(false);
        databaseInfo.setDateOverridesToTimestamp(true);
        databaseInfo.setNonBlankCharColumnSpacePadded(true);
        databaseInfo.setBlankCharColumnSpacePadded(true);
        databaseInfo.setCharColumnSpaceTrimmed(false);
        databaseInfo.setEmptyStringNulled(true);
        databaseInfo.setTriggersCreateOrReplaceSupported(true);
        databaseInfo.setBinaryQuoteStart("0x");
        databaseInfo.setBinaryQuoteEnd("");
        databaseInfo.setFunctionalIndicesSupported(true);
    }

    @Override
    public String mapDefaultValue(Object defaultValue, Column column) {
        String newValue = super.mapDefaultValue(defaultValue, column);
        if (databaseInfo.getDefaultValuesToTranslate().containsKey(defaultValue.toString())) {
            return newValue;
        }
        int typeCode = column.getMappedTypeCode();
        if ((typeCode == Types.TIMESTAMP || typeCode == ColumnTypes.ORACLE_TIMESTAMPTZ
                || typeCode == ColumnTypes.ORACLE_TIMESTAMPLTZ)) {
            if (newValue.startsWith("(") && newValue.endsWith(")")) {
                newValue = newValue.substring(1, newValue.length() - 1);
            }
            if (!column.allPlatformColumnNamesContain("oracle")) {
                String uppercaseValue = newValue.trim().toUpperCase();
                if (uppercaseValue.startsWith("SYSDATE(")) {
                    newValue = "SYSDATE";
                } else if (uppercaseValue.startsWith("CURRENT_TIMESTAMP") || uppercaseValue.startsWith("CURRENT TIMESTAMP")
                        || uppercaseValue.startsWith("GETDATE(") || uppercaseValue.startsWith("SYSDATETIME")
                        || uppercaseValue.startsWith("NOW(") || uppercaseValue.startsWith("LOCALTIME")
                        || uppercaseValue.startsWith("TRANSACTION_TIMESTAMP(") || uppercaseValue.startsWith("STATEMENT_TIMESTAMP(")
                        || uppercaseValue.startsWith("CLOCK_TIMESTAMP(")) {
                    newValue = "SYSTIMESTAMP";
                } else if (uppercaseValue.startsWith("GETUTCDATE(") || uppercaseValue.startsWith("SYSUTCDATETIME(")
                        || uppercaseValue.startsWith("UTC_TIMESTAMP")) {
                    newValue = "SYS_EXTRACT_UTC(SYSTIMESTAMP)";
                }
            }
        }
        return newValue;
    }

    @Override
    protected boolean shouldUseQuotes(String defaultValue, Column column) {
        String defaultValueStr = mapDefaultValue(defaultValue, column);
        while (defaultValueStr != null && defaultValueStr.startsWith("(") && defaultValueStr.endsWith(")")) {
            defaultValueStr = defaultValueStr.substring(1, defaultValueStr.length() - 1);
        }
        return super.shouldUseQuotes(defaultValue, column) && !(defaultValueStr.trim().toUpperCase().startsWith("SYSTIMESTAMP")
                || defaultValueStr.trim().toUpperCase().startsWith("SYS_EXTRACT_UTC(")
                || defaultValueStr.trim().toUpperCase().startsWith("SYS_GUID")
                || defaultValueStr.trim().toUpperCase().startsWith("SYS_CONTEXT")
                || defaultValueStr.trim().toUpperCase().startsWith("NVL"));
    }

    @Override
    protected void createTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        // lets create any sequences
        Column[] columns = table.getAutoIncrementColumns();
        if (!temporary && !recreate) {
            for (int idx = 0; idx < columns.length; idx++) {
                createAutoIncrementSequence(table, columns[idx], ddl);
            }
        }
        super.createTable(table, ddl, temporary, recreate);
        if (!temporary) {
            for (int idx = 0; idx < columns.length; idx++) {
                createAutoIncrementTrigger(table, columns[idx], ddl);
            }
        }
    }

    @Override
    protected void dropTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        if (!temporary && !recreate) {
            // The only difference to the Oracle 8/9 variant is the purge which
            // prevents the table from being moved to the recycle bin (which is
            // new in Oracle 10)
            Column[] columns = table.getAutoIncrementColumns();
            for (int idx = 0; idx < columns.length; idx++) {
                dropAutoIncrementTrigger(table, columns[idx], ddl);
                dropAutoIncrementSequence(table, columns[idx], ddl);
            }
        }
        ddl.append("DROP TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(table));
        ddl.append(" CASCADE CONSTRAINTS PURGE");
        printEndOfStatement(ddl);
    }

    /*
     * Creates the sequence necessary for the auto-increment of the given column.
     */
    protected void createAutoIncrementSequence(Table table, Column column, StringBuilder ddl) {
        ddl.append("CREATE SEQUENCE ");
        printIdentifier(getConstraintName(PREFIX_SEQUENCE, table, column.getName(), null), ddl);
        ddl.append(" CACHE 1000 ORDER");
        printEndOfStatement(ddl);
    }

    /*
     * Creates the trigger necessary for the auto-increment of the given column.
     */
    protected void createAutoIncrementTrigger(Table table, Column column, StringBuilder ddl) {
        String columnName = getColumnName(column);
        String triggerName = getConstraintName(PREFIX_TRIGGER, table, column.getName(), null);
        if (scriptModeOn) {
            // For the script, we output a more nicely formatted version
            ddl.append("CREATE OR REPLACE TRIGGER ");
            printlnIdentifier(triggerName, ddl);
            ddl.append("BEFORE INSERT ON ");
            ddl.append(getFullyQualifiedTableNameShorten(table));
            ddl.append("FOR EACH ROW WHEN (new.");
            printIdentifier(columnName, ddl);
            println(" IS NULL)", ddl);
            println("BEGIN", ddl);
            ddl.append("  SELECT ");
            printIdentifier(getConstraintName(PREFIX_SEQUENCE, table, column.getName(), null), ddl);
            ddl.append(".nextval INTO :new.");
            printIdentifier(columnName, ddl);
            ddl.append(" FROM dual");
            println(databaseInfo.getSqlCommandDelimiter(), ddl);
            ddl.append("END");
            println(databaseInfo.getSqlCommandDelimiter(), ddl);
            println("/", ddl);
            println(ddl);
        } else {
            /**
             * Note that the BEGIN ... SELECT ... END; is all in one line and does not contain a semicolon except for the END-one this way, the tokenizer will
             * not split the statement before the END
             */
            ddl.append("CREATE OR REPLACE TRIGGER ");
            printIdentifier(triggerName, ddl);
            ddl.append(" BEFORE INSERT ON ");
            ddl.append(getFullyQualifiedTableNameShorten(table));
            ddl.append(" FOR EACH ROW WHEN (new.");
            printIdentifier(columnName, ddl);
            println(" IS NULL)", ddl);
            ddl.append("BEGIN SELECT ");
            printIdentifier(getConstraintName(PREFIX_SEQUENCE, table, column.getName(), null), ddl);
            ddl.append(".nextval INTO :new.");
            printIdentifier(columnName, ddl);
            ddl.append(" FROM dual");
            ddl.append(databaseInfo.getSqlCommandDelimiter());
            ddl.append(" END");
            /*
             * It is important that there is a semicolon at the end of the statement (or more precisely, at the end of the PL/SQL block), and thus we put two
             * semicolons here because the tokenizer will remove the one at the end
             */
            ddl.append(databaseInfo.getSqlCommandDelimiter());
            printEndOfStatement(ddl);
        }
    }

    /*
     * Drops the sequence used for the auto-increment of the given column.
     */
    protected void dropAutoIncrementSequence(Table table, Column column, StringBuilder ddl) {
        ddl.append("DROP SEQUENCE ");
        printIdentifier(getConstraintName(PREFIX_SEQUENCE, table, column.getName(), null), ddl);
        printEndOfStatement(ddl);
    }

    /*
     * Drops the trigger used for the auto-increment of the given column.
     */
    protected void dropAutoIncrementTrigger(Table table, Column column, StringBuilder ddl) {
        ddl.append("DROP TRIGGER ");
        printIdentifier(getConstraintName(PREFIX_TRIGGER, table, column.getName(), null), ddl);
        printEndOfStatement(ddl);
    }

    @Override
    public void dropExternalForeignKeys(Table table, StringBuilder ddl) {
        // no need to as we drop the table with CASCASE CONSTRAINTS
    }

    @Override
    protected String getFullyQualifiedIndexNameShorten(Table table, IIndex index) {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotBlank(table.getSchema())) {
            sb.append(getDelimitedIdentifier(table.getSchema())).append(databaseInfo.getSchemaSeparator());
        }
        sb.append(getDelimitedIdentifier(getIndexName(index)));
        return sb.toString();
    }

    /**
     * Index names in Oracle are unique to a schema and hence Oracle does not use the ON <tablename> clause
     */
    @Override
    public void writeExternalIndexDropStmt(Table table, IIndex index, StringBuilder ddl) {
        ddl.append("DROP INDEX ");
        printIdentifier(getIndexName(index), ddl);
        printEndOfStatement(ddl);
    }

    @Override
    protected String getNativeDefaultValue(Column column) {
        if ((column.getMappedTypeCode() == Types.BIT)
                || (PlatformUtils.supportsJava14JdbcTypes() && (column.getMappedTypeCode() == PlatformUtils
                        .determineBooleanTypeCode()))) {
            return getDefaultValueHelper().convert(column.getDefaultValue(),
                    column.getMappedTypeCode(), Types.SMALLINT);
        }
        /*
         * Oracle does not accept ISO formats, so we have to convert an ISO spec if we find one But these are the only formats that we make sure work, every
         * other format has to be database-dependent and thus the user has to ensure that it is correct
         */
        else if (column.getMappedTypeCode() == Types.DATE) {
            if (Pattern.matches("\\d{4}\\-\\d{2}\\-\\d{2}", column.getDefaultValue())) {
                return "TO_DATE('" + column.getDefaultValue() + "', 'YYYY-MM-DD')";
            }
        } else if (column.getMappedTypeCode() == Types.TIME) {
            if (Pattern.matches("\\d{2}:\\d{2}:\\d{2}", column.getDefaultValue())) {
                return "TO_DATE('" + column.getDefaultValue() + "', 'HH24:MI:SS')";
            }
        } else if (column.getMappedTypeCode() == Types.TIMESTAMP) {
            if (Pattern.matches("\\d{4}\\-\\d{2}\\-\\d{2} \\d{2}:\\d{2}:\\d{2}[\\.\\d{1,8}]?",
                    column.getDefaultValue())) {
                return "TO_DATE('" + column.getDefaultValue() + "', 'YYYY-MM-DD HH24:MI:SS')";
            }
        }
        return super.getNativeDefaultValue(column);
    }

    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        // we're using sequences instead
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        Column[] columns = table.getAutoIncrementColumns();
        if (columns.length > 0) {
            StringBuilder result = new StringBuilder();
            result.append("SELECT ");
            for (int idx = 0; idx < columns.length; idx++) {
                if (idx > 0) {
                    result.append(",");
                }
                result.append(getDelimitedIdentifier(getConstraintName(PREFIX_SEQUENCE, table,
                        columns[idx].getName(), null)));
                result.append(".currval");
            }
            result.append(" FROM dual");
            return result.toString();
        } else {
            return null;
        }
    }

    protected void processChange(Database currentModel, Database desiredModel,
            ColumnSizeChange change, StringBuilder ddl) {
        writeTableAlterStmt(change.getChangedTable(), ddl);
        ddl.append(" MODIFY ");
        Column column = change.getChangedColumn();
        column.setSizeAndScale(change.getNewSize(), change.getNewScale());
        printIdentifier(getColumnName(column), ddl);
        ddl.append(" ");
        ddl.append(getSqlType(column));
        printEndOfStatement(ddl);
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes, StringBuilder ddl) {
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            if (change instanceof AddColumnChange) {
                AddColumnChange addColumnChange = (AddColumnChange) change;
                if (addColumnChange.getNewColumn().isRequired()
                        && (addColumnChange.getNewColumn().getDefaultValue() == null)) {
                    // we need to rebuild the full table
                    return;
                }
            } else if (change instanceof ColumnSizeChange) {
                processChange(currentModel, desiredModel, (ColumnSizeChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof ColumnDefaultValueChange) {
                processChange(currentModel, desiredModel, (ColumnDefaultValueChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof ColumnRequiredChange) {
                processChange(currentModel, desiredModel, (ColumnRequiredChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof ColumnAutoIncrementChange) {
                if (processChange(currentModel, desiredModel, (ColumnAutoIncrementChange) change,
                        ddl)) {
                    changeIt.remove();
                }
            }
        }
        // First we drop primary keys as necessary
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            if (change instanceof RemovePrimaryKeyChange) {
                processChange(currentModel, desiredModel, (RemovePrimaryKeyChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof PrimaryKeyChange) {
                PrimaryKeyChange pkChange = (PrimaryKeyChange) change;
                RemovePrimaryKeyChange removePkChange = new RemovePrimaryKeyChange(
                        pkChange.getChangedTable(), pkChange.getOldPrimaryKeyColumns());
                processChange(currentModel, desiredModel, removePkChange, ddl);
            }
        }
        /*
         * Next we add/remove columns While Oracle has an ALTER TABLE MODIFY statement, it is somewhat limited esp. if there is data in the table, so we don't
         * use it
         */
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            if (change instanceof AddColumnChange) {
                processChange(currentModel, desiredModel, (AddColumnChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof RemoveColumnChange) {
                processChange(currentModel, desiredModel, (RemoveColumnChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof CopyColumnValueChange) {
                CopyColumnValueChange copyColumnChange = (CopyColumnValueChange) change;
                processChange(currentModel, desiredModel, copyColumnChange, ddl);
                changeIt.remove();
            }
        }
        // Finally we add primary keys
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            if (change instanceof AddPrimaryKeyChange) {
                processChange(currentModel, desiredModel, (AddPrimaryKeyChange) change, ddl);
                changeIt.remove();
            } else if (change instanceof PrimaryKeyChange) {
                PrimaryKeyChange pkChange = (PrimaryKeyChange) change;
                AddPrimaryKeyChange addPkChange = new AddPrimaryKeyChange(
                        pkChange.getChangedTable(), pkChange.getNewPrimaryKeyColumns());
                processChange(currentModel, desiredModel, addPkChange, ddl);
                changeIt.remove();
            }
        }
        super.processTableStructureChanges(currentModel, desiredModel, sourceTable, targetTable,
                changes, ddl);
    }

    protected void processChange(Database currentModel, Database desiredModel,
            ColumnDefaultValueChange change, StringBuilder ddl) {
        boolean changeNeeded = true;
        String newValue = change.getNewDefaultValue();
        String oldValue = change.getChangedColumn().getDefaultValue();
        if ((oldValue == null && "NULL".equals(newValue))
                || (newValue == null && "NULL".equals(oldValue))) {
            changeNeeded = false;
        }
        if (changeNeeded) {
            writeTableAlterStmt(change.getChangedTable(), ddl);
            ddl.append(" MODIFY (");
            Column column = change.getChangedColumn();
            column.setDefaultValue(change.getNewDefaultValue());
            printIdentifier(getColumnName(column), ddl);
            ddl.append(" DEFAULT ");
            writeColumnDefaultValue(change.getChangedTable(), column, ddl);
            ddl.append(" )");
            printEndOfStatement(ddl);
        }
    }

    protected boolean processChange(Database currentModel, Database desiredModel,
            ColumnAutoIncrementChange change, StringBuilder ddl) {
        boolean autoIncrement = !change.getColumn().isAutoIncrement();
        if (!autoIncrement) {
            dropAutoIncrementTrigger(change.getChangedTable(), change.getColumn(), ddl);
            dropAutoIncrementSequence(change.getChangedTable(), change.getColumn(), ddl);
            return true;
        } else {
            // TODO
            return false;
        }
    }

    protected void processChange(Database currentModel, Database desiredModel,
            ColumnRequiredChange change, StringBuilder ddl) {
        boolean required = !change.getChangedColumn().isRequired();
        writeTableAlterStmt(change.getChangedTable(), ddl);
        ddl.append(" MODIFY (");
        Column column = change.getChangedColumn();
        printIdentifier(getColumnName(column), ddl);
        if (required) {
            ddl.append(" NOT NULL ");
        } else {
            ddl.append(" NULL ");
        }
        ddl.append(" )");
        printEndOfStatement(ddl);
    }

    @Override
    protected boolean writeAlterColumnDataTypeToBigInt(ColumnDataTypeChange change, StringBuilder ddl) {
        writeTableAlterStmt(change.getChangedTable(), ddl);
        ddl.append("MODIFY (");
        Column column = change.getChangedColumn();
        column.setTypeCode(change.getNewTypeCode());
        printIdentifier(getColumnName(column), ddl);
        ddl.append(" ");
        ddl.append(getSqlType(column));
        ddl.append(")");
        printEndOfStatement(ddl);
        return true;
    }

    /*
     * Processes the addition of a column to a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(change.getChangedTable()));
        printIndent(ddl);
        ddl.append("ADD ");
        writeColumn(change.getChangedTable(), change.getNewColumn(), ddl);
        printEndOfStatement(ddl);
        if (change.getNewColumn().isAutoIncrement()) {
            createAutoIncrementSequence(change.getChangedTable(), change.getNewColumn(), ddl);
            createAutoIncrementTrigger(change.getChangedTable(), change.getNewColumn(), ddl);
        }
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /*
     * Processes the removal of a column from a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveColumnChange change, StringBuilder ddl) {
        if (change.getColumn().isAutoIncrement()) {
            dropAutoIncrementTrigger(change.getChangedTable(), change.getColumn(), ddl);
            dropAutoIncrementSequence(change.getChangedTable(), change.getColumn(), ddl);
        }
        ddl.append("ALTER TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(change.getChangedTable()));
        printIndent(ddl);
        ddl.append("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /*
     * Processes the removal of a primary key from a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemovePrimaryKeyChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(change.getChangedTable()));
        printIndent(ddl);
        ddl.append("DROP PRIMARY KEY");
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    @Override
    public String getSqlType(Column column) {
        PlatformColumn platformColumn = column.findPlatformColumn(databaseName);
        if (platformColumn != null && platformColumn.getType() != null) {
            if (platformColumn.getType().equals(ROWID_TYPE)) {
                return ROWID_TYPE;
            } else if (platformColumn.getType().equals(DATE_TYPE)) {
                return DATE_TYPE;
            }
        }
        if (column.getJdbcTypeCode() == ColumnTypes.ORACLE_TIMESTAMPTZ || column.getMappedTypeCode() == ColumnTypes.ORACLE_TIMESTAMPTZ) {
            return "TIMESTAMP(" + column.getSizeAsInt() + ") WITH TIME ZONE";
        } else if (column.getJdbcTypeCode() == ColumnTypes.ORACLE_TIMESTAMPLTZ || column.getMappedTypeCode() == ColumnTypes.ORACLE_TIMESTAMPLTZ) {
            return "TIMESTAMP(" + column.getSizeAsInt() + ") WITH LOCAL TIME ZONE";
        } else {
            return super.getSqlType(column);
        }
    }

    @Override
    protected void writeCascadeAttributesForForeignKeyUpdate(ForeignKey key, StringBuilder ddl) {
        // Oracle does not support ON UPDATE
        return;
    }

    @Override
    protected void writeCascadeAttributesForForeignKeyDelete(ForeignKey key, StringBuilder ddl) {
        // Oracle only supports CASCADE and SET NULL
        if (key.getOnDeleteAction() == ForeignKeyAction.CASCADE || key.getOnDeleteAction() == ForeignKeyAction.SETNULL) {
            super.writeCascadeAttributesForForeignKeyDelete(key, ddl);
        }
    }
}
