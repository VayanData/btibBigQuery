package fr.btib.bigquery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import fr.btib.connector.timeseries.BTimeSeriesConnector;
import fr.btib.connector.timeseries.exception.TimeSeriesConnectorException;
import fr.btib.core.BtibLogger;
import fr.btib.core.tool.BtibIconTool;
import fr.btib.core.tool.CompTool;

import javax.baja.nre.annotations.NiagaraProperty;
import javax.baja.nre.annotations.NiagaraType;
import javax.baja.sys.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.AbstractMap;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.stream.Collectors;

@NiagaraType
@NiagaraProperty(
    name = "serviceAccountJson",
    type = "String",
    defaultValue = ""
)
public class BBigQueryTSConnector extends BTimeSeriesConnector
{

    /*+ ------------ BEGIN BAJA AUTO GENERATED CODE ------------ +*/
    /*@ $BBigQueryTSConnector(3690780903)1.0$ @*/
    /* Generated Tue Aug 25 15:09:56 CEST 2020 by Slot-o-Matic (c) Tridium, Inc. 2012 */

    ////////////////////////////////////////////////////////////////
    // Property "serviceAccountJson"
    ////////////////////////////////////////////////////////////////

    /**
     * Slot for the {@code serviceAccountJson} property.
     *
     * @see #getServiceAccountJson
     * @see #setServiceAccountJson
     */
    public static final Property serviceAccountJson = newProperty(0, "", null);

    /**
     * Get the {@code serviceAccountJson} property.
     *
     * @see #serviceAccountJson
     */
    public String getServiceAccountJson()
    {
        return this.getString(serviceAccountJson);
    }

    /**
     * Set the {@code serviceAccountJson} property.
     *
     * @see #serviceAccountJson
     */
    public void setServiceAccountJson(String v)
    {
        this.setString(serviceAccountJson, v, null);
    }

    ////////////////////////////////////////////////////////////////
    // Type
    ////////////////////////////////////////////////////////////////

    @Override
    public Type getType()
    {
        return TYPE;
    }

    public static final Type TYPE = Sys.loadType(BBigQueryTSConnector.class);

    /*+ ------------ END BAJA AUTO GENERATED CODE -------------- +*/

    public static final BtibLogger LOG = BtibLogger.getLogger(TYPE);
    public static final BIcon ICON = BtibIconTool.getComponentIcon(TYPE);

    public static final String DATASET = "dataset";
    public static final String TABLE = "table";

    ////////////////////////////////////////////////////////////////
    // BExternalConnector
    ////////////////////////////////////////////////////////////////

    @Override
    public void doPing()
    {
        this.setLastAttempt(BAbsTime.now());
        try
        {
            BigQuery bigQuery = this.getBigQueryClient();
            bigQuery.listDatasets(BigQuery.DatasetListOption.all());
            this.setLastSuccess(BAbsTime.now());
            CompTool.setOk(this);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            this.setLastFailure(BAbsTime.now());
            CompTool.setFault(this, e.getMessage(), e, LOG);
        }
    }

    ////////////////////////////////////////////////////////////////
    // BTimeSeriesConnector
    ////////////////////////////////////////////////////////////////

    @Override
    protected void export_(List<Map<String, Object>> data, Map<String, String> options) throws TimeSeriesConnectorException
    {
        String dataset = options.get(DATASET);
        if (dataset == null || dataset.isEmpty())
        {
            throw new TimeSeriesConnectorException("Dataset is null or empty");
        }

        String table = options.get(DATASET);
        if (table == null || table.isEmpty())
        {
            throw new TimeSeriesConnectorException("Table is null or empty");
        }

        if (data.isEmpty())
        {
            return;
        }

        try
        {
            BigQuery bigQueryClient = this.getBigQueryClient();
            this.doPrivileged(() -> {
                this.createDatasetIfNeeded(bigQueryClient, dataset);
                this.createOrUpdateTable(bigQueryClient, dataset, table, data.get(0));
                this.writeData(bigQueryClient, data, dataset, table);
                return null;
            });
        }
        catch (Exception e)
        {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new TimeSeriesConnectorException(e);
        }
    }

    @Override
    protected void reset_(Map<String, String> options) throws TimeSeriesConnectorException
    {
        String dataset = options.get(DATASET);
        if (dataset == null || dataset.isEmpty())
        {
            throw new TimeSeriesConnectorException("Dataset is null or empty");
        }

        String table = options.get(DATASET);
        if (table == null || table.isEmpty())
        {
            throw new TimeSeriesConnectorException("Table is null or empty");
        }

        try
        {
            this.doPrivileged(() -> {
                BigQuery bigQueryClient = this.getBigQueryClient();
                Table bqTable = bigQueryClient.getTable(dataset, table);
                if (bqTable != null)
                {
                    bqTable.delete();
                }
                return null;
            });
        }
        catch (Exception e)
        {
            throw new TimeSeriesConnectorException(e);
        }
    }

    @Override
    public BtibLogger getBtibLogger()
    {
        return LOG;
    }

    ////////////////////////////////////////////////////////////////
    // Utils
    ////////////////////////////////////////////////////////////////

    /**
     * Build the bigquery client
     *
     * @return
     * @throws IOException
     */
    private BigQuery getBigQueryClient() throws Exception
    {
        return this.doPrivileged(() -> {
            ByteArrayInputStream credentialsStream = new ByteArrayInputStream(this.getServiceAccountJson().getBytes());
            return BigQueryOptions.newBuilder()
                .setCredentials(GoogleCredentials.fromStream(credentialsStream))
                .build()
                .getService();
        });
    }

    /**
     * Writes data to bigquery
     *
     * @param bigQueryClient
     * @param data
     * @param dataset
     * @param table
     */
    private void writeData(BigQuery bigQueryClient, List<Map<String, Object>> data, String dataset, String table)
    {
        Table bqTable = bigQueryClient.getTable(dataset, table);
        Iterable<InsertAllRequest.RowToInsert> rows = data.stream().map(element -> convertToBQType(element)).map(InsertAllRequest.RowToInsert::of).collect(Collectors.toList());
        InsertAllResponse response = bqTable.insert(rows, true, true);
        response.getInsertErrors().forEach((key, value) -> System.out.println(value.stream().map(BigQueryError::getMessage).collect(Collectors.joining("\n"))));
    }

    /**
     * Converts types
     *
     * @param element
     * @return
     */
    private Map<String, Object> convertToBQType(Map<String, Object> element)
    {
        return element.entrySet().stream()
            .map(entry -> {
                if (entry.getValue() instanceof Number)
                {
                    Number value = (Number) entry.getValue();
                    return new AbstractMap.SimpleEntry<>(entry.getKey(), value.doubleValue());
                }
                if (entry.getValue() instanceof Date)
                {
                    Date value = (Date) entry.getValue();
                    return new AbstractMap.SimpleEntry<>(entry.getKey(), value.toString());
                }
                if (entry.getValue() == null)
                {
                    ;
                    return new AbstractMap.SimpleEntry<>(entry.getKey(), "null");
                }
                return entry;
            })
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Creates or update the table
     *
     * @param bigQueryClient
     * @param dataset
     * @param table
     * @param element
     */
    private void createOrUpdateTable(BigQuery bigQueryClient, String dataset, String table, Map<String, Object> element)
    {
        TableInfo tableInfo = this.buildTableInfo(dataset, table, element);
        Table bqTable = bigQueryClient.getTable(dataset, table);
        if (bqTable == null || !bqTable.exists())
        {
            bigQueryClient.create(tableInfo);
        }
        else
        {
            bigQueryClient.update(tableInfo);
        }
    }

    /**
     * Build the table
     *
     * @param dataset
     * @param table
     * @param element
     * @return
     */
    private TableInfo buildTableInfo(String dataset, String table, Map<String, Object> element)
    {
        List<Field> fields = element.entrySet().stream().map((entry) -> Field.of(entry.getKey(), this.getBigQueryType(entry.getValue()))).collect(Collectors.toList());
        Schema schema = Schema.of(fields);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        return TableInfo.newBuilder(TableId.of(dataset, table), tableDefinition).build();
    }

    /**
     * Create the dataset if not exists
     *
     * @param bigQueryClient
     * @param dataset
     */
    private void createDatasetIfNeeded(BigQuery bigQueryClient, String dataset)
    {
        Dataset bqDataset = bigQueryClient.getDataset(dataset);
        if (bqDataset == null)
        {
            bigQueryClient.create(DatasetInfo.newBuilder(dataset).build());
        }
    }

    /**
     * Map java types to bigquery types
     *
     * @param databaseSystem
     * @param value
     * @return
     */
    private StandardSQLTypeName getBigQueryType(Object value)
    {
        if (value instanceof Number)
        {
            return StandardSQLTypeName.FLOAT64;
        }
        if (value instanceof Boolean)
        {
            return StandardSQLTypeName.BOOL;
        }
        return StandardSQLTypeName.STRING;
    }

    /**
     * Run code in a privileged context with exception handling
     *
     * @param callable
     * @param <T>
     * @return
     * @throws Exception
     */
    protected <T> T doPrivileged(Callable<T> callable) throws Exception
    {
        AtomicReference<Exception> exception = new AtomicReference<>();
        T returnValue = AccessController.doPrivileged((PrivilegedAction<T>) () -> {
            try
            {
                return callable.call();
            }
            catch (Exception e)
            {
                exception.set(e);
            }
            return null;
        });
        if (exception.get() != null)
        {
            throw exception.get();
        }
        return returnValue;
    }

    ////////////////////////////////////////////////////////////////
    // Getters / Setters
    ////////////////////////////////////////////////////////////////

    @Override
    public BIcon getIcon()
    {
        return ICON;
    }
}
