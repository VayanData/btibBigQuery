package fr.btib.bigquery;

import fr.btib.connector.timeseries.BTimeSeriesConnector;
import fr.btib.connector.timeseries.exception.TimeSeriesConnectorException;
import fr.btib.connector.timeseries.operation.TimeSeriesExportOperation;
import fr.btib.core.log.BLog;
import fr.btib.core.sformat.BSFormat;
import fr.btib.dataflow.source.table.BSourceTable;
import fr.btib.strategy.datatype.BArtifacts;
import fr.btib.strategy.execution.StrategyExecution;
import fr.btib.structure.engine.memaccess.MemoryAccess;

import javax.baja.sys.Context;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class BIgQueryExportOperation extends TimeSeriesExportOperation
{
    private final Optional<BTimeSeriesConnector> connector;
    private final String dataset;
    private final BSFormat table;
    ////////////////////////////////////////////////////////////////
    // Constructors
    ////////////////////////////////////////////////////////////////

    /**
     * Constructor
     *
     * @param block
     * @param strategyExecution
     * @param log
     * @param context
     * @param connector
     * @param id
     * @param memoryAccess
     * @param artifacts
     * @param dataset
     * @param table
     */
    public BIgQueryExportOperation(BBigQueryExportBlock block, StrategyExecution strategyExecution, BLog log, Context context, Optional<BTimeSeriesConnector> connector, String id, MemoryAccess memoryAccess, BArtifacts artifacts, String dataset, BSFormat table)
    {
        super(id, memoryAccess, artifacts, block, log, context);
        this.connector = connector;
        this.dataset = dataset;
        this.table = table;
    }

    ////////////////////////////////////////////////////////////////
    // TimeSeriesExportOperation
    ////////////////////////////////////////////////////////////////

    @Override
    protected Optional<BTimeSeriesConnector> getTimeSeriesConnector()
    {
        return this.connector;
    }

    @Override
    protected Map<String, String> getConnectorExportOptions(BSourceTable sourceTable) throws TimeSeriesConnectorException
    {
        Map<String, String> options = new HashMap<>();
        options.put(BBigQueryTSConnector.DATASET, this.dataset);
        options.put(BBigQueryTSConnector.TABLE, this.getDestination(sourceTable));
        return options;
    }

    @Override
    protected String getDestination(BSourceTable sourceTable) throws TimeSeriesConnectorException
    {
        // Params
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("origin", sourceTable);
        params.put("base", sourceTable);
        Object tableName = this.table.resolve(params, this.log, this.cx);

        if (tableName == null || tableName.toString().isEmpty())
        {
            throw new TimeSeriesConnectorException("Table name is null or empty");
        }

        return tableName.toString();
    }
}
