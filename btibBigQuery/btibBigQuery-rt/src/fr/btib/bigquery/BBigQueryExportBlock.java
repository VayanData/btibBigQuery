package fr.btib.bigquery;

import fr.btib.connector.timeseries.BTimeSeriesConnector;
import fr.btib.connector.timeseries.block.BTimeSeriesBlock;
import fr.btib.core.log.BLog;
import fr.btib.core.sformat.BSFormat;
import fr.btib.core.tool.BtibIconTool;
import fr.btib.strategy.datatype.BArtifacts;
import fr.btib.strategy.execution.StrategyExecution;
import fr.btib.structure.engine.memaccess.MemoryAccess;
import fr.btib.structure.engine.operation.Operation;

import javax.baja.nre.annotations.Facet;
import javax.baja.nre.annotations.NiagaraProperty;
import javax.baja.nre.annotations.NiagaraType;
import javax.baja.sys.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@NiagaraType
@NiagaraProperty(
    name = "bigQueryConnector",
    type = "String",
    defaultValue = "",
    facets = @Facet(name = "BFacets.FIELD_EDITOR", value = "\"btibBigQuery:BigQueryTSConnectorFE\"")
)
@NiagaraProperty(
    name = "dataset",
    type = "String",
    defaultValue = "niagara"
)
@NiagaraProperty(
    name = "table",
    type = "BSFormat",
    defaultValue = "BSFormat.make(\"export\")"
)
public class BBigQueryExportBlock extends BTimeSeriesBlock
{
    /*+ ------------ BEGIN BAJA AUTO GENERATED CODE ------------ +*/
    /*@ $BBigQueryExportBlock(3664760139)1.0$ @*/
    /* Generated Tue Aug 25 17:33:51 CEST 2020 by Slot-o-Matic (c) Tridium, Inc. 2012 */

    ////////////////////////////////////////////////////////////////
    // Property "bigQueryConnector"
    ////////////////////////////////////////////////////////////////

    /**
     * Slot for the {@code bigQueryConnector} property.
     *
     * @see #getBigQueryConnector
     * @see #setBigQueryConnector
     */
    public static final Property bigQueryConnector = newProperty(0, "", BFacets.make(BFacets.FIELD_EDITOR, "bigquery:BigQueryTSConnectorFE"));

    /**
     * Get the {@code bigQueryConnector} property.
     *
     * @see #bigQueryConnector
     */
    public String getBigQueryConnector()
    {
        return this.getString(bigQueryConnector);
    }

    /**
     * Set the {@code bigQueryConnector} property.
     *
     * @see #bigQueryConnector
     */
    public void setBigQueryConnector(String v)
    {
        this.setString(bigQueryConnector, v, null);
    }

    ////////////////////////////////////////////////////////////////
    // Property "dataset"
    ////////////////////////////////////////////////////////////////

    /**
     * Slot for the {@code dataset} property.
     *
     * @see #getDataset
     * @see #setDataset
     */
    public static final Property dataset = newProperty(0, "niagara", null);

    /**
     * Get the {@code dataset} property.
     *
     * @see #dataset
     */
    public String getDataset()
    {
        return this.getString(dataset);
    }

    /**
     * Set the {@code dataset} property.
     *
     * @see #dataset
     */
    public void setDataset(String v)
    {
        this.setString(dataset, v, null);
    }

    ////////////////////////////////////////////////////////////////
    // Property "table"
    ////////////////////////////////////////////////////////////////

    /**
     * Slot for the {@code table} property.
     *
     * @see #getTable
     * @see #setTable
     */
    public static final Property table = newProperty(0, BSFormat.make("export"), null);

    /**
     * Get the {@code table} property.
     *
     * @see #table
     */
    public BSFormat getTable()
    {
        return (BSFormat) this.get(table);
    }

    /**
     * Set the {@code table} property.
     *
     * @see #table
     */
    public void setTable(BSFormat v)
    {
        this.set(table, v, null);
    }

    ////////////////////////////////////////////////////////////////
    // Type
    ////////////////////////////////////////////////////////////////

    @Override
    public Type getType()
    {
        return TYPE;
    }

    public static final Type TYPE = Sys.loadType(BBigQueryExportBlock.class);

    /*+ ------------ END BAJA AUTO GENERATED CODE -------------- +*/

    public static final BIcon ICON = BtibIconTool.getComponentIcon(TYPE);

    ////////////////////////////////////////////////////////////////
    // BTimeSeriesBlock
    ////////////////////////////////////////////////////////////////


    @Override
    protected String getConnectorHandle()
    {
        return this.getBigQueryConnector();
    }

    @Override
    protected Map<String, String> getConnectorResetOptions(String destination)
    {
        Map<String, String> options = new HashMap<>();
        options.put(BBigQueryTSConnector.DATASET, this.getDataset());
        options.put(BBigQueryTSConnector.TABLE, destination);
        return options;
    }

    @Override
    protected Operation makeOperation_(StrategyExecution strategyExecution, BLog log, Context context, Optional<BTimeSeriesConnector> connector, String id, MemoryAccess memoryAccess, BArtifacts artifacts)
    {
        return new BIgQueryExportOperation(this , strategyExecution, log, context, connector, id, memoryAccess, artifacts, this.getDataset(), this.getTable());
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
