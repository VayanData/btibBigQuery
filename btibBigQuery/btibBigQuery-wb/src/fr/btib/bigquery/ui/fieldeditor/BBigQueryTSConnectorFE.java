package fr.btib.bigquery.ui.fieldeditor;

import fr.btib.bigquery.BBigQueryTSConnector;
import fr.btib.connector.BExternalConnector;
import fr.btib.connector.wb.fieldeditor.timeseries.BTimeSeriesConnectorFE;

import javax.baja.nre.annotations.NiagaraType;
import javax.baja.sys.Sys;
import javax.baja.sys.Type;

@NiagaraType
public class BBigQueryTSConnectorFE extends BTimeSeriesConnectorFE
{
    /*+ ------------ BEGIN BAJA AUTO GENERATED CODE ------------ +*/
    /*@ $BBigQueryTSConnectorFE(2979906276)1.0$ @*/
    /* Generated Tue Aug 25 17:16:43 CEST 2020 by Slot-o-Matic (c) Tridium, Inc. 2012 */

    ////////////////////////////////////////////////////////////////
    // Type
    ////////////////////////////////////////////////////////////////

    @Override
    public Type getType()
    {
        return TYPE;
    }

    public static final Type TYPE = Sys.loadType(BBigQueryTSConnectorFE.class);

    /*+ ------------ END BAJA AUTO GENERATED CODE -------------- +*/

    @Override
    public boolean isConnectorValid(BExternalConnector connector)
    {
        return super.isConnectorValid(connector) && connector instanceof BBigQueryTSConnector;
    }
}
