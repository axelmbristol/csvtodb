package database;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import entities.ExcelDataRow;
import trikita.log.Log;

import java.util.Comparator;
import java.util.List;

public class CassandraDataBase {
    private static String TAG = CassandraDataBase.class.getName();
    private Cluster cluster;
    private Session session;

    public CassandraDataBase(){
        Log.d(TAG,"cassandra init...");
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();
    }

    public void addEntry(List<ExcelDataRow> excelDataRow){
        excelDataRow.sort(Comparator.comparing(ExcelDataRow::getDate));
        /*String query = String.format("Create keyspace IF NOT EXISTS \"%s\" " +
                "with replication={'class':'SimpleStrategy', 'replication_factor':3}", excelDataRow.getControlStation());
        //Log.d(TAG,"query="+query);
        session.execute(query);

        query = String.format("Create table IF NOT EXISTS" +
                " \"%s\".\"%s\"(" +
                "date Text," +
                "time Text," +
                "control_station Text," +
                "serial_number Text," +
                "signal_strength Int," +
                "battery_voltage Int," +
                "first_sensor_value Int," +
                "second_sensor_values Text," +
                "Primary key( time, date )" +
                ")", excelDataRow.getControlStation(), excelDataRow.getTagSerialNumber());

        //Log.d(TAG,"query="+query);
        session.execute(query);

        query = String.format("Insert into \"%s\".\"%s\"(" +
                        "date,time," +
                        "control_station,serial_number," +
                        "signal_strength,battery_voltage," +
                        "first_sensor_value,second_sensor_values)" +
                        " values(" +
                        "\'%s\',\'%s\'," +
                        "\'%s\',\'%s\'," +
                        "%d,%d," +
                        "%d,\'%s\'" +
                        ")",
                String.valueOf(excelDataRow.getControlStation()),
                String.valueOf(excelDataRow.getTagSerialNumber()),

                excelDataRow.getDate(),
                excelDataRow.getTime(),
                String.valueOf(excelDataRow.getControlStation()),
                String.valueOf(excelDataRow.getTagSerialNumber()),
                Integer.valueOf(excelDataRow.getSignalStrength().replace("@","")),
                0,
                excelDataRow.getFirstSensorValue(),
                excelDataRow.getSecondSensorValuesXYZ()
        );*/
        

        BatchStatement batch = new BatchStatement();
        String a = String.format("Insert into \"%s\".\"%s\" (date, time, control_station, serial_number, signal_strength," +
                        " battery_voltage, first_sensor_value, second_sensor_values) values(?, ?, ?, ?, ?, ?, ?, ?)",
                String.valueOf(excelDataRow.get(0).getControlStation()), String.valueOf(excelDataRow.get(0).getTagSerialNumber()));
        PreparedStatement ps = session.prepare(a);
        Log.d(TAG,"query="+a);

        int cpt = 0;
        for (ExcelDataRow row : excelDataRow){

            String query = String.format("Create keyspace IF NOT EXISTS \"%s\" " +
                    "with replication={'class':'SimpleStrategy', 'replication_factor':3}", row.getControlStation());
            //Log.d(TAG,"query="+query);
            session.execute(query);

            query = String.format("Create table IF NOT EXISTS" +
                    " \"%s\".\"%s\"(" +
                    "date Text," +
                    "time Text," +
                    "control_station Text," +
                    "serial_number Text," +
                    "signal_strength Int," +
                    "battery_voltage Int," +
                    "first_sensor_value Int," +
                    "second_sensor_values Text," +
                    "Primary key( time, date )" +
                    ")", row.getControlStation(), row.getTagSerialNumber());

            //Log.d(TAG,"query="+query);
            session.execute(query);



            if(cpt >= 250){
                //Log.d(TAG,"reached max size.");
                session.execute(batch);
                batch.clear();
                batch = new BatchStatement();
            }

            batch.add(ps.bind(row.getDate(), row.getTime(), String.valueOf(row.getControlStation()),
                    String.valueOf(row.getTagSerialNumber()), Integer.valueOf(row.getSignalStrength().replace("@",
                            "")), 0, row.getFirstSensorValue(), row.getSecondSensorValuesXYZ() ));
            cpt++;
        }
        Log.d(TAG,"execute batch...");
        session.execute(batch);
        batch.clear();

        //Log.d(TAG,"query="+query);
        //session.execute(query);
    }

}
