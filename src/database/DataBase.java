package database;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import entities.Tag;
import trikita.log.Log;

import java.net.UnknownHostException;

/**
 * Created by Axel on 26/09/2018.
 * class for database init and communication
 */
public class DataBase {
    private static String TAG = DataBase.class.getName();
    private MongoClient mongoClient;

    private void init(){
        try {
            Log.d(TAG, "mongodb init...");
            mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));

        } catch (UnknownHostException e) {
            Log.e(TAG,"error while initializing mongoClient", e);
        }
    }

    private void addTagToDB(String dataBaseName, Tag tag){
        Log.d("add tag "+tag.getTagSerialNumber()+" to database "+dataBaseName);
        if(mongoClient != null){
            mongoClient.getDB(dataBaseName).getCollection("data").insert(createTagDBObject(tag));
        }else {
            Log.e(TAG,"mongoClient is null !");
        }

    }

    private DBObject createTagDBObject(Tag tag){
        Log.d(TAG, "createTAgDBObject...");
        return new BasicDBObject("date", tag.getDate())
                .append("time", tag.getTime())
                .append("control_station", tag.getControlStation())
                .append("type_12_tag_messages", tag.getType12TagMessages())
                .append("serial_mumber", tag.getTagSerialNumber())
                .append("signal_strength", tag.getTagSerialNumber())
                .append("battery_voltage", tag.getBatteryVoltage())
                .append("first_accelerometer_counter", tag.getFirstAccelerometerCounter())
                .append("first_sensor_value", tag.getFirstSensorValue())
                .append("second_accelerometer_counter", tag.getSecondAccelerometerCounter())
                .append("second_sensor_values_xyz", tag.getSecondSensorValuesXYZ())
                .append("correlation_identifier", tag.getCorrelationIdentifier())
                .append("correlation_value", tag.getCorrelationValue());
    }

    private Tag getTAgFromDB(){
        //// TODO: 26/09/2018
        return null;
    }

}


