package database;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import entities.Tag;
import trikita.log.Log;
import org.bson.*;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Axel on 26/09/2018.
 * class for database init and communication
 */
public class DataBase {
    private static String TAG = DataBase.class.getName();
    private MongoClient mongoClient;

    private void init(){
            Log.d(TAG, "mongodb init...");
            mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
    }

    private void addTagToDB(String dataBaseName, Tag tag){
        Log.d("add tag "+tag.getTagSerialNumber()+" to database "+dataBaseName);
        if(mongoClient != null){
            Document dbTagObject = createTagDBObject(tag);
            mongoClient.getDatabase(dataBaseName).getCollection(dbTagObject.get("control_station").toString())

                    .getCollection(dbTagObject.get("serial_number").toString())
                    .getCollection(dbTagObject.get("date").toString())

        }else {
            Log.e(TAG,"mongoClient is null !");
        }
    }

    private Document createTagDBObject(Tag tag){
        Log.d(TAG, "createTAgDBObject...");
        return new Document("date", tag.getDate())
                .append("time", tag.getTime())
                .append("control_station", tag.getControlStation())
                .append("type_12_tag_messages", tag.getType12TagMessages())
                .append("serial_number", tag.getTagSerialNumber())
                .append("signal_strength", tag.getTagSerialNumber())
                .append("battery_voltage", tag.getBatteryVoltage())
                .append("first_accelerometer_counter", tag.getFirstAccelerometerCounter())
                .append("first_sensor_value", tag.getFirstSensorValue())
                .append("second_accelerometer_counter", tag.getSecondAccelerometerCounter())
                .append("second_sensor_values_xyz", tag.getSecondSensorValuesXYZ())
                .append("correlation_identifier", tag.getCorrelationIdentifier())
                .append("correlation_value", tag.getCorrelationValue());
    }

    private Tag DBObjectToTag(DBObject dbObject){
        return new Tag((BasicDBObject) dbObject);
    }

    private List<Tag> getTagFromDBBySerialNumber(String dataBaseName, Long serialNumber){
        if(mongoClient != null){
            Iterator<DBObject> it = mongoClient.getDB(dataBaseName).getCollection(serialNumber.toString()).find().iterator();
            List<Tag> tags = new ArrayList<Tag>();
            while(it.hasNext()){
                tags.add(DBObjectToTag(it.next()));
            }
            Log.d(TAG,"getTagFromDBBySerialNumbertags="+tags);
            return tags;
        }else {
            Log.e(TAG,"mongoClient is null !");
            return Collections.emptyList();
        }
    }


    private List<Tag> getTagFromDBByControlStation(String dataBaseName, Integer controlStation){
        if(mongoClient != null){
            Iterator<DBObject> it = mongoClient.getDB(dataBaseName).getCollection(controlStation.toString()).find().iterator();
            List<Tag> tags = new ArrayList<Tag>();
            while(it.hasNext()){
                tags.add(DBObjectToTag(it.next()));
            }
            return tags;
        }else {
            Log.e(TAG,"mongoClient is null !");
            return Collections.emptyList();
        }
    }

}


