package database;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.model.Filters;
import entities.TagData;
import trikita.log.Log;
import org.bson.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;

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

    private void addTagToDB(String dataBaseName, TagData tagData){
        Log.d("add tagData "+ tagData.getTagSerialNumber()+" to database "+dataBaseName);
        if(mongoClient != null){
            Document dbTagObject = createFarmDocument(tagData);
            mongoClient.getDatabase(dataBaseName)
                    .getCollection(dbTagObject.get("control_station").toString())
                    .insertOne(dbTagObject);
        }else {
            Log.e(TAG,"mongoClient is null !");
        }
    }

    private Document createFarmDocument(TagData tagData){
        return new Document("_id", tagData.getControlStation())
                .append("control_station", tagData.getControlStation())
                .append("animals", createAnimalDocument(tagData))
                ;
    }

    private Document createAnimalDocument(TagData tagData){
        return new Document("_id", tagData.getTagSerialNumber())
                .append("serial_number", tagData.getTagSerialNumber())
                .append("days",createDayDocument(tagData));
    }

    private Document createDayDocument(TagData tagData){
        return new Document("_id", tagData.getDate())
                .append("date", tagData.getDate())
                .append("tags", createTagDocument(tagData));
    }

    private Document createTagDocument(TagData tagData){
        return new Document("_id", tagData.getTime())
                .append("date", tagData.getDate())
                .append("time", tagData.getTime())
                .append("control_station", tagData.getControlStation())
                .append("type_12_tag_messages", tagData.getType12TagMessages())
                .append("serial_number", tagData.getTagSerialNumber())
                .append("signal_strength", tagData.getTagSerialNumber())
                .append("battery_voltage", tagData.getBatteryVoltage())
                .append("first_accelerometer_counter", tagData.getFirstAccelerometerCounter())
                .append("first_sensor_value", tagData.getFirstSensorValue())
                .append("second_accelerometer_counter", tagData.getSecondAccelerometerCounter())
                .append("second_sensor_values_xyz", tagData.getSecondSensorValuesXYZ())
                .append("correlation_identifier", tagData.getCorrelationIdentifier())
                .append("correlation_value", tagData.getCorrelationValue());
    }

    private TagData DocumentToTag(Document doc){
        return new TagData(doc);
    }


    private List<TagData> getAnimalTagData(String dataBaseName, Integer controlStation, Integer serialNumber){
        if(mongoClient != null){
            Iterator<Document> it = mongoClient.getDatabase(dataBaseName).getCollection(controlStation.toString())
                    .find(Filters.and(eq("serial_number",serialNumber),exists("second_sensor_values_xyz"))).iterator();
            List<TagData> tagDatas = new ArrayList<TagData>();
            while(it.hasNext()){
                tagDatas.add(DocumentToTag(it.next()));
            }
            return tagDatas;
        }else {
            Log.e(TAG,"mongoClient is null !");
            return Collections.emptyList();
        }
    }

    private List<TagData> getAllTagsInControlStation(String dataBaseName, Integer controlStation){
        if(mongoClient != null){
            Iterator<Document> it = mongoClient.getDatabase(dataBaseName).getCollection(controlStation.toString())
                    .find(exists("second_sensor_values_xyz")).iterator();
            List<TagData> tagDatas = new ArrayList<TagData>();
            while(it.hasNext()){
                tagDatas.add(DocumentToTag(it.next()));
            }
            return tagDatas;
        }else {
            Log.e(TAG,"mongoClient is null !");
            return Collections.emptyList();
        }
    }

}


