package database;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.model.Filters;
import entities.CSVTagData;
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

    private void addTagToDB(String dataBaseName, CSVTagData CSVTagData){
        Log.d("add CSVTagData "+ CSVTagData.getTagSerialNumber()+" to database "+dataBaseName);
        if(mongoClient != null){
            Document farmDocument = createFarmDocument(CSVTagData);
            mongoClient.getDatabase(dataBaseName)
                    .getCollection(farmDocument.get("control_station").toString())
                    .insertOne(farmDocument);
        }else {
            Log.e(TAG,"mongoClient is null !");
        }
    }

    private Document createFarmDocument(CSVTagData CSVTagData){
        return new Document("_id", CSVTagData.getControlStation())
                .append("control_station", CSVTagData.getControlStation())
                .append("animals", createAnimalDocument(CSVTagData))
                ;
    }

    private Document createAnimalDocument(CSVTagData CSVTagData){
        return new Document("_id", CSVTagData.getTagSerialNumber())
                .append("serial_number", CSVTagData.getTagSerialNumber())
                .append("days",createDayDocument(CSVTagData));
    }

    private Document createDayDocument(CSVTagData CSVTagData){
        return new Document("_id", CSVTagData.getDate())
                .append("date", CSVTagData.getDate())
                .append("tags", createTagDocument(CSVTagData));
    }

    private Document createTagDocument(CSVTagData CSVTagData){
        return new Document("_id", CSVTagData.getTime())
                .append("date", CSVTagData.getDate())
                .append("time", CSVTagData.getTime())
                .append("control_station", CSVTagData.getControlStation())
                .append("type_12_tag_messages", CSVTagData.getType12TagMessages())
                .append("serial_number", CSVTagData.getTagSerialNumber())
                .append("signal_strength", CSVTagData.getTagSerialNumber())
                .append("battery_voltage", CSVTagData.getBatteryVoltage())
                .append("first_accelerometer_counter", CSVTagData.getFirstAccelerometerCounter())
                .append("first_sensor_value", CSVTagData.getFirstSensorValue())
                .append("second_accelerometer_counter", CSVTagData.getSecondAccelerometerCounter())
                .append("second_sensor_values_xyz", CSVTagData.getSecondSensorValuesXYZ())
                .append("correlation_identifier", CSVTagData.getCorrelationIdentifier())
                .append("correlation_value", CSVTagData.getCorrelationValue());
    }

    private CSVTagData DocumentToTag(Document doc){
        return new CSVTagData(doc);
    }


    private List<CSVTagData> getAnimalTagData(String dataBaseName, Integer controlStation, Integer serialNumber){
        if(mongoClient != null){
            Iterator<Document> it = mongoClient.getDatabase(dataBaseName).getCollection(controlStation.toString())
                    .find(Filters.and(eq("serial_number",serialNumber),exists("second_sensor_values_xyz"))).iterator();
            List<CSVTagData> CSVTagDatas = new ArrayList<CSVTagData>();
            while(it.hasNext()){
                CSVTagDatas.add(DocumentToTag(it.next()));
            }
            return CSVTagDatas;
        }else {
            Log.e(TAG,"mongoClient is null !");
            return Collections.emptyList();
        }
    }

    private List<CSVTagData> getAllTagsInControlStation(String dataBaseName, Integer controlStation){
        if(mongoClient != null){
            Iterator<Document> it = mongoClient.getDatabase(dataBaseName).getCollection(controlStation.toString())
                    .find(exists("second_sensor_values_xyz")).iterator();
            List<CSVTagData> CSVTagDatas = new ArrayList<CSVTagData>();
            while(it.hasNext()){
                CSVTagDatas.add(DocumentToTag(it.next()));
            }
            return CSVTagDatas;
        }else {
            Log.e(TAG,"mongoClient is null !");
            return Collections.emptyList();
        }
    }

}


