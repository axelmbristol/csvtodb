package database;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import entities.CSVTagData;
import org.bson.Document;
import trikita.log.Log;

import java.util.*;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.in;

/**
 * Created by Axel on 26/09/2018.
 * class for database init and communication
 */
public class DataBase {
    private static String TAG = DataBase.class.getName();
    private MongoClient mongoClient;
    private String name;
    private MongoDatabase database;

    public DataBase(String dataBaseName){
        this.name = dataBaseName;
        Log.d(TAG, "mongodb init...");
        mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        database = mongoClient.getDatabase(name);
    }

    public void addData(CSVTagData CSVTagData){
        Log.d("add CSVTagData "+ CSVTagData.getTagSerialNumber()+" to database "+name);

        Document farmDocument = createFarmDocument(CSVTagData);
        String collectionName = "_"+farmDocument.get("control_station").toString(); //need _ for MongoDB collection syntax convention.

        Log.d(TAG,"farm document="+farmDocument.toJson());

        if(!isCollectionExists(collectionName)){
            //collection does not exist
            Log.d(TAG, "collection does not exist. create new collection.");
            database.getCollection(collectionName).insertOne(farmDocument);
        }else{
            //update collection
            Log.d(TAG, "collection already exists. update collection "+collectionName+".");
            Log.d(TAG, "checking if animal exists...");

            if(isAnimalExists(collectionName, CSVTagData.getTagSerialNumber().toString())){
                Log.d(TAG,"animal "+ CSVTagData.getTagSerialNumber().toString() +" does exist");
                updateAnimal(CSVTagData, database.getCollection(collectionName));
            }else{
                Log.d(TAG,"animal "+ CSVTagData.getTagSerialNumber().toString() +" does not exist.");
                Log.d(TAG,"adding new animal.");
                addNewAnimal(CSVTagData, database.getCollection(collectionName));
            }
        }

    }

    private boolean isCollectionExists(String collectionName){
        return database.listCollectionNames()
                .into(new ArrayList<String>()).contains(collectionName);
    }

    private boolean isAnimalExists(String collectionName, String serialNumber){
        return (database.getCollection(collectionName).find(new Document("_id", serialNumber))).first() != null;
    }

    private void addNewAnimal(CSVTagData CSVTagData, MongoCollection collection){
        List<Document> animals = new ArrayList();
        animals.add(createAnimalDocument(CSVTagData));
        collection.updateOne(eq("_id",
                CSVTagData.getControlStation()),
                Updates.addToSet("animals", animals));
    }

    private void updateAnimal(CSVTagData CSVTagData, MongoCollection collection){
        Log.d(TAG, "updateAnimal...");
        if(isDayExists(collection, CSVTagData.getTagSerialNumber().toString(), CSVTagData.getDate())){
            Log.d(TAG, "day exist adding new data...");
            collection.updateOne(Filters.and(eq("serial_number", CSVTagData.getTagSerialNumber()),
                    eq("date", CSVTagData.getDate())),
                    Updates.addToSet("tagData", createTagDocument(CSVTagData)));
        }else{
            Log.d(TAG, "day does not exist adding new day...");
            collection.updateOne(eq("serial_number", CSVTagData.getTagSerialNumber()),
                    Updates.addToSet("days", createDayDocument(CSVTagData)));
        }

    }

    private boolean isDayExists(MongoCollection collection, String serialNumber, String day){
        return collection.find(Filters.and(in("days", new BasicDBObject("date", day)),
                eq("serial_number", serialNumber))
        ).first() != null;
    }

    private Document createFarmDocument(CSVTagData CSVTagData){
        return new Document("_id", CSVTagData.getControlStation())
                .append("control_station", CSVTagData.getControlStation())
                .append("animals", createAnimalDocument(CSVTagData));
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
                .append("time", CSVTagData.getTime())
                .append("type_12_tag_messages", CSVTagData.getType12TagMessages())
                .append("signal_strength", CSVTagData.getSignalStrength())
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


