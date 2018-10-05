package database;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import entities.CSVTagData;
import jdk.nashorn.api.scripting.JSObject;
import org.bson.BSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import trikita.log.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static java.lang.System.out;

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

            if(isAnimalExists(collectionName, CSVTagData.getTagSerialNumber())){
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

    private boolean isAnimalExists(String collectionName, Long serialNumber){
        BasicDBObject query = new BasicDBObject("animals.serial_number", serialNumber);
        return database.getCollection(collectionName).find(query).first() != null;
    }

    private void addNewAnimal(CSVTagData CSVTagData, MongoCollection collection){
        collection.updateOne(eq("_id",
                CSVTagData.getControlStation()),
                Updates.addToSet("animals", createAnimalDocument(CSVTagData)));
    }

    private void updateAnimal(CSVTagData CSVTagData, MongoCollection<Document> collection){
        Log.d(TAG, "updateAnimal...");
        if(isDayExists(collection, CSVTagData.getTagSerialNumber(), CSVTagData.getDate())){
            Log.d(TAG, "day exist adding new data...");
            int index = getItemIndex(collection, CSVTagData.getTagSerialNumber(), CSVTagData.getDate());
            collection.updateOne(Filters.and(
                    eq("animals.serial_number", CSVTagData.getTagSerialNumber())
                    ),
                    Updates.push("animals.$.days."+index+".tagData", createTagDocument(CSVTagData)));
        }else{
            Log.d(TAG, "day does not exist adding new day...");
            collection.updateOne(eq("animals.serial_number", CSVTagData.getTagSerialNumber()),
                    Updates.addToSet("animals.$.days", createDayDocument(CSVTagData)));
        }
    }

    private int getItemIndex(MongoCollection<Document> collection, Long serialNumber, String date){
        //Document d = collection.find(elemMatch("animals", Document.parse("{ serial_number:"+serialNumber+"}"))).first();

        Document d = collection.find(elemMatch("animals", eq("serial_number", 40101310285L))).first();

        String json = d.toJson();

        JsonObject jsonObject = (new JsonParser()).parse(d.toJson()).getAsJsonObject();
        JsonArray animals = jsonObject.getAsJsonArray("animals");
        int index = 0;
        for(JsonElement animal : animals){

            //JsonElement id = ((JsonObject) animal).get("serial_number").;

            //if( id != serialNumber) continue;

            JsonArray days = ((JsonObject) animal).getAsJsonArray("days");
            for(int i = 0; i < days.size(); i++){
                if(((JsonObject) days.get(i)).get("date").getAsString().equals(date)){
                    index = i;
                    break;
                }
            }
        }
        Log.d(TAG,"index="+index+" for date="+date);

        return index;
    }



    private boolean isDayExists(MongoCollection<Document> collection, Long serialNumber, String day){
        BasicDBObject query = new BasicDBObject("animals.days.date", day);
        return collection.find(query).first() != null;
    }

    private Document createFarmDocument(CSVTagData CSVTagData){
        List<BasicDBObject> animals = new ArrayList<BasicDBObject>();
        animals.add(createAnimalDocument(CSVTagData));
        return new Document("_id", CSVTagData.getControlStation())
                .append("control_station", CSVTagData.getControlStation())
                .append("animals", animals);
    }

    private BasicDBObject createAnimalDocument(CSVTagData CSVTagData){
        List<BasicDBObject> days = new ArrayList<BasicDBObject>();
        days.add(createDayDocument(CSVTagData));
        return new BasicDBObject("_id", CSVTagData.getTagSerialNumber())
                .append("serial_number", CSVTagData.getTagSerialNumber())
                .append("days",days);
    }

    private BasicDBObject createDayDocument(CSVTagData CSVTagData){
        List<BasicDBObject> tags = new ArrayList<BasicDBObject>();
        tags.add(createTagDocument(CSVTagData));
        return new BasicDBObject("_id", CSVTagData.getDate())
                .append("date", CSVTagData.getDate())
                .append("tagData", tags);
    }

    private BasicDBObject createTagDocument(CSVTagData CSVTagData){
        return new BasicDBObject("_id", CSVTagData.getTime())
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


