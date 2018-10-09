package database;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import entities.CSVTagData;
import org.bson.Document;
import trikita.log.Log;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by Axel on 26/09/2018.
 * class for database init and communication
 */
public class DataBase {
    private static String TAG = DataBase.class.getName();
    private String name;
    private MongoDatabase database;

    public DataBase(String dataBaseName){
        this.name = dataBaseName;
        Log.d(TAG, "mongodb init...");
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
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

            if(isAnimalExists(database.getCollection(collectionName), CSVTagData.getTagSerialNumber())){
                Log.d(TAG,"animal "+ CSVTagData.getTagSerialNumber().toString() +" does exist");
                updateAnimal(CSVTagData, database.getCollection(collectionName));
            }else{
                Log.d(TAG,"animal "+ CSVTagData.getTagSerialNumber().toString() +" does not exist.");
                Log.d(TAG,"adding new animal.");
                addNewAnimal(CSVTagData, database.getCollection(collectionName));
            }
        }
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
            Integer index = getItemIndex(collection, CSVTagData.getTagSerialNumber(), CSVTagData.getDate());
            if(index != null){
                collection.updateOne(Filters.and(
                        eq("animals.serial_number", CSVTagData.getTagSerialNumber())
                        ),
                        Updates.push("animals.$.days."+index+".tagData", createTagDocument(CSVTagData)));
            }else {
                Log.d(TAG,"index is null");
            }

        }else{
            Log.d(TAG, "day does not exist adding new day...");
            collection.updateOne(eq("animals.serial_number", CSVTagData.getTagSerialNumber()),
                    Updates.addToSet("animals.$.days", createDayDocument(CSVTagData)));
        }
    }

    private Integer getItemIndex(MongoCollection<Document> collection, Long serialNumber, String date){
        Integer index = null;
        JsonArray days = getAnimalDays(collection, serialNumber);
        for (int i = days.size() - 1; i >= 0; i--) {
            Log.d(TAG, "i=" + i);
            if (((JsonObject) days.get(i)).get("date").getAsString().equals(date)) {
                index = i;
                break;
            }
        }
        Log.d(TAG,"index="+index+" for date="+date+" serial_number="+serialNumber);
        return index;
    }

    private boolean isCollectionExists(String collectionName){
        return database.listCollectionNames()
                .into(new ArrayList<String>()).contains(collectionName);
    }

    private boolean isAnimalExists(MongoCollection<Document> collection, Long serialNumber){
        return getAnimal(collection, serialNumber).has("serial_number");
    }

    private boolean isDayExists(MongoCollection<Document> collection, Long serialNumber, String day){
        boolean dayExist = false;
        JsonArray days = getAnimalDays(collection, serialNumber);
        for (int i = days.size() - 1; i >= 0; i--) {
            if (((JsonObject) days.get(i)).get("date").getAsString().equals(day)) {
                dayExist = true;
                break;
            }
        }
        Log.d(TAG,"dayExist ? "+dayExist);
        return dayExist;
    }

    private JsonArray getAnimalDays(MongoCollection<Document> collection, Long serialNumber){
        return getAnimal(collection, serialNumber).getAsJsonArray("days");
    }

    private JsonObject getAnimal(MongoCollection<Document> collection, Long serialNumber){
        JsonObject result = new JsonObject();
        JsonArray animals = getAnimalsArray(collection);
        for(JsonElement animal : animals){
            JsonElement json = ((JsonObject) animal).get("serial_number");
            Long sn = ((JsonObject) json).get("$numberLong").getAsLong();
            if (!sn.equals(serialNumber)) continue;
            result = (JsonObject)animal;
        }
        Log.d(TAG,"getAnimal result="+result+" size="+result.size());
        return result;
    }

    private JsonArray getAnimalsArray(MongoCollection<Document> collection){
        return (new JsonParser()).parse(collection.find().first().toJson())
                .getAsJsonObject().getAsJsonArray("animals");
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


}


