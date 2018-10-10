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
import entities.ExcelDataRow;
import org.bson.Document;
import trikita.log.Log;

import javax.print.Doc;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.mongodb.client.model.Filters.eq;
import static utils.Utils.humanReadableFormat;
/**
 * Created by Axel on 26/09/2018.
 * class for database init and communication
 */
public class DataBase {
    private static String TAG = DataBase.class.getName();
    private String name;
    private MongoDatabase database;
    private boolean dataExist;

    public DataBase(String dataBaseName){
        this.name = dataBaseName;
        Log.d(TAG, "mongodb init...");
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        database = mongoClient.getDatabase(name);
    }

    public void addData(ExcelDataRow ExcelDataRow, double currEntryNumb, double totalEntryNumb){
        Instant start = Instant.now();
        Document farmDocument = createFarmDocument(ExcelDataRow);
        String collectionName = "_"+farmDocument.get("control_station").toString(); //need _ for MongoDB collection syntax convention.

        //Log.d(TAG,"farm document="+farmDocument.toJson());

        if(!isCollectionExists(collectionName)){
            //collection does not exist
            //Log.d(TAG, "collection does not exist. create new collection.");
            database.getCollection(collectionName).insertOne(farmDocument);
        }else{
            //update collection
            //Log.d(TAG, "collection already exists. update collection "+collectionName+".");
            //Log.d(TAG, "checking if animal exists...");

            if(isAnimalExists(database.getCollection(collectionName), ExcelDataRow.getTagSerialNumber())){
                //Log.d(TAG,"animal "+ ExcelDataRow.getTagSerialNumber().toString() +" does exist");
                updateAnimal(ExcelDataRow, database.getCollection(collectionName));
            }else{
                //Log.d(TAG,"animal "+ ExcelDataRow.getTagSerialNumber().toString() +" does not exist.");
                //Log.d(TAG,"adding new animal.");
                addNewAnimal(ExcelDataRow, database.getCollection(collectionName));
            }
        }
        Instant end = Instant.now();

        System.out.println(String.format("progress:%.0f/%.0f  time:%6s  animal: %d...", currEntryNumb, totalEntryNumb ,
                humanReadableFormat(Duration.between(start, end)), ExcelDataRow.getTagSerialNumber()));
    }

    private void addNewAnimal(ExcelDataRow ExcelDataRow, MongoCollection collection){
        collection.updateOne(eq("_id",
                ExcelDataRow.getControlStation()),
                Updates.addToSet("animals", createAnimalDocument(ExcelDataRow)));
    }

    private void updateAnimal(ExcelDataRow ExcelDataRow, MongoCollection<Document> collection){
        //Log.d(TAG, "updateAnimal...");
        if(isDayExists(collection, ExcelDataRow.getTagSerialNumber(), ExcelDataRow.getDate(), ExcelDataRow.getTime())){
            //Log.d(TAG, "day exist adding new data...");
            if(!dataExist){
                Integer index = getItemIndex(collection, ExcelDataRow.getTagSerialNumber(), ExcelDataRow.getDate());
                if(index != null){
                    collection.updateOne(Filters.and(
                            eq("animals.serial_number", ExcelDataRow.getTagSerialNumber())
                            ),
                            Updates.push("animals.$.days."+index+".tagData", createTagDocument(ExcelDataRow)));
                }
            }else{
                Log.d(TAG, "data exists.");
            }

        }else{
            //Log.d(TAG, "day does not exist adding new day...");
            collection.updateOne(eq("animals.serial_number", ExcelDataRow.getTagSerialNumber()),
                    Updates.addToSet("animals.$.days", createDayDocument(ExcelDataRow)));
        }
    }

    private Integer getItemIndex(MongoCollection<Document> collection, Long serialNumber, String date){
        Integer index = null;
        JsonArray days = getAnimalDays(collection, serialNumber);
        for (int i = days.size() - 1; i >= 0; i--) {
            //Log.d(TAG, "i=" + i);
            if (((JsonObject) days.get(i)).get("date").getAsString().equals(date)) {
                index = i;
                break;
            }
        }
        //Log.d(TAG,"index="+index+" for date="+date+" serial_number="+serialNumber);
        return index;
    }

    private boolean isCollectionExists(String collectionName){
        return database.listCollectionNames()
                .into(new ArrayList<>()).contains(collectionName);
    }

    private boolean isAnimalExists(MongoCollection<Document> collection, Long serialNumber){
        return getAnimal(collection, serialNumber).has("serial_number");
    }

    private boolean isDayExists(MongoCollection<Document> collection, Long serialNumber, String day, String time){
        boolean dayExist = false;
        dataExist = false;
        JsonArray days = getAnimalDays(collection, serialNumber);
        for (int i = days.size() - 1; i >= 0; i--) {
            if (((JsonObject) days.get(i)).get("date").getAsString().equals(day)) {
                dayExist = true;

                JsonArray tagData = ((JsonObject) days.get(i)).getAsJsonArray("tagData");
                for (int j = tagData.size() - 1; j >= 0; j--) {
                    if (((JsonObject) tagData.get(j)).get("time").getAsString().equals(time)) {
                        dataExist = true;
                        break;
                    }
                }

                break;
            }
        }
        //Log.d(TAG,"dayExist ? "+dayExist);
        return dayExist;
    }

    private boolean isAnimalDataExist(MongoCollection<Document> collection, Long serialNumber, String date, String time){
        boolean dataExist = false;
        JsonArray days = getAnimalDays(collection, serialNumber);
        for (int i = days.size() - 1; i >= 0; i--) {
            if (((JsonObject) days.get(i)).get("date").getAsString().equals(date)) {
                JsonArray tagData = ((JsonObject) days.get(i)).getAsJsonArray("tagData");
                for (int j = tagData.size() - 1; j >= 0; j--) {
                    if (((JsonObject) tagData.get(j)).get("time").getAsString().equals(time)) {
                        dataExist = true;
                        break;
                    }
                }
                break;
            }
        }
        return dataExist;
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
            break;
        }
        //Log.d(TAG,"getAnimal result="+result+" size="+result.size());
        return result;
    }

    private JsonArray getAnimalsArray(MongoCollection<Document> collection){
        return (new JsonParser()).parse(Objects.requireNonNull(collection.find().first()).toJson())
                .getAsJsonObject().getAsJsonArray("animals");
    }

    private Document createFarmDocument(ExcelDataRow ExcelDataRow){
        List<BasicDBObject> animals = new ArrayList<>();
        animals.add(createAnimalDocument(ExcelDataRow));
        return new Document("_id", ExcelDataRow.getControlStation())
                .append("control_station", ExcelDataRow.getControlStation())
                .append("animals", animals);
    }

    private BasicDBObject createAnimalDocument(ExcelDataRow ExcelDataRow){
        List<BasicDBObject> days = new ArrayList<>();
        days.add(createDayDocument(ExcelDataRow));
        return new BasicDBObject("_id", ExcelDataRow.getTagSerialNumber())
                .append("serial_number", ExcelDataRow.getTagSerialNumber())
                .append("days",days);
    }

    private BasicDBObject createDayDocument(ExcelDataRow ExcelDataRow){
        List<BasicDBObject> tags = new ArrayList<>();
        tags.add(createTagDocument(ExcelDataRow));
        return new BasicDBObject("_id", ExcelDataRow.getDate())
                .append("date", ExcelDataRow.getDate())
                .append("tagData", tags);
    }

    private BasicDBObject createTagDocument(ExcelDataRow ExcelDataRow){
        return new BasicDBObject("_id", ExcelDataRow.getTime())
                .append("time", ExcelDataRow.getTime())
                .append("type_12_tag_messages", ExcelDataRow.getType12TagMessages())
                .append("signal_strength", ExcelDataRow.getSignalStrength())
                .append("battery_voltage", ExcelDataRow.getBatteryVoltage())
                .append("first_accelerometer_counter", ExcelDataRow.getFirstAccelerometerCounter())
                .append("first_sensor_value", ExcelDataRow.getFirstSensorValue())
                .append("second_accelerometer_counter", ExcelDataRow.getSecondAccelerometerCounter())
                .append("second_sensor_values_xyz", ExcelDataRow.getSecondSensorValuesXYZ())
                .append("correlation_identifier", ExcelDataRow.getCorrelationIdentifier())
                .append("correlation_value", ExcelDataRow.getCorrelationValue());
    }


}


