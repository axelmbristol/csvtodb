package database;

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

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
/**
 * Created by Axel on 26/09/2018.
 * class for database init and communication
 */
public class DataBase {
    private static String TAG = DataBase.class.getName();
    private MongoDatabase database;
    private String collectionName;

    public DataBase(String dataBaseName){
        Log.d(TAG, "mongodb init...");
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        Log.d(TAG, "purge...");
        mongoClient.getDatabase(dataBaseName).drop();
        database = mongoClient.getDatabase(dataBaseName);
    }

    public void init(ExcelDataRow excelDataRow){
        Document farmDocument = createFarmDocument(excelDataRow);
        collectionName = "_"+farmDocument.get("control_station")+"-"+excelDataRow.getDate().split(" ")[0]; //need _ for MongoDB collection syntax convention.
        if(!isCollectionExists(collectionName)){
            Log.d(TAG,"creating new farm collection."+collectionName+" "+farmDocument);
            database.getCollection(collectionName).insertOne(farmDocument);
        }else {
            Log.d(TAG,"collection already exists."+collectionName+" "+farmDocument);
        }
    }

    public void addAnimal(ExcelDataRow excelDataRow){
        addNewAnimal(excelDataRow, database.getCollection(collectionName));
    }

    private void addNewAnimal(ExcelDataRow ExcelDataRow, MongoCollection collection){
        collection.updateOne(eq("_id",
                ExcelDataRow.getControlStation()),
                Updates.addToSet("animals", createAnimalDocument(ExcelDataRow)));
    }

    public void addDay(ExcelDataRow ExcelDataRow){
        BasicDBObject d = createDayDocument(ExcelDataRow);
        database.getCollection(collectionName).updateOne(eq("animals.serial_number", ExcelDataRow.getTagSerialNumber()),
                Updates.addToSet("animals.$.days", d));
    }

    public void addEntry(ExcelDataRow ExcelDataRow, int index){
        BasicDBObject d = createTagDocument(ExcelDataRow);
        database.getCollection(collectionName).updateOne(Filters.and(
                eq("animals.serial_number", ExcelDataRow.getTagSerialNumber())
                ),
                Updates.push("animals.$.days."+index+".tagData", d));
    }

    private boolean isCollectionExists(String collectionName){
        List<String> collections = database.listCollectionNames().into(new ArrayList<>());
        Log.d(TAG,"collections="+collections);
        return collections.contains(collectionName);
    }
//
//    private boolean isAnimalExists(MongoCollection<Document> collection, Long serialNumber){
//        Instant start = Instant.now();
//        boolean result = getAnimal(collection, serialNumber).has("serial_number");
//        Instant end = Instant.now();
//        isAnimalExistsTime = humanReadableFormat(Duration.between(start, end));
//        return result;
//    }

//    private boolean isDayExists(MongoCollection<Document> collection, Long serialNumber, String day, String time){
//
//        boolean dayExist = false;
//        //dataExist = false;
//        getAnimalDays(collection, serialNumber);
//        for (int i = days.size() - 1; i >= 0; i--) {
//            if (((JsonObject) days.get(i)).get("date").getAsString().equals(day)) {
//                dayExist = true;
////                JsonArray tagData = ((JsonObject) days.get(i)).getAsJsonArray("tagData");
////                if (((JsonObject) tagData.get(0)).get("time").getAsString().equals(time)) {
////                    //dataExist = true;
////                    cpt2 = i;
////                    break;
////                }
//                cpt2 = i;
//                break;
//            }
//        }
//        //Log.d(TAG,"dayExist ? "+dayExist);
//        return dayExist;
//    }

//    private JsonArray getAnimalDays(MongoCollection<Document> collection, Long serialNumber){
//        Instant start = Instant.now();
//        days = getAnimal(collection, serialNumber).getAsJsonArray("days");
//        Instant end = Instant.now();
//        getAnimalDaysTime = humanReadableFormat(Duration.between(start, end));
//        return days;
//    }

//    private JsonObject getAnimal(MongoCollection<Document> collection, Long serialNumber){
//        Instant start = Instant.now();
//        JsonObject result = new JsonObject();
//        JsonArray animals = getAnimalsArray(collection);
//        cpt3 = 0;
//        for(JsonElement animal : animals){
//            cpt3++;
//            JsonElement json = ((JsonObject) animal).get("serial_number");
//            Long sn = ((JsonObject) json).get("$numberLong").getAsLong();
//            if (!sn.equals(serialNumber)) continue;
//            result = (JsonObject)animal;
//            break;
//        }
//        Instant end = Instant.now();
//        getAnimalTime = humanReadableFormat(Duration.between(start, end));
//
//        //Log.d(TAG,"getAnimal result="+result+" size="+result.size());
//        return result;
//    }
//
//    private JsonArray getAnimalsArray(MongoCollection<Document> collection){
//        Instant start = Instant.now();
//        String json = Objects.requireNonNull(collection.find().first()).toJson();
//        jsonSize = json.length();
//        Instant end = Instant.now();
//        getAnimalsArrayTime = humanReadableFormat(Duration.between(start, end));
//
//        start = Instant.now();
//        JsonArray animals = parser.parse(json).getAsJsonObject().getAsJsonArray("animals");
//        end = Instant.now();
//        jsonParseTime = humanReadableFormat(Duration.between(start, end));
//
//        return animals;
//    }
    //    private void updateAnimal(ExcelDataRow ExcelDataRow, MongoCollection<Document> collection){
//        //Log.d(TAG, "updateAnimal...");
//        if(isDayExists(collection, ExcelDataRow.getTagSerialNumber(), ExcelDataRow.getDate(), ExcelDataRow.getTime())){
//            //Log.d(TAG, "day exist adding new data...");
//            //if(!dataExist){
//                Integer index = getItemIndex(collection, ExcelDataRow.getTagSerialNumber(), ExcelDataRow.getDate());
//                if(index != null){
//                    collection.updateOne(Filters.and(
//                            eq("animals.serial_number", ExcelDataRow.getTagSerialNumber())
//                            ),
//                            Updates.push("animals.$.days."+index+".tagData", createTagDocument(ExcelDataRow)));
//                }
////            }else{
////                Log.d(TAG, "data exists.");
////            }
//        }else{
//            //Log.d(TAG, "day does not exist adding new day...");
//            collection.updateOne(eq("animals.serial_number", ExcelDataRow.getTagSerialNumber()),
//                    Updates.addToSet("animals.$.days", createDayDocument(ExcelDataRow)));
//        }
//    }
//
//    private Integer getItemIndex(MongoCollection<Document> collection, Long serialNumber, String date){
//        Instant start = Instant.now();
//        Integer index = null;
//        //days = getAnimalDays(collection, serialNumber);
//        for (int i = days.size() - 1; i >= 0; i--) {
//            if (((JsonObject) days.get(i)).get("date").getAsString().equals(date)) {
//                index = i;
//                cpt1 = i;
//                break;
//            }
//        }
//        Instant end = Instant.now();
//        getItemIndexTime = humanReadableFormat(Duration.between(start, end));
//        //Log.d(TAG,"index="+index+" for date="+date+" serial_number="+serialNumber);
//        return index;
//    }

    private Document createFarmDocument(ExcelDataRow ExcelDataRow){
        List<BasicDBObject> animals = new ArrayList<>();
        animals.add(createAnimalDocument(ExcelDataRow));
        return new Document("_id", ExcelDataRow.getControlStation())
                .append("control_station", ExcelDataRow.getControlStation())
                .append("animals", animals);
    }

    private BasicDBObject createAnimalDocument(ExcelDataRow ExcelDataRow){
        List<BasicDBObject> days = new ArrayList<>();
        //days.add(createDayDocument(ExcelDataRow));
        return new BasicDBObject("_id", ExcelDataRow.getTagSerialNumber())
                .append("serial_number", ExcelDataRow.getTagSerialNumber())
                .append("days",days);
    }

    private BasicDBObject createDayDocument(ExcelDataRow ExcelDataRow){
        List<BasicDBObject> tags = new ArrayList<>();
        //tags.add(createTagDocument(ExcelDataRow));
        return new BasicDBObject("_id", ExcelDataRow.getDate())
                .append("date", ExcelDataRow.getDate())
                .append("tagData", tags);
    }

    private BasicDBObject createTagDocument(ExcelDataRow ExcelDataRow){
        return new BasicDBObject("_id", ExcelDataRow.getTime())
                .append("time", ExcelDataRow.getTime())
                .append("date", ExcelDataRow.getDate())
                .append("serial_number", ExcelDataRow.getTagSerialNumber())
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


