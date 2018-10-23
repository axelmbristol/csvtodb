package database;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.Updates;
import entities.Day;
import entities.ExcelDataRow;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.Document;
import trikita.log.Log;
import utils.Utils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static utils.Utils.prettyDate;


/**
 * Created by Axel on 26/09/2018.
 * class for database init and communication
 */
public class DataBase {
    private static String TAG = DataBase.class.getName();
    private MongoDatabase database;
    private String collectionName;
    private MongoCollection currCollection;
    private MongoClient mongoClient;

    public DataBase(){
        Log.d(TAG, "mongodb init...");
        mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
        List<String> databases = mongoClient.listDatabaseNames().into(new ArrayList<>());
        Log.d(TAG,"databases="+databases);
        Log.d(TAG, "purge...");
        for(String db : databases){
            if(db.length() >= "70091100060".length()) {
                Log.d(TAG, "drop db="+db);
                mongoClient.getDatabase(db).drop();
            }
        }
    }

    public void init(ExcelDataRow excelDataRow){
        Document farmDocument = createFarmDocument(excelDataRow);
        collectionName = "_"+farmDocument.get("control_station")+"-"+excelDataRow.getDate(); //need _ for MongoDB collection syntax convention.
        currCollection = database.getCollection(collectionName);
        if(!isCollectionExists(collectionName)){
            Log.d(TAG,"creating new farm collection."+collectionName+" "+farmDocument);
            database.getCollection(collectionName).insertOne(farmDocument);
        }else {
            Log.d(TAG,"collection already exists."+collectionName+" "+farmDocument);
        }
    }

    private void initCopy(ExcelDataRow excelDataRow){
        Document farmDocument = createFarmDocument(excelDataRow);
        collectionName = collectionName +"-"+ Instant.now().toEpochMilli();
        currCollection = database.getCollection(collectionName);
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

//    public void addDay(ExcelDataRow ExcelDataRow){
//        BasicDBObject d = createDayDocument(ExcelDataRow);
//        database.getCollection(collectionName).updateOne(eq("animals.serial_number", ExcelDataRow.getTagSerialNumber()),
//                Updates.addToSet("animals.$.days", d));
//    }

    public void purgeDB(String dataBaseName){
        if(isDataBaseExists(dataBaseName)){
            mongoClient.getDatabase(dataBaseName).drop();
        }
    }

    public void addEntry(Day day, String dataBaseName){

        database = mongoClient.getDatabase(dataBaseName);

        List<BasicDBObject> animals = new ArrayList<>();
        for (List<ExcelDataRow> animal : day.data){
            List<BasicDBObject> tags = new ArrayList<>();
            for (ExcelDataRow entryRow : animal){
                tags.add(createTagDocument(entryRow));
            }
            animals.add(new BasicDBObject("serial_number", animal.get(0).getTagSerialNumber())
                            .append("tag_data", tags));
        }

        collectionName = "_"+day.controlStation+"_"+day.epoch+"_"+day.prettyDate;

        Document farmDocument = new Document("control_station", day.controlStation)
                .append("animals", animals);

        try{
            //need _ for MongoDB collection syntax convention.
            database.getCollection(collectionName).insertOne(farmDocument);
        }catch (BsonMaximumSizeExceededException e){
            Log.e(TAG,"error while trying to add entry.",e);
            Utils.writeToLogFile("error while trying to add entry. e="+e);

            List<List<BasicDBObject>> split = Lists.partition(animals, (animals.size()/2)+1);
            for (int i = 0; i < split.size(); i++){
                Document doc = new Document("control_station", day.controlStation)
                        .append("animals", split.get(i));
                collectionName = "_"+day.controlStation+"_"+day.epoch+"_"+day.prettyDate+"_"+i;
                Log.d(TAG,"splitting collection new collection="+collectionName);
                database.getCollection(collectionName).insertOne(doc);
            }
        }
    }

    public void addEntry(ExcelDataRow ExcelDataRow){
        try{
            BasicDBObject d = createTagDocument(ExcelDataRow);
            currCollection.updateOne(Filters.and(
                    eq("animals.serial_number", ExcelDataRow.getTagSerialNumber())
                    ),
                    Updates.push("animals.$.tag_data", d));
        }catch (MongoWriteException e){
            Log.e(TAG,"error while trying to add entry.",e);
            //todo create new document
            initCopy(ExcelDataRow);
            BasicDBObject d = createTagDocument(ExcelDataRow);
            database.getCollection(collectionName).updateOne(Filters.and(
                    eq("animals.serial_number", ExcelDataRow.getTagSerialNumber())
                    ),
                    Updates.push("animals.$.tag_data", d));
        }
    }

    private boolean isCollectionExists(String collectionName){
        List<String> collections = database.listCollectionNames().into(new ArrayList<>());
        Log.d(TAG,"collections="+collections);
        return collections.contains(collectionName);
    }

    private boolean isDataBaseExists(String databaseName){
        List<String> databases = mongoClient.listDatabaseNames().into(new ArrayList<>());
        Log.d(TAG,"databases="+databases);
        return databases.contains(databaseName);
    }

    private Document createFarmDocument(ExcelDataRow ExcelDataRow){
        List<BasicDBObject> animals = new ArrayList<>();
        animals.add(createAnimalDocument(ExcelDataRow));
        return new Document("_id", ExcelDataRow.getControlStation())
                .append("control_station", ExcelDataRow.getControlStation())
                .append("animals", animals);
    }

    private BasicDBObject createAnimalDocument(ExcelDataRow ExcelDataRow){
        List<BasicDBObject> tags = new ArrayList<>();
        //days.add(createDayDocument(ExcelDataRow));
        return new BasicDBObject("_id", ExcelDataRow.getTagSerialNumber())
                .append("serial_number", ExcelDataRow.getTagSerialNumber())
                .append("tag_data", tags);
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


