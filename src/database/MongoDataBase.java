package database;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import entities.Day;
import entities.ExcelDataRow;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.Document;
import trikita.log.Log;
import utils.Utils;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by Axel on 26/09/2018.
 * class for database init and communication
 */
public class MongoDataBase {
    private static String TAG = MongoDataBase.class.getName();
    private MongoClient mongoClient;

    public MongoDataBase(){
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


    public void addEntry(Day day, String dataBaseName){
        MongoDatabase database = mongoClient.getDatabase(dataBaseName);
        List<BasicDBObject> animals = new ArrayList<>();
        for (List<ExcelDataRow> animal : day.data){
            List<BasicDBObject> tags = new ArrayList<>();
            for (ExcelDataRow entryRow : animal){
                tags.add(createTagDocument(entryRow));
            }
            animals.add(new BasicDBObject("serial_number", animal.get(0).getTagSerialNumber())
                            .append("tag_data", tags));
        }
        String collectionName = "_" + day.controlStation + "_" + day.epoch + "_" + day.prettyDate;
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
                Log.d(TAG,"splitting collection new collection="+ collectionName);
                try {
                    database.getCollection(collectionName).insertOne(doc);
                }catch (BsonMaximumSizeExceededException v){
                    Log.e(TAG,"error while trying to add entry.",v);
                }
            }
        }
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


