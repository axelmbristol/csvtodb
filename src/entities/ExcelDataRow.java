package entities;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import com.mongodb.BasicDBObject;
import org.bson.Document;

/**
 * Created by Axel on 26/09/2018.
 * class rfid tag
 */
public class ExcelDataRow {

    @SerializedName("date")
    @Expose
    private String date;
    @SerializedName("time")
    @Expose
    private String time;
    @SerializedName("control_station")
    @Expose
    private Long controlStation;
    @SerializedName("type_12_tag_messages")
    @Expose
    private Integer type12TagMessages;
    @SerializedName("tag_serial_number")
    @Expose
    private Long tagSerialNumber;
    @SerializedName("signal_strength")
    @Expose
    private String signalStrength;
    @SerializedName("battery_voltage")
    @Expose
    private String batteryVoltage;
    @SerializedName("first_accelerometer_counter")
    @Expose
    private String firstAccelerometerCounter;
    @SerializedName("first_sensor_value")
    @Expose
    private Integer firstSensorValue;
    @SerializedName("second_accelerometer_counter")
    @Expose
    private String secondAccelerometerCounter;
    @SerializedName("second_sensor_values_xyz")
    @Expose
    private String secondSensorValuesXYZ;
    @SerializedName("correlation_identifier")
    @Expose
    private String correlationIdentifier;
    @SerializedName("correlation_value")
    @Expose
    private Integer correlationValue;
    @SerializedName("FIELD14")
    @Expose
    private String fIELD14;
    @SerializedName("FIELD15")
    @Expose
    private Integer fIELD15;
    @SerializedName("FIELD16")
    @Expose
    private Double fIELD16;

    public ExcelDataRow(Document d) {
        this.date = d.getString("date");
        this.time = d.getString("time");
        this.controlStation = d.getLong("control_station");
        this.type12TagMessages = d.getInteger("type_12_tag_messages");
        this.tagSerialNumber = d.getLong("tag_serial_number");
        this.signalStrength = d.getString("signal_strength");
        this.batteryVoltage = d.getString("battery_voltage");
        this.firstAccelerometerCounter = d.getString("first_accelerometer_counter");
        this.firstSensorValue = d.getInteger("first_sensor_value");
        this.secondAccelerometerCounter = d.getString("second_accelerometer_counter");
        this.secondSensorValuesXYZ = d.getString("second_sensor_values_xyz");
        this.correlationIdentifier = d.getString("correlation_identifier");
        this.correlationValue = d.getInteger("correlation_value");
    }

    public ExcelDataRow(String date, String time, Long controlStation, Integer type12TagMessages,
                        Long tagSerialNumber, String signalStrength,
                        String batteryVoltage, String firstAccelerometerCounter,
                        Integer firstSensorValue, String secondAccelerometerCounter,
                        String secondSensorValuesXYZ, String correlationIdentifier,
                        Integer correlationValue

    ) {
        this.date = date;
        this.time = time;
        this.controlStation = controlStation;
        this.type12TagMessages = type12TagMessages;
        this.tagSerialNumber = tagSerialNumber;
        this.signalStrength = signalStrength;
        this.batteryVoltage = batteryVoltage;
        this.firstAccelerometerCounter = firstAccelerometerCounter;
        this.firstSensorValue = firstSensorValue;
        this.secondAccelerometerCounter = secondAccelerometerCounter;
        this.secondSensorValuesXYZ = secondSensorValuesXYZ;
        this.correlationIdentifier = correlationIdentifier;
        this.correlationValue = correlationValue;
    }

    public ExcelDataRow(String date, String time, Long controlStation,
                        Long tagSerialNumber, String signalStrength,
                        String batteryVoltage, Integer activityLevel

    ) {
        this.date = date;
        this.time = time;
        this.controlStation = controlStation;
        this.type12TagMessages = null;
        this.tagSerialNumber = tagSerialNumber;
        this.signalStrength = signalStrength;
        this.batteryVoltage = batteryVoltage;
        this.firstAccelerometerCounter = null;
        this.firstSensorValue = activityLevel;
        this.secondAccelerometerCounter = null;
        this.secondSensorValuesXYZ = null;
        this.correlationIdentifier = null;
        this.correlationValue = null;
    }

    public String getDate() {
        return date;
    }

    public String getTime() {
        return time;
    }

    public Long getControlStation() {
        return controlStation;
    }

    public Integer getType12TagMessages() {
        return type12TagMessages;
    }

    public Long getTagSerialNumber() {
        return tagSerialNumber;
    }

    public String getSignalStrength() {
        return signalStrength;
    }

    public String getBatteryVoltage() {
        return batteryVoltage;
    }

    public String getFirstAccelerometerCounter() {
        return firstAccelerometerCounter;
    }

    public Integer getFirstSensorValue() {
        return firstSensorValue;
    }

    public String getSecondAccelerometerCounter() {
        return secondAccelerometerCounter;
    }

    public String getSecondSensorValuesXYZ() {
        return secondSensorValuesXYZ;
    }

    public String getCorrelationIdentifier() {
        return correlationIdentifier;
    }

    public Integer getCorrelationValue() {
        return correlationValue;
    }

    @Override
    public String toString() {

        return "{date:"+this.date
                +",time:"+this.time
                +",controlStation:"+this.controlStation
                +",tagSerialNumber:"+this.tagSerialNumber
                +",signalStrength:"+this.signalStrength
                +",batteryVoltage:"+this.batteryVoltage
                +",tagSerialNumber:"+this.tagSerialNumber
                +",firstAccelerometerCounter:"+this.firstAccelerometerCounter
                +",firstSensorValue:"+this.firstSensorValue
                +",secondAccelerometerCounter:"+this.secondAccelerometerCounter
                +",secondSensorValuesXYZ:"+this.secondSensorValuesXYZ+"}";

//        return String.format(
//                "ExcelDataRow[date=%s, time='%s', controlStation='%s']",
//                this.date, this.time, this.controlStation);
    }



}