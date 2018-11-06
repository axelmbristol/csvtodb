package entities;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import trikita.log.Log;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Axel on 26/09/2018.
 * class rfid tag
 */
public class ExcelDataRow {
    private static String TAG = ExcelDataRow.class.getName();

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

    private Date epoch;


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
        this.epoch = computeEpoch();
    }

    private Date computeEpoch(){
        String dateString= (this.date +" "+time);
        try {
            Date date=new SimpleDateFormat("dd/MM/yy hh:mm:ss a").parse(dateString);
            return date;
        } catch (ParseException e) {
            Log.e(TAG,"error while getting epoch for "+dateString, e);
            return null;
        }
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
        this.epoch = computeEpoch();
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

        return "{epoch:"+this.epoch
                +",date:"+this.date
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

    public Date getEpoch() {
        return epoch;
    }
}