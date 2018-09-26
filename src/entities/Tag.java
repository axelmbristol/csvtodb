package entities;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

/**
 * Created by Axel on 26/09/2018.
 * class rfid tag
 */
public class Tag {

    @SerializedName("Date")
    @Expose
    private String date;
    @SerializedName("Time")
    @Expose
    private String time;
    @SerializedName("Control station")
    @Expose
    private Integer controlStation;
    @SerializedName("Type 12 tag messages")
    @Expose
    private Integer type12TagMessages;
    @SerializedName("Tag serial number")
    @Expose
    private Integer tagSerialNumber;
    @SerializedName("Signal strength")
    @Expose
    private String signalStrength;
    @SerializedName("Battery voltage")
    @Expose
    private String batteryVoltage;
    @SerializedName("First accelerometer counter")
    @Expose
    private String firstAccelerometerCounter;
    @SerializedName("First sensor value")
    @Expose
    private Integer firstSensorValue;
    @SerializedName("Second accelerometer counter")
    @Expose
    private String secondAccelerometerCounter;
    @SerializedName("Second sensor values (X-Y-Z)")
    @Expose
    private String secondSensorValuesXYZ;
    @SerializedName("Correlation identifier")
    @Expose
    private String correlationIdentifier;
    @SerializedName("Correlation value")
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

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Integer getControlStation() {
        return controlStation;
    }

    public void setControlStation(Integer controlStation) {
        this.controlStation = controlStation;
    }

    public Integer getType12TagMessages() {
        return type12TagMessages;
    }

    public void setType12TagMessages(Integer type12TagMessages) {
        this.type12TagMessages = type12TagMessages;
    }

    public Integer getTagSerialNumber() {
        return tagSerialNumber;
    }

    public void setTagSerialNumber(Integer tagSerialNumber) {
        this.tagSerialNumber = tagSerialNumber;
    }

    public String getSignalStrength() {
        return signalStrength;
    }

    public void setSignalStrength(String signalStrength) {
        this.signalStrength = signalStrength;
    }

    public String getBatteryVoltage() {
        return batteryVoltage;
    }

    public void setBatteryVoltage(String batteryVoltage) {
        this.batteryVoltage = batteryVoltage;
    }

    public String getFirstAccelerometerCounter() {
        return firstAccelerometerCounter;
    }

    public void setFirstAccelerometerCounter(String firstAccelerometerCounter) {
        this.firstAccelerometerCounter = firstAccelerometerCounter;
    }

    public Integer getFirstSensorValue() {
        return firstSensorValue;
    }

    public void setFirstSensorValue(Integer firstSensorValue) {
        this.firstSensorValue = firstSensorValue;
    }

    public String getSecondAccelerometerCounter() {
        return secondAccelerometerCounter;
    }

    public void setSecondAccelerometerCounter(String secondAccelerometerCounter) {
        this.secondAccelerometerCounter = secondAccelerometerCounter;
    }

    public String getSecondSensorValuesXYZ() {
        return secondSensorValuesXYZ;
    }

    public void setSecondSensorValuesXYZ(String secondSensorValuesXYZ) {
        this.secondSensorValuesXYZ = secondSensorValuesXYZ;
    }

    public String getCorrelationIdentifier() {
        return correlationIdentifier;
    }

    public void setCorrelationIdentifier(String correlationIdentifier) {
        this.correlationIdentifier = correlationIdentifier;
    }

    public Integer getCorrelationValue() {
        return correlationValue;
    }

    public void setCorrelationValue(Integer correlationValue) {
        this.correlationValue = correlationValue;
    }

    public String getFIELD14() {
        return fIELD14;
    }

    public void setFIELD14(String fIELD14) {
        this.fIELD14 = fIELD14;
    }

    public Integer getFIELD15() {
        return fIELD15;
    }

    public void setFIELD15(Integer fIELD15) {
        this.fIELD15 = fIELD15;
    }

    public Double getFIELD16() {
        return fIELD16;
    }

    public void setFIELD16(Double fIELD16) {
        this.fIELD16 = fIELD16;
    }

}