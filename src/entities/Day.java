package entities;

import java.util.Date;
import java.util.List;

import static utils.Utils.prettyDate;

public class Day  {
    public Date date;
    public String prettyDate;
    public String controlStation;
    public List<List<ExcelDataRow>> data;
    public Long epoch;

    public Day(Date date, Long controlStation, List<List<ExcelDataRow>> data){
        this.date = date;
        this.data = data;
        this.controlStation = String.valueOf(controlStation);
        this.prettyDate = prettyDate(date);
        this.epoch = date.getTime();
    }

}
