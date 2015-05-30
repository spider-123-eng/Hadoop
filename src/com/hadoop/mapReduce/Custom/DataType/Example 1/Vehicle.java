package com.hadoop.mapReduce.Custom.DataType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * 
 * Custom data type to be used as a value in Hadoop.
 * In Hadoop every data type to be used as values must implement Writable interface.
 *
 */
public class Vehicle implements Writable {

  private String model;
  private String vin;
  private int mileage;

  public void write(DataOutput out) throws IOException {
    out.writeUTF(model);
    out.writeUTF(vin);
    out.writeInt(mileage);
  }

  public void readFields(DataInput in) throws IOException {
    model = in.readUTF();
    vin = in.readUTF();
    mileage = in.readInt();
  }

  @Override
  public String toString() {
    return model + ", " + vin + ", "
        + Integer.toString(mileage);
  }

  public String getModel() {
    return model;
  }
  public void setModel(String model) {
    this.model = model;
  }
  public String getVin() {
    return vin;
  }
  public void setVin(String vin) {
    this.vin = vin;
  }
  public int getMileage() {
    return mileage;
  }
  public void setMileage(int mileage) {
    this.mileage = mileage;
  }
  
}
