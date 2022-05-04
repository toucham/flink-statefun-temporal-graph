package org.apache.flink.statefun.playground.java.graphanalytics.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.util.StringUtils;

/**
 * This class mimics Flink's Tuple2 class. This class is created to make unmarshalling json object
 * to tuple object easier
 * @param <T0>
 * @param <T1>
 */
public class CustomTuple2<T0, T1> {

  @JsonProperty("f0")
  private T0 f0;

  @JsonProperty("f1")
  private T1 f1;

  public CustomTuple2() {}

  /**
   * Overloaded and encapsulated constructor
   * @param field0
   * @param field1
   */
  private CustomTuple2(T0 field0, T1 field1) {
    this.f0 = field0;
    this.f1 = field1;
  }

  /**
   * getter method
   * @param pos
   * @return
   */
  @SuppressWarnings("unchecked")
  public <T> T getField(int pos) {
    switch (pos) {
      case 0:
        return (T) this.f0;
      case 1:
        return (T) this.f1;
      default:
        throw new IndexOutOfBoundsException(String.valueOf(pos));
    }
  }

  /**
   * setter method
   * @param pos
   * @param value
   */
  @SuppressWarnings("unchecked")
  public <T> void setField(int pos, T value) {
    switch (pos) {
      case 0:
        this.f0 = (T0) value;
        break;
      case 1:
        this.f1 = (T1) value;
        break;
      default:
        throw new IndexOutOfBoundsException(String.valueOf(pos));
    }
  }

  /**
   * Method that calls the private constructor and creates a CustomTuple2 object
   * @param field0
   * @param field1
   * @return a CustomTuple2 object
   */
  public static CustomTuple2 createTuple2(int field0, long field1) {
    return new CustomTuple2(field0, field1);
  }

  /**
   * This method creates a string representation of the CustomTuple2 object in the form (f0, f1)
   * @return
   */
  public String toString() {
    return "("
        + StringUtils.arrayAwareToString(this.f0)
        + ","
        + StringUtils.arrayAwareToString(this.f1)
        + ")";
  }
}
