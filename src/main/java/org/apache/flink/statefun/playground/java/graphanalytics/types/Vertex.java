package org.apache.flink.statefun.playground.java.graphanalytics.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Vertex {

  @JsonProperty("src")
  private int src;

  @JsonProperty("dst")
  private int dst;

  @JsonProperty("t")
  private long timestamp;

  public Vertex() {}

  /**
   * overloaded constructor
   *
   * @param src
   * @param dst
   * @param timestamp
   */
  public Vertex(int src, int dst, long timestamp) {
    this.src = src;
    this.dst = dst;
    this.timestamp = timestamp;
  }

  public int getSrc() {
    return src;
  }

  public int getDst() {
    return dst;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
