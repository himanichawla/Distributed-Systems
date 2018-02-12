package com.himani.chawla.distributed;

import java.io.Serializable;

public class Message implements Serializable {

  /**
  * 
  */
  private static final long serialVersionUID = 1L;

  public String getType() {
    return type;
  }

  public String getContents() {
    return contents;
  }

  private String type;
  private String contents;

  public Message(String type, int source, int destination, String contents) {
    this.type = type;
    this.contents = contents;
  }

}
