package com.himani.chawla.distributed;

public class TestThird {

  public static void main(String[] args) {
    Node currNode3 = Node.createCurrentNode("configFile2.txt");
    currNode3.storeMessages("inputFile2.txt");
    new Thread(currNode3).start();
  }
}
