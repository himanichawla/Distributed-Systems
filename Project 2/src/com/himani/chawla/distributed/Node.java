package com.himani.chawla.distributed;

import static com.himani.chawla.distributed.Utils.LOG;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TimerTask;
import java.util.TreeMap;

public class Node extends TimerTask {

  private int port;
  private int rangeStart;
  private int rangeEnd;
  private int joinTS;
  private int leaveTS;
  private InetAddress host;
  private DatagramSocket udpSocket;
  private int nextHop = -1;
  private int prevHop = -1;
  private int token = -1;
  static Long myclock;
  private Map<Integer, String> messages;
  private boolean running = false;
  private long msg_start = 0;
  private long msg_end = 0;
  private long TAT = 0;
  public String outFile = "";
  boolean started = false;
  private String token_msg = "";

  static {
    myclock = System.currentTimeMillis();
  }

  Node(int port, int start, int end, int joinTS, int leaveTS) {
    this.port = port;
    this.rangeStart = start;
    this.rangeEnd = end;
    this.joinTS = joinTS;
    this.leaveTS = leaveTS;

    messages = new TreeMap<Integer, String>();

  }

  public void setOutFile(String outFileName) {
    this.outFile = outFileName;
  }

  public int findNextHop() {
    // nextHop = -1;
    for (int i = port + 1; i <= rangeEnd; i++) {
      // if (isAlive()) {
      LOG(myclock, "port# " + port + " sending ping to " + i);
      sendMessage(i, RingConstants.PROBE_PING, "ping");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG(myclock, "message sending failed");
        // e.printStackTrace();
      }
      if (nextHop != -1) {
        break;
      }
      // }

    }
    if (nextHop == -1) {
      for (int i = rangeStart; i <= port - 1; i++) {
        // if (isAlive()) {
        LOG(myclock, "port# " + port + " sending ping to " + i);
        sendMessage(i, RingConstants.PROBE_PING, "ping");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG(myclock, "message sending failed");
          // e.printStackTrace();
        }
        if (nextHop != -1) {
          break;
        }
      }

      // }
    }
    return nextHop;
  }

  private void sendMessage(int destinationPort, String type, String str) {
    Message msg = new Message(type, -1, -1, str);
    byte[] buffer = Utils.convertMessageToBytes(msg);
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, host, destinationPort);
    try {
      udpSocket.send(packet);
    } catch (IOException e) {
      // e.printStackTrace();
      LOG(myclock, "socket is closed");
    }
  }

  public void election() {
    String ID = String.valueOf(port);
    File file = new File(outFile);
    FileWriter writer;
    try {
      writer = new FileWriter(file, true);
      writer.write("[" + Utils.getElapsedSeconds(myclock) + "] " + "started election, send election message to client " + nextHop + "\n"); // change so write only when port is leader
      writer.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      // e.printStackTrace();
    }
    // LOG(myclock, "Starting election at port " + port + "sending message to"
    // + nextHop);

    sendMessage(nextHop, RingConstants.ELECTION, ID);

  }

  private String generate_token() {
    if (token == -1) {
      // String token_msg = "1";
      Random rand = new Random();
      token = rand.nextInt(10000);
    }

    return "TOKEN: " + String.valueOf(token);
  }

  @SuppressWarnings("deprecation")
  @Override
  public void run() {

    Thread receiver = new Thread(new MessageReceiver());
    while (!started) {
      if (Utils.getElapsedSeconds(myclock) >= joinTS) {
        try {
          host = InetAddress.getLocalHost();
          udpSocket = new DatagramSocket(port);
          LOG(myclock, "Server is starting on port " + port + " ...");
        } catch (IOException e) {
          e.printStackTrace();
          LOG(myclock, "Error on socket creation!");
        }
        running = true;
        receiver.start();
        while (findNextHop() == -1) {
          LOG(myclock, "still looking for next hop");
          continue;
        }
        // findNextHop();
        election();
        started = true;
      } else {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG(myclock, "LLL");
          // e.printStackTrace();
        }
      }
    }
    while (started) {
      if (Utils.getElapsedSeconds(myclock) >= leaveTS) {
        File file = new File(outFile);
        FileWriter writer;
        try {
          writer = new FileWriter(file, true);
          writer.write("[" + Utils.secondsToString(myclock) + "] " + "ring is broken. " + "\n");
          writer.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          // e.printStackTrace();
        }
        LOG(myclock, "Ring is broken, port " + port + " is leaving");
        sendMessage(prevHop, RingConstants.UPDATE_PORT, RingConstants.NEXT + "@" + nextHop);
        // running = false;
        started = false;

        // udpSocket.close();
        // receiver.stop();

      }

    }

  }

  public static Node createCurrentNode(String fileName) {
    int rangeStart = -1;
    int rangeEnd = -1;
    int myPort = -1;
    int joinTimeStamp = -1;
    int leaveTimeStamp = -1;
    try {
      File f = new File(fileName);
      BufferedReader b = new BufferedReader(new FileReader(f));
      String readLine = "";

      while ((readLine = b.readLine()) != null) {
        String key = readLine.split(":")[0];
        if (key.equals(RingConstants.CLIENT_PORT)) {
          String portsRange = readLine.split(":")[1].trim();
          rangeStart = Integer.parseInt(portsRange.split("-")[0]);
          rangeEnd = Integer.parseInt(portsRange.split("-")[1]);
        } else if (key.equals(RingConstants.MY_PORT)) {
          myPort = Integer.parseInt(readLine.split(":")[1].trim());
        } else if (key.equals(RingConstants.JOIN_TIME)) {
          joinTimeStamp = Utils.convertToTimeStamp(readLine.split(":", 2)[1].trim());
        } else if (key.equals(RingConstants.LEAVE_TIME)) {
          leaveTimeStamp = Utils.convertToTimeStamp(readLine.split(":", 2)[1].trim());
        }
      }
    } catch (IOException e) {
      // e.printStackTrace();
    }
    return new Node(myPort, rangeStart, rangeEnd, joinTimeStamp, leaveTimeStamp);
  }

  public void storeMessages(String inputFile) {
    try {
      File f = new File(inputFile);
      BufferedReader b = new BufferedReader(new FileReader(f));
      String readLine = "";

      while ((readLine = b.readLine()) != null) {
        String[] split = readLine.split("\t");
        Integer timestamp = Utils.convertToTimeStamp(split[0].trim());
        messages.put(timestamp, split[1].trim());
      }
    } catch (IOException e) {
      // e.printStackTrace();
    }
  }

  public void sendMsgOrPassToken(String msg_received) {
    File file = new File(outFile);
    FileWriter writer;
    if (messages.isEmpty()) {
      sendMessage(nextHop, RingConstants.TOKEN, "");
      try {
        writer = new FileWriter(file, true);
        writer.write("[" + Utils.secondsToString(myclock) + "] " + "token " + msg_received + " was received " + "\n"); // change so write only when port is leader
        writer.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        // e.printStackTrace();
      }
      // count =0;
    } else {
      Iterator<Map.Entry<Integer, String>> iter = messages.entrySet().iterator();
      Map.Entry<Integer, String> entry = iter.next();
      // LOG(myclock, "Map entry: " + entry.getKey() + "-" + entry.getValue());
      Long currTime = Utils.getElapsedSeconds(myclock);
      // LOG(myclock, currTime);
      if (entry.getKey() <= currTime) {
        int sender_port = port;
        String msg_to_send = sender_port + ":" + entry.getKey() + ":" + entry.getValue();
        sendMessage(nextHop, RingConstants.TEXT, msg_to_send);
        try {
          writer = new FileWriter(file, true);
          writer.write("[" + Utils.secondsToString(myclock) + " post" + entry.getValue() + " was sent  " + "\n"); // change so write only when port is leader
          writer.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          // e.printStackTrace();
        }

        msg_start = myclock;
      } else {
        sendMessage(nextHop, RingConstants.TOKEN, msg_received);
        try {
          writer = new FileWriter(file, true);
          // writer.write("[" + Utils.getElapsedSeconds(myclock) + "] " + "token " + msg_received + " was received " + "\n"); // change so write only when port is leader
          writer.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          // e.printStackTrace();
        }
      }
    }
  }

  public boolean isAlive() {
    return Utils.getElapsedSeconds(myclock) < leaveTS;
  }

  public static void main(String[] args) throws IOException {

    Node currNode = createCurrentNode("configFile.txt");
    currNode.storeMessages("inputFile.txt");
    Node currNode2 = createCurrentNode("configFile2.txt");
    currNode2.storeMessages("inputFile2.txt");
    Node currNode3 = createCurrentNode("configFile3.txt");
    currNode3.storeMessages("inputFile3.txt");
    Node currNode4 = createCurrentNode("configFile4.txt");
    currNode4.storeMessages("inputFile4.txt");

    String outFile = "output.txt";
    String outFile2 = "output2.txt";
    String outFile3 = "output3.txt";
    String outFile4 = "output4.txt";

    // Map<String, String> arguments = new TreeMap<String, String>();
    // arguments.put(args[0], args[1]);
    // arguments.put(args[2], args[3]);
    // arguments.put(args[4], args[5]);
    //
    // Node currNode = createCurrentNode(arguments.get("-c"));
    // currNode.storeMessages(arguments.get("-i"));
    //
    // new Thread(currNode).start();
    // outFile = arguments.get("-c");

    // For 1 thread
    currNode.setOutFile(outFile);
    File file = new File(outFile);
    file.createNewFile();
    FileWriter writer = new FileWriter(file, true);

    // For 2 thread
    currNode2.setOutFile(outFile2);
    File file2 = new File(outFile2);
    file2.createNewFile();
    FileWriter writer2 = new FileWriter(file2, true);

    // For 3 thread
    currNode3.setOutFile(outFile3);
    File file3 = new File(outFile3);
    file3.createNewFile();
    FileWriter writer3 = new FileWriter(file3, true);

    // For 4 thread
    currNode4.setOutFile(outFile4);
    File file4 = new File(outFile4);
    file4.createNewFile();
    FileWriter writer4 = new FileWriter(file4, true);

    new Thread(currNode).start();
    new Thread(currNode2).start();
    new Thread(currNode3).start();
    new Thread(currNode4).start();
  }

  class MessageReceiver implements Runnable {
    File file = new File(outFile);
    FileWriter writer;

    boolean is_part_of_ring = false;

    private void actOnMessageReceived(Message msg, int srcPort) {
      // Probe messages
      // LOG(myclock, "running is" + running);
      if (msg.getType().equals(RingConstants.PROBE_PING)) {
        LOG(myclock, "Port#" + port + " RECD-> [ " + msg.getType() + "|" + msg.getContents()
            + " ] from Port# " + srcPort);
        LOG(myclock, "port# " + port + "prevHop is " + prevHop);
        if (prevHop == -1 || (Math.floorMod(srcPort - port, rangeEnd - rangeStart) > Math
            .floorMod(prevHop - port, rangeEnd - rangeStart))) {

          if (prevHop != -1) {
            LOG(myclock, "SENDING UPDATE PORT MESSAGE FROM: " + port + " to " + prevHop);
            Message updatePortToPrevPort = new Message(RingConstants.UPDATE_PORT, -1, -1, RingConstants.NEXT + "@" + srcPort);
            byte[] updatePortToPrevPortBytes = Utils.convertMessageToBytes(updatePortToPrevPort);
            try {
              udpSocket.send(new DatagramPacket(updatePortToPrevPortBytes, updatePortToPrevPortBytes.length, host, prevHop));
            } catch (IOException e) {
              // e.printStackTrace();
            }
          }

          prevHop = srcPort;

          Message response = new Message(RingConstants.PROBE_RESPONSE, -1, -1, "pong");
          byte[] responseBytes = Utils.convertMessageToBytes(response);
          LOG(myclock, "port#" + port + "sending pong message to " + prevHop);
          try {
            udpSocket.send(new DatagramPacket(responseBytes, responseBytes.length, host, prevHop));

          } catch (IOException e) {
            // e.printStackTrace();
            LOG(myclock, "ll");
          }
        }
      } else if (msg.getType().equals(RingConstants.PROBE_RESPONSE)) {
        LOG(myclock, "port #" + port + "receives pong ");
        // receivePacket.getPort() );
        if (nextHop == -1 || (Math.floorMod(srcPort - port,
            rangeEnd - rangeStart) < Math.floorMod(nextHop - port, rangeEnd - rangeStart))) {
          nextHop = srcPort;
        }

        LOG(myclock, "nexthop of " + port + " is " + nextHop);

        try {
          writer = new FileWriter(file, true);
          writer.write("[" + Utils.secondsToString(myclock) + "]:" + "next hop is changed to client" + nextHop + "\n");
          writer.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          // e.printStackTrace();
        }

      }
      // Election
      else if (nextHop != -1 && msg.getType().equals(RingConstants.ELECTION)) {
        String msg_content = msg.getContents();
        LOG(myclock, "Port#" + port + " RECD-> [ " + msg.getType() + "|" + msg.getContents()
            + " ] from Port# " + srcPort);
        int msg_port = Integer.parseInt(msg_content);
        if (msg_port < port) {
          String ID = String.valueOf(port);
          try {
            writer = new FileWriter(file, true);
            writer.write("[" + Utils.getElapsedSeconds(myclock) + "] " + "relayed election message, replaced leader. " + "\n"); // change so write only when port is leader
            writer.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
          }
          // LOG(myclock, "nextHop is " + nextHop);
          sendMessage(nextHop, RingConstants.ELECTION, ID);
        } else if (msg_port > port) {
          String ID = String.valueOf(msg_port);
          // LOG(myclock, "nextHop1 is " + nextHop);
          sendMessage(nextHop, RingConstants.ELECTION, ID);
        } else if (msg_port == port) {
          String leader_ID = String.valueOf(port);
          try {
            writer = new FileWriter(file, true);
            writer.write("[" + Utils.getElapsedSeconds(myclock) + "] " + "relayed election message, leader: client  " + port + "\n"); // change so write only when port is leader
            writer.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
          }
          // LOG(myclock, "nextHop2 is " + nextHop);
          sendMessage(nextHop, RingConstants.LEADER, leader_ID);
        }
      }
      // Leader message
      else if (msg.getType().equals(RingConstants.LEADER)) {
        LOG(myclock, "Port#" + port + " RECD-> [ " + msg.getType() + "|" + msg.getContents()
            + " ] from Port# " + srcPort);
        String leader_ID = msg.getContents();
        int leader = Integer.parseInt(leader_ID);
        if (leader > port) {
          is_part_of_ring = true;
          try {
            writer = new FileWriter(file, true);
            writer.write("[" + Utils.getElapsedSeconds(myclock) + "] " + "relayed election message, leader: client  " + leader + "\n"); // change so write only when port is leader
            writer.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
          }
          // LOG(myclock, "sending leader message to" + port);
          sendMessage(nextHop, RingConstants.LEADER, leader_ID);
        } else if (leader == port) {
          is_part_of_ring = true;
          LOG(myclock, "leader " + port);
          // File file = new File("output.txt");
          // FileWriter writer;
          try {
            writer = new FileWriter(file, true);
            writer.write("[" + Utils.secondsToString(myclock) + "]: leader selected" + leader + "\n"); // change so write only when port is leader
            writer.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
          }

          token_msg = generate_token();

          try {
            writer = new FileWriter(file, true);
            writer.write("[" + Utils.secondsToString(myclock) + "]: new token generated [" + token_msg + "]" + "\n");
            writer.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
          }
          sendMessage(nextHop, RingConstants.TOKEN, token_msg);
          try {
            writer = new FileWriter(file, true);
            writer.write("[" + Utils.secondsToString(myclock) + " ] token " + token_msg + " was sent to client " + nextHop + "\n"); // change so write only when port is leader
            writer.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
          }

          // count = 0;
        } else {
          is_part_of_ring = true;

        }
      }

      // Tokens
      else if (msg.getType().equals(RingConstants.TOKEN) && is_part_of_ring == true) {
        String msg_received = msg.getContents();
        try {
          writer = new FileWriter(file, true);
          writer.write("[" + Utils.secondsToString(myclock) + "]" + "token " + token_msg + " was received " + "\n"); // change so write only when port is leader
          writer.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          // e.printStackTrace();
        }
        // LOG(myclock, " TOKEN RECEIVED" + msg.getContents() + " by " + port);
        sendMsgOrPassToken(msg_received);

      } else if (msg.getType().equals(RingConstants.TEXT) && is_part_of_ring == true) {
        LOG(myclock, "Port#" + port + " RECD-> [ " + msg.getType() + "|" + msg.getContents()
            + " ] from Port# " + srcPort);
        String msg_received = msg.getContents();

        int origin_port = Integer.parseInt((msg_received.split(":"))[0]);
        int message_time = Integer.parseInt((msg_received.split(":"))[1]);
        String message = (msg_received.split(":"))[2];
        if (origin_port == port) {
          LOG(myclock, "delivered to all ");
          try {
            writer = new FileWriter(file, true);
            writer.write("[" + Utils.secondsToString(myclock) + "] " + " post " + message + " was delivered to all sucessfully " + "\n"); // change so write only when port is leader
            writer.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
          }
          msg_end = myclock;
          TAT = msg_end - msg_start;
          // RingConstants.TIMEOUT = 2 * TAT;
          LOG(myclock, "TAT " + TAT);
          messages.remove(message_time);
          sendMsgOrPassToken(msg_received);
        } else {
          sendMessage(nextHop, RingConstants.TEXT, msg_received);
          LOG(myclock, "forwarding message");
          try {
            writer = new FileWriter(file, true);
            writer.write("[" + Utils.secondsToString(myclock) + "] " + " post " + message + "from client" + origin_port + "was relayed " + "\n"); // change so write only when port is leader
            writer.close();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
          }
        }

      } else if (msg.getType().equals(RingConstants.UPDATE_PORT)) {
        LOG(myclock, "Port#" + port + " RECD-> [ " + msg.getType() + "|" + msg.getContents()
            + " ] from Port# " + srcPort);
        String contents = msg.getContents();
        String msgType = contents.split("@")[0];
        String portToBeUpdated = contents.split("@")[1];

        if (msgType.equals(RingConstants.NEXT)) {
          nextHop = Integer.valueOf(portToBeUpdated);
          Message updatePortToPrevPort = new Message(RingConstants.UPDATE_PORT, -1, -1, RingConstants.PREV + "@" + port);
          byte[] updatePortToPrevPortBytes = Utils.convertMessageToBytes(updatePortToPrevPort);
          try {
            udpSocket.send(new DatagramPacket(updatePortToPrevPortBytes, updatePortToPrevPortBytes.length, host, nextHop));
          } catch (IOException e) {
            // e.printStackTrace();
          }
        } else if (msgType.equals(RingConstants.PREV)) {
          prevHop = Integer.valueOf(portToBeUpdated);
        }

      }
    }

    @Override
    public void run() {

      LOG(myclock, "Port#" + port + " listening started at " + Utils.getElapsedSeconds(myclock));
      while (running) {
        byte[] receiveData = new byte[1024];
        boolean received = true;
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        try {
          udpSocket.receive(receivePacket);
        } catch (SocketTimeoutException e) {
          LOG(myclock, "TIMEOUT");
          received = false;
        } catch (IOException e) {
          // e.printStackTrace();
          LOG(myclock, "not receiving");
        }
        if (received) {
          Message msg = Utils.convertBytesToMessage(receiveData);
          int srcPort = receivePacket.getPort();
          if (srcPort == -1) {
            LOG(myclock, "-1 -1 -1");
          }
          actOnMessageReceived(msg, srcPort);
        }

      }

    }
  }

}
