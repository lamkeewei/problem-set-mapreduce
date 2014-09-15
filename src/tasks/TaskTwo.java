/* 
  Which SIS level 2 classroom has the highest maximum number 
  of users per day from 12:00:00 to 13:00:00?
*/

package tasks;

import java.io.*;
import java.util.*;
import aa.Mapper;
import aa.Reducer;
import aa.MapReduce;
import util.Helper;
import java.net.*;

public class TaskTwo implements Mapper, Reducer { 
  private final String FILTER = "101020";
  private final int START_TIME = 12;
  private final int END_TIME = 13;
  private static Map<String,String> locationMapping;

  @Override
  public HashMap map(List list) {
    HashMap results = new HashMap();

    for (Object r : list) {
      String record = (String) r;
      String[] tokens = record.split(",");      
      String roomID = locationMapping.get(tokens[2]);      

      if (tokens[2].startsWith(FILTER) && roomID.indexOf("SR2-") > -1) {
        int hour = Helper.grabHour(tokens[0]);
        boolean withinTime = hour == START_TIME || tokens[2].indexOf("13:00:00") > -1;

        if (withinTime) {
          int date = Helper.grabDate(tokens[0]);
          String id = (String) roomID;


          if(results.get(date) == null) {
            Map<String, Set<String>> roomRecords = new HashMap<>();
            Set<String> users = new HashSet<>();
            users.add(tokens[1]);
            roomRecords.put(roomID, users);

            results.put(date, roomRecords);
          } else {
            Map<String, Set<String>> prev = (Map<String, Set<String>>) results.get(date);
            Set<String> users = prev.get(roomID);

            if (users == null) {
              users = new HashSet<String>();
              prev.put(roomID, users);
            }
            
            users.add(tokens[1]);
          }
        }
      }
    }
    // System.out.println("Results size: " + results.size());
    return results;
  }

  @Override
  public HashMap reduce(Object key, List data) {
    HashMap map = new HashMap(1);
    HashMap<String, Set<String>> roomCounts = new HashMap<>();

    for (Object o : data) {
      HashMap<String, Set<String>> roomRecords = (HashMap<String, Set<String>>) o;

      for (String id : roomRecords.keySet()) {        
        if (roomCounts.get(id) == null) {
          roomCounts.put(id, new HashSet<String>());
        }

        Set<String> userIds = roomCounts.get(id);
        userIds.addAll(roomRecords.get(id));
      }
    }

    String maxRoom = "";
    int maxCount = 0;
    for (String id : roomCounts.keySet()) {
      int count = roomCounts.get(id).size();

      if (count > maxCount) {
        maxCount = count;
        maxRoom = id + ":" + count;
      }
    }

    map.put(key, maxRoom);
    return map;
  }

  
  public static void main(String[] args) {
    List<String> data = new ArrayList<>();

    try {
      locationMapping = Helper.loadLocationMappings();
      System.out.println(locationMapping.keySet().size() + " mappings loaded");

      for (String fileName : args) {
        data.addAll(Helper.readFile(fileName));
      }

      System.out.println(data.size() + " records loaded");
    } catch (IOException e) {
      System.err.println("Can't read file.  See stack trace");
      e.printStackTrace();
      System.exit(0);
    }

    TaskTwo mapper = new TaskTwo(); 
    TaskTwo reducer = new TaskTwo(); 
    HashMap<Object, List> results = null;

    System.gc(); 
    long s = System.currentTimeMillis();

    try {       
      results = MapReduce.mapReduce(mapper, reducer, data, 5); 

    } catch (InterruptedException e) {

      System.out.println("Something unexpected happened");
      e.printStackTrace();
    }

    long e = System.currentTimeMillis(); 

    System.out.println("Clock time elapsed: " + (e - s) + " ms");

    for (Object key : results.keySet()) {
      List values = results.get(key);

      System.out.println(key + "/02/2014 - " + values.get(0));
    }
  }
}