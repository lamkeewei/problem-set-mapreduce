package tasks;

import java.io.*;
import java.util.*;
import aa.Mapper;
import aa.Reducer;
import aa.MapReduce;
import util.Helper;
import java.net.*;

public class TaskOne implements Mapper, Reducer {

  private final String FILTER = "101020";
  private final int START_TIME = 12;
  private final int END_TIME = 13;
  private static HashMap locationMap;

  @Override
  public HashMap map(List list) {
    HashMap results = new HashMap<>();

    for (Object r : list) {
      String record = (String) r;
      String[] tokens = record.split(",");
      int hour = Helper.grabHour(tokens[0]);
      String locationCode = tokens[2];
      String location = (String) locationMap.get(locationCode);
      // int second = Helper.grabSecond(tokens[0]);

      boolean withinTime = hour >= START_TIME && hour <= END_TIME; ;
      if(hour == END_TIME) {
        withinTime = Helper.grabMinute(tokens[0]) == 0;
      }

      if (locationCode.startsWith(FILTER) && withinTime) {
        List<String> userIds = null;
        if(results.get(location)==null) {
          userIds = new ArrayList<>();
        } else {
          userIds = (ArrayList<String>) results.get(location);
        }
        userIds.add(tokens[1]);
        results.put(location,userIds);
      }
    }
    // System.out.println("Results size: " + results.size());
    return results;
  }

  @Override
  public HashMap reduce(Object key, List data) {
    HashMap map = new HashMap();
    Set<String> userIds = new HashSet<>();
    for(Object o: data){
      List<String> list = (ArrayList) o;
      for(String id:list) {
        if (!userIds.contains(id)) {
          userIds.add(id);
        }
      }

    }
    map.put(key,userIds.size());
    // System.out.println("Reduced size: " + map.size());
    return map;
  }


  public static void main(String[] args) {
    List<String> data = new ArrayList<>();
    List<String> locations = new ArrayList<>();

    try {
      int i = 0;
      for (String fileName : args) {
        if(i==0) {
          locations.addAll(Helper.readFile(fileName));
          i++;
        } else {
          data.addAll(Helper.readFile(fileName));
        }
      }
      System.out.println(data.size() + " records loaded");
    } catch (IOException e) {
      System.err.println("Can't read file.  See stack trace");
      e.printStackTrace();
      System.exit(0);
    }

    locationMap = Helper.mapLocation(locations);

    TaskOne mapper = new TaskOne(); 
    TaskOne reducer = new TaskOne(); 
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

    int max = 0;
    String location = "";
    for (Object key : results.keySet()) {
      // list size is 1, caused reducer also performed combine step
      int size = (int) results.get(key).get(0);
      String room = (String) key;
      System.out.println("Room : " + room +" / Size : " + size);
      if (size > max) {
        max = size;
        location = (String) key;
      }
    }

    System.out.println("Count: " + max);
    System.out.println("Location: " + location);
  }
}