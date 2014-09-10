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

  @Override
  public HashMap map(List list) {
    HashMap results = new HashMap();

    for (Object r : list) {
      String record = (String) r;
      String[] tokens = record.split(",");
      int hour = Helper.grabHour(tokens[0]);
      int minute = Helper.grabMinute(tokens[0]);
      // int second = Helper.grabSecond(tokens[0]);    

      boolean withinTime = hour >= START_TIME && hour <= END_TIME;
      if(hour == END_TIME) {
        withinTime = minute == 0;
      }

      if (tokens[2].startsWith(FILTER) && withinTime) {
        if(results.get(tokens[2]) == null) {
          results.put(tokens[2], tokens[1]);
        } else {
          String prev = (String) results.get(tokens[2]);
          results.put(tokens[2], prev + "," + tokens[1]);
        }
      }
    }
    // System.out.println("Results size: " + results.size());
    return results;
  }

  @Override
  public HashMap reduce(Object key, List data) {
    HashMap map = new HashMap();
    int count = 0;
    String combine = "";

    for(Object o: data){
      // Need to append a "," infront
      combine += "," + (String) o;
    }

    // Clear the first comma with substring
    String[] ids = combine.substring(1).split(",");
    Set unique = new HashSet(Arrays.asList(ids));

    map.put(key, unique.size());
    return map;
  }

  
  public static void main(String[] args) {
    List<String> data = new ArrayList<>();

    try {
      for (String fileName : args) {
        data.addAll(Helper.readFile(fileName));
      }

      System.out.println(data.size() + " records loaded");
    } catch (IOException e) {
      System.err.println("Can't read file.  See stack trace");
      e.printStackTrace();
      System.exit(0);
    }

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
      int size = (int)results.get(key).get(0);
      if (size > max) {
        max = size;
        location = (String) key;
      }
    }

    System.out.println("Count: " + max);
    System.out.println("Location: " + location);
  }
}