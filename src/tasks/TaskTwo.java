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
    private static HashMap locationMap;

    @Override
    public HashMap map(List list) {
      HashMap results = new HashMap<>();

      for (Object r : list) {
        String record = (String) r;
        String[] tokens = record.split(",");
        int month = Helper.grabMonth(tokens[0]);
        int day = Helper.grabDay(tokens[0]);
        int hour = Helper.grabHour(tokens[0]);
        String locationCode = tokens[2];
        String location = (String) locationMap.get(locationCode);
      // int second = Helper.grabSecond(tokens[0]);

        boolean withinTime = month==2 && hour >= START_TIME && hour <= END_TIME; ;
        if(hour == END_TIME) {
          withinTime = Helper.grabMinute(tokens[0]) == 0 && Helper.grabSecond(tokens[0])==0;
        }

        if (locationCode.startsWith(FILTER) && withinTime && location.indexOf("L2SR2")!=-1) {
          HashMap map = (HashMap) results.get(day);
          String userId = (String) tokens[1];
          if(map==null) {
            map = new HashMap<>();
          }
          List<String> userIds = (ArrayList<String>) map.get(location);
          if (userIds == null) {
            userIds = new ArrayList<String>();
          }
          if(!userIds.contains(userId)) {
            userIds.add(userId);
            map.put(location, userIds);
          }
          results.put(day,map);
        }
      }
    // System.out.println("Results size: " + results.size());
      return results;
    }

    @Override
    public HashMap reduce(Object key, List data) {
      HashMap result = new HashMap();
      HashMap subMap = new HashMap();
      for(Object o: data){
        HashMap map = (HashMap) o;
        for(Object subKey:map.keySet()) {
          int count = 0;
          if (subMap.containsKey(subKey)) {
            count = (int) subMap.get(subKey);
          } 
          List list = (ArrayList) map.get(subKey);
          count += list.size();
          subMap.put(subKey,count);
        }
      }
      result.put(key,subMap);
      return result;
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

      System.gc();

        TaskTwo mapper = new TaskTwo(); 
        TaskTwo reducer = new TaskTwo(); 
        HashMap<Object, List> results = null;

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
          Map subResults = (HashMap)results.get(key).get(0);
          int max = 0;
          Object maxSubKey = null;
          for(Object subKey : subResults.keySet()) {
            int v = (int) subResults.get(subKey);
            if (v>max) {
              max = v;
              maxSubKey = subKey;
            }

          }
          System.out.println(key + "/02/2014 - " + maxSubKey + " : "+ max);

        }
      
    }
  }