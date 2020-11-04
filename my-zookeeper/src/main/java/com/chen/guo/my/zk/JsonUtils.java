package com.chen.guo.my.zk;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class JsonUtils {
  private JsonUtils() {

  }

  public static void main(String[] args)
      throws JsonProcessingException {
    HashMap<String, Object> map = new HashMap<>();
    map.put("one", "111");
    map.put("two", "222");
    final String jsonString = JsonUtils.mapToJsonString(map);
    System.out.println(jsonString);

    final Map<String, String> recoveredMap = jsonToMap(jsonString);
    System.out.println(recoveredMap);
  }

  public static Map<String, String> jsonToMap(String json) {
    if (json == null || json.trim().isEmpty()) {
      return new HashMap<>();
    }

    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.readValue(json, new TypeReference<HashMap<String, String>>() {
      });
    } catch (IOException e) {
      throw new RuntimeException(String.format("Fail to convert the string %s to a map", json), e);
    }
  }

  public static String mapToJsonString(Map<String, Object> map)
      throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(map);
  }
}
