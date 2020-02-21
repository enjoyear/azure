package com.chen.guo.ivy;

import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;


/**
 * Docs regarding ivy terminology: https://ant.apache.org/ivy/history/latest-milestone/terminology.html
 */
@AllArgsConstructor
@Getter
@ToString
public class ArtifactCoordinate {
  /**
   * the organization name of the ivy artifact, aka the groupId in Maven
   */
  String organization;
  /**
   * the module name of the ivy artifact, aka the artifactId in Maven
   */
  String module;
  /**
   * the revision number of the ivy artifact, aka the version in Maven
   */
  String revision;
  /**
   * classifier the classifier name of the ivy artifact, aka the classifier in Maven
   */
  String classifier;
  /**
   * file name extension of the ivy artifact, aka the type in Maven
   */
  String extension;

  /**
   * Convert the ArtifactCoordinate object to a map
   */
  public Map<String, String> toMap() {
    Map<String, String> coordinateMap = new HashMap<>();
    coordinateMap.put("organization", organization);
    coordinateMap.put("module", module);
    coordinateMap.put("version", revision);
    coordinateMap.put("classifier", classifier);
    coordinateMap.put("ext", extension);
    return coordinateMap;
  }
}