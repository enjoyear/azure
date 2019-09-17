package com.chen.guo.log;

import org.apache.log4j.PatternLayout;
import org.apache.log4j.helpers.PatternParser;

public class ADFPatternLayout extends PatternLayout {
  public ADFPatternLayout() {
    super();
  }

  public ADFPatternLayout(String pattern) {
    super(pattern);
  }

  @Override
  protected PatternParser createPatternParser(String pattern) {
    return new ADFPatternParser(pattern);
  }
}
