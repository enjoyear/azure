package com.chen.guo.log;

import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.helpers.PatternParser;

public class ADFPatternParser extends PatternParser {

  public ADFPatternParser(final String conversionPattern) {
    super(conversionPattern);
  }

  /**
   * https://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/PatternLayout.html
   *
   * @param c Converter denotation character
   */
  @Override
  protected void finalizeConverter(char c) {
    PatternConverter pc;
    switch (c) {
      case 'P':
        pc = new ADFPipelineConverter();
        addConverter(pc);
        break;
      case 'A':
        pc = new ADFActivityConverter();
        addConverter(pc);
        break;
      default:
        super.finalizeConverter(c);
    }
  }
}
