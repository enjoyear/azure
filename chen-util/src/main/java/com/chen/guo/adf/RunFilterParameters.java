package com.chen.guo.adf;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * The POJO for
 * https://docs.microsoft.com/en-us/rest/api/datafactory/pipelineruns/querybyfactory#runfilterparameters
 * <p>
 * https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.management.datafactory.models.runfilterparameters?view=azure-dotnet
 */
@Accessors(prefix = {"_"})
@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RunFilterParameters {
  public final static ObjectWriter objectWriter = new ObjectMapper()
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
      //.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, false)
      .writerWithDefaultPrettyPrinter();

  //The continuation token for getting the next page of results. Null for first page.
  private String _continuationToken;

  //List of filters
  private RunQueryFilter[] _filters;

  /**
   * Data Factory only stores pipeline run data for 45 days.
   * To persist more, check https://docs.microsoft.com/en-us/azure/data-factory/monitor-programmatically#data-range
   */
  //The time at or after which the run event was updated in 'ISO 8601' format.
  private String _lastUpdatedAfter;

  //The time at or before which the run event was updated in 'ISO 8601' format.
  private String _lastUpdatedBefore;

  //List of OrderBy option.
  private RunQueryOrderBy[] _orderBy;

  @Override
  public String toString() {
    try {
      return objectWriter.writeValueAsString(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  @AllArgsConstructor
  @Getter
  @Setter
  public static class RunQueryFilter {
    //Parameter name to be used for filter.
    private RunQueryFilterOperand _operand;

    //Operator to be used for filter
    private RunQueryFilterOperator _operator;

    //List of filter values.
    private String[] _values;

    /**
     * The allowed operands:
     * to query pipeline runs are PipelineName, RunStart, RunEnd and Status;
     * to query activity runs are ActivityName, ActivityRunStart, ActivityRunEnd, ActivityType and Status,
     * to query trigger runs are TriggerName, TriggerRunTimestamp and Status.
     */
    public enum RunQueryFilterOperand {
      ActivityName,
      ActivityRunEnd,
      ActivityRunStart,
      ActivityType,
      LatestOnly,
      PipelineName,
      RunEnd,
      RunGroupId,
      RunStart,
      Status,
      TriggerName,
      TriggerRunTimestamp
    }

    public enum RunQueryFilterOperator {
      Equals,
      In,
      NotEquals,
      NotIn
    }
  }

  @AllArgsConstructor
  @Getter
  @Setter
  public static class RunQueryOrderBy {
    //Sorting order of the parameter.
    RunQueryOrder _order;

    //Parameter name to be used for order by.
    RunQueryOrderByField _orderBy;

    public enum RunQueryOrder {
      ASC,
      DESC
    }

    /**
     * The allowed parameters to order by
     * for pipeline runs are PipelineName, RunStart, RunEnd and Status
     * for activity runs are ActivityName, ActivityRunStart, ActivityRunEnd and Status
     * for trigger runs are TriggerName, TriggerRunTimestamp and Status
     */
    public enum RunQueryOrderByField {
      ActivityName,
      ActivityRunEnd,
      ActivityRunStart,
      PipelineName,
      RunEnd,
      RunStart,
      Status,
      TriggerName,
      TriggerRunTimestamp
    }
  }
}
