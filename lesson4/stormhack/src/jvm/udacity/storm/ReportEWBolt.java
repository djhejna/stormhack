package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import udacity.storm.tools.*;
import udacity.storm.tools.Rankings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A bolt that prints the word and count to redis
 */
public class ReportEWBolt extends BaseRichBolt
{
  // place holder to keep the connection to redis
  transient RedisConnection<String,String> redis;

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // instantiate a redis connection
    RedisClient client = new RedisClient("localhost",6379);

    // initiate the actual connection
    redis = client.connect();
    
    int numBins = 5;
    double minLong = -124.848974;
    double maxLong = -66.885444;
    double LongRange = maxLong - minLong;  // positive
    double binWidth = LongRange / numBins; // positive
    int i;

    double [] binRange = new double[numBins];

    for (i = 0; i < numBins; i++) {
	binRange[i] = maxLong + binWidth * i;
    }
    
  }

  @Override
  public void execute(Tuple tuple)
  {
      /**
    Rankings rankableList = (Rankings) tuple.getValue(0);

    for (Rankable r: rankableList.getRankings()){
      String word = r.getObject().toString();
      //updated for TweetsWithTopHashtagsBolt since there is no count
      Long count = r.getCount();
      redis.publish("WordCountTopology", word + ":" + Long.toString(count));
    }
      **/

    //access the first column 'word'
    String latlong = tuple.getStringByField("geoLocation");
    // access the first column 'tweets'
    //    String tweets = tuple.getStringByField("tweets");

    // access the second column 'count'
    // String word = rankedWords.toString();
    //    Integer count = tuple.getIntegerByField("count");
    Long count = new Long(30);




    // publish the word count to redis using word as the key

    //    redis.publish("WordCountTopology", word + ":" + Long.toString(count));
    // TODO!!!


    //redis.publish("WordCountTopology", tweets + "|" + Long.toString(count));
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}
