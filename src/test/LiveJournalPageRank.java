package test;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



import org.apache.giraph.Algorithm;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

/**
 * Page Rank for LiveJournal 
 */
@Algorithm(
    name = "PageRank",
    description = "Finds PageRank for LiveJournal data"
)
public class LiveJournalPageRank extends
    Vertex<IntWritable, DoubleWritable,
    NullWritable, DoubleWritable> {
  /** The shortest paths id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("LiveJournalPageRank.sourceId", 1);

  public static final int MAX_SUPERSTEPS = 30;

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(LiveJournalPageRank.class);

 
  @Override
  public void compute(Iterable<DoubleWritable> messages) {
    
    // Set the initial PageRank to be 1 / N, where N is the total number of vertices
    // NOTE: This is the 2nd superstep to allow one step to count the total number of vertices
    if (getSuperstep() ==  1) {
       setValue(new DoubleWritable(1.0d / getTotalNumVertices()));
     }

     // Calculate PageRank from incoming messages
     if (getSuperstep() > 1) {
       
       double pageRankSum = 0d; 
       for (DoubleWritable message: messages) {
          pageRankSum += message.get();
       }

       float d = 0.85f; // Damping factor
       DoubleWritable vertexRank = new DoubleWritable(((1.0f - d) / getTotalNumVertices()) + d * pageRankSum);
       setValue(vertexRank);
     }

     if (getSuperstep() < MAX_SUPERSTEPS) {
       long edges = getNumEdges();
       for (Edge<IntWritable, NullWritable> edge : getEdges()) {
        sendMessage(edge.getTargetVertexId(), new DoubleWritable(getValue().get() / edges));
       }
     } else {
        voteToHalt();
     }
  } 



}
