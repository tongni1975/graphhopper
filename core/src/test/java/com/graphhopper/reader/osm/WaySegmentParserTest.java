/*
 *  Licensed to GraphHopper GmbH under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for
 *  additional information regarding copyright ownership.
 *
 *  GraphHopper GmbH licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except in
 *  compliance with the License. You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.graphhopper.reader.osm;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.graphhopper.reader.dem.ElevationProvider;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.FlagEncoder;
import com.graphhopper.storage.GraphBuilder;
import com.graphhopper.storage.GraphHopperStorage;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.PointList;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import static com.graphhopper.util.Helper.nf;

class WaySegmentParserTest {

    private final File file = new File("../map-matching/files/leipzig_germany.osm.pbf");

    @Test
    void useOldOSMReaderForComparison() throws IOException {
        // germany-190101: 284s
        EncodingManager encodingManager = EncodingManager.create("car,foot");
        GraphHopperStorage ghStorage = new GraphBuilder(encodingManager).build();
        new OSMReader(ghStorage).setFile(file).readGraph();
        System.out.println("nodes: " + nf(ghStorage.getNodes()) + ", edges: " + nf(ghStorage.getEdges()));
    }

    @Test
    void useNewOSMReader() throws IOException {
        // germany-190101: 361s (using hash maps in WaySegmentParser it took 419s)
        EncodingManager encodingManager = EncodingManager.create("car,foot");
        GraphHopperStorage ghStorage = new GraphBuilder(encodingManager).build();
        ghStorage.create(1000);
        new NewOSMReader().readOSM(file, ghStorage, ElevationProvider.NOOP, new NewOSMReader.Config());
        System.out.println("nodes: " + nf(ghStorage.getNodes()) + ", edges: " + nf(ghStorage.getEdges()));
    }

    @Test
    public void importGraph() {
        EncodingManager encodingManager = EncodingManager.create("car,foot");
        GraphHopperStorage ghStorage = new GraphBuilder(encodingManager).create();
        FlagEncoder encoder = encodingManager.fetchEdgeEncoders().get(0);
        LongIntMap ghNodesByOSMNodes = new LongIntHashMap();
        LongArrayList osmWaysByGHEdges = new LongArrayList();
        // very simple filtering, simply accept all highways and route relations
        Predicate<ReaderWay> wayFilter = way -> way.getTag("highway") != null;
        Predicate<ReaderRelation> relationFilter = relation -> relation.hasTag("type", "route");
        WaySegmentParser.WaySegmentHandler waySegmentHandler = new WaySegmentParser.WaySegmentHandler() {
            private int nextNodeId = 0;

            @Override
            public void handleSegment(ReaderWay way, List<ReaderNode> readerNodes, List<ReaderRelation> relations) {
                if (readerNodes.size() < 2) {
                    return;
                }
                // todo: handle barriers and other node tags here, they are all available in readerNodes
                // todo: parse way tags to set default speed, access and all this.
                //       we can even consider node tags like traffic lights to determine speed, or put this information
                //       into an encoded (edge) value
                // todo: use the relations we found for this way segment as well
                // todo: handle loops
                ReaderNode firstNode = readerNodes.get(0);
                if (!ghNodesByOSMNodes.containsKey(firstNode.getId())) {
                    ghNodesByOSMNodes.put(firstNode.getId(), nextNodeId);
                    ghStorage.getNodeAccess().setNode(nextNodeId, firstNode.getLat(), firstNode.getLon());
                    nextNodeId++;
                }
                ReaderNode lastNode = readerNodes.get(readerNodes.size() - 1);
                if (!ghNodesByOSMNodes.containsKey(lastNode.getId())) {
                    ghNodesByOSMNodes.put(lastNode.getId(), nextNodeId);
                    ghStorage.getNodeAccess().setNode(nextNodeId, lastNode.getLat(), lastNode.getLon());
                    nextNodeId++;
                }
                int from = ghNodesByOSMNodes.get(firstNode.getId());
                int to = ghNodesByOSMNodes.get(lastNode.getId());
                EdgeIteratorState edge = ghStorage.edge(from, to)
                        .set(encoder.getAverageSpeedEnc(), 60)
                        .set(encoder.getAccessEnc(), true);
                assert edge.getEdge() == osmWaysByGHEdges.size();

                // we will need this association to deal with the turn relations
                osmWaysByGHEdges.add(way.getId());

                int s = readerNodes.size();
                if (s > 2) {
                    PointList geometry = new PointList(s - 2, false);
                    for (int i = 1; i < s - 1; i++) {
                        geometry.add(readerNodes.get(i).getLat(), readerNodes.get(i).getLon());
                    }
                    edge.setWayGeometry(geometry);
                }

            }
        };
        WaySegmentParser.TurnRelationHandler turnRelationHandler = turnRelation -> {
            // skipped here. we will need the ghNodesByOSMNodes map to determine the tower node
            // id based on the osm node of the relation. we also need osmWaysByGHEdges so we
            // can translate the OSM way Ids used in the turn relation to a turn restriction with
            // GH edge Ids
        };

        // run the import
        new WaySegmentParser().readOSM(file, wayFilter, relationFilter, waySegmentHandler, turnRelationHandler);
        System.out.println("nodes: " + nf(ghStorage.getNodes()) + ", edges: " + nf(ghStorage.getEdges()));

    }

}