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

import com.carrotsearch.hppc.*;
import com.graphhopper.reader.OSMTurnRelation;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.graphhopper.reader.dem.EdgeSampling;
import com.graphhopper.reader.dem.ElevationProvider;
import com.graphhopper.reader.dem.GraphElevationSmoothing;
import com.graphhopper.routing.ev.BooleanEncodedValue;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.util.parsers.TurnCostParser;
import com.graphhopper.storage.GraphHopperStorage;
import com.graphhopper.storage.IntsRef;
import com.graphhopper.storage.NodeAccess;
import com.graphhopper.storage.TurnCostStorage;
import com.graphhopper.util.*;
import com.graphhopper.util.shapes.GHPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.graphhopper.reader.osm.OSMReader.isOnePassable;

public class NewOSMReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(NewOSMReader.class);

    private GraphHopperStorage graphHopperStorage;
    private EncodingManager encodingManager;
    private ElevationProvider elevationProvider;
    private NodeAccess nodeAccess;
    private TurnCostStorage turnCostStorage;

    private final DouglasPeucker simplifyAlgo = new DouglasPeucker();
    private final DistanceCalc distCalc = DistanceCalcEarth.DIST_EARTH;

    private final LongIntMap ghNodeIdsByOSMNodeIds = new LongIntHashMap();
    private final LongArrayList osmWayIDsByGHEdgeIds = new LongArrayList();
    private final IntSet barrierNodes = new IntHashSet();
    private long nextArtificialOSMNodeID = Long.MIN_VALUE;
    private Config config;

    public Date readOSM(File osmFile, GraphHopperStorage graphHopperStorage, ElevationProvider elevationProvider,
                        Config config) {
        // todonow: we may only run this method once (or need to clear tmp data)
        this.graphHopperStorage = graphHopperStorage;
        this.encodingManager = graphHopperStorage.getEncodingManager();
        this.nodeAccess = graphHopperStorage.getNodeAccess();
        this.turnCostStorage = graphHopperStorage.getTurnCostStorage();
        this.elevationProvider = elevationProvider;

        simplifyAlgo.setMaxDistance(config.getMaxWayPointDistance());
        simplifyAlgo.setElevationMaxDistance(config.getElevationMaxWayPointDistance());
        this.config = config;

        WaySegmentParser waySegmentParser = new WaySegmentParser();
        waySegmentParser.readOSM(osmFile, this::acceptWay, this::acceptRelation, this::handleWaySegment, this::handleTurnRelation, config.getWorkerThreads());
        return waySegmentParser.getTimestamp();
    }

    /**
     * @return true if way segments for the given way should be generated, false otherwise
     */
    private boolean acceptWay(ReaderWay way) {
        // ignore 'broken' ways with less than two nodes
        if (way.getNodes().size() < 2)
            return false;

        // ignore multipolygon geometry
        if (!way.hasTags())
            return false;

        return encodingManager.acceptWay(way, new EncodingManager.AcceptWay());
    }

    /**
     * @return true if this relation should be added to the way segments that include the corresponding member ways,
     * false otherwise
     */
    private boolean acceptRelation(ReaderRelation relation) {
        return relation.hasTag("type", "route");
    }

    private void handleWaySegment(ReaderWay way, List<ReaderNode> readerNodes, List<ReaderRelation> relations) {
        {
            // todonow: is this comment still valid and do we need this code? and should it be here?
            // TODO move this after we have created the edge and know the coordinates => encodingManager.applyWayTags
            // Estimate length of ways containing a route tag e.g. for ferry speed calculation
            ReaderNode firstNode = readerNodes.get(0);
            ReaderNode lastNode = readerNodes.get(readerNodes.size() - 1);
            if (!Double.isNaN(firstNode.getLat()) && !Double.isNaN(firstNode.getLon()) && !Double.isNaN(lastNode.getLat()) && !Double.isNaN(lastNode.getLon())) {
                double estimatedDist = distCalc.calcDist(firstNode.getLat(), firstNode.getLon(), lastNode.getLat(), lastNode.getLon());
                // Add artificial tag for the estimated distance and center
                way.setTag("estimated_distance", estimatedDist);
                way.setTag("estimated_center", new GHPoint((firstNode.getLat() + lastNode.getLat()) / 2, (firstNode.getLon() + lastNode.getLon()) / 2));
            }
        }
        IntsRef edgeFlags = extractEdgeFlags(way, relations);
        // todonow: what is this supposed to do?
        if (edgeFlags.isEmpty())
            return;

        // we might have to split the segment into further sub segments because some nodes might be blocked by a barrier
        List<ReaderNode> subSegment = new ArrayList<>();
        for (ReaderNode readerNode : readerNodes) {
            long nodeFlags = readerNode.hasTags() ? encodingManager.handleNodeTags(readerNode) : 0;
            boolean isBarrier = nodeFlags > 0;
            if (isBarrier && isOnePassable(encodingManager.getAccessEncFromNodeFlags(nodeFlags), edgeFlags)) {
                // todo: how to deal with barriers that are 'junctions'?
                // create an edge for the nodes we collected so far, it will end at and including the barrier node
                if (!subSegment.isEmpty()) {
                    subSegment.add(readerNode);
                    createEdge(subSegment, way, edgeFlags);
                    subSegment = new ArrayList<>();
                }
                int barrierNode = setAndGetGHNode(readerNode);
                // todonow
                // at junctions do not add multiple barrier edges for the same node
                // ...but actually... *why not*?? maybe we need even more barrier edges to really block
                // passing the barrier node no matter where we come from and where we go!
                if (barrierNodes.add(barrierNode)) {
                    ReaderNode artificialReaderNode = new ReaderNode(nextArtificialOSMNodeID++, readerNode.getLat(), readerNode.getLon());
                    int extraBarrierEdgeNode = setAndGetGHNode(artificialReaderNode);
                    createBarrierEdge(barrierNode, extraBarrierEdgeNode, nodeFlags, edgeFlags, way);
                    subSegment.add(artificialReaderNode);
                } else {
                    // todonow: add some logs to find out if there are cases where barrier nodes are at
                    // junctions.
                    subSegment.add(readerNode);
                }
            } else {
                subSegment.add(readerNode);
            }
        }
        if (subSegment.size() > 1) {
            createEdge(subSegment, way, edgeFlags);
        } else {
            // if the subsegment ends with a barrier node no further edge is added
        }
    }

    private void createEdge(List<ReaderNode> nodes, ReaderWay way, IntsRef edgeFlags) {
        if (nodes.size() < 2)
            throw new IllegalStateException("Creating an edge with less than two nodes is not possible, " + nodes.get(0));

        int nodeFrom = setAndGetGHNode(nodes.get(0));
        int nodeTo = setAndGetGHNode(nodes.get(nodes.size() - 1));

        // handle loops, see #1525,1533
        // todonow: why do we even do this?
        if (nodeFrom == nodeTo) {
            if (nodes.size() > 2) {
                // todonow: maybe rather avoid recursion here?
                createEdge(nodes.subList(0, nodes.size() - 1), way, edgeFlags);
                createEdge(nodes.subList(nodes.size() - 2, nodes.size()), way, edgeFlags);
            } else {
                // todonow: what now? this seems to be a 'real' loop (no intermediate nodes)
            }
            return;
        }

        PointList pointList = buildGeometry(nodes);
        double towerNodeDistance = getTowerNodeDistance(pointList, way.getId());
        EdgeIteratorState edge = addEdgeToGraph(nodeFrom, nodeTo, towerNodeDistance, edgeFlags, way.getId());

        // If the entire way is just the first and last point, do not waste space storing an empty way geometry
        if (pointList.size() > 2) {
            // the geometry consists only of pillar nodes, but we check that the first and last points of the pointList
            // are equal to the tower node coordinates
            // todonow: not sure if this is really needed still
            checkCoordinates(nodeFrom, pointList.get(0));
            checkCoordinates(nodeTo, pointList.get(pointList.size() - 1));
            edge.setWayGeometry(pointList.shallowCopy(1, pointList.size() - 1, false));
        }
        checkDistance(edge);

        encodingManager.applyWayTags(way, edge);
    }

    private double getTowerNodeDistance(PointList pointList, long id) {
        double towerNodeDistance = distCalc.calcDistance(pointList);
        if (towerNodeDistance < 0.001) {
            // As investigation shows often two paths should have crossed via one identical point
            // but end up in two very close points.
            // todonow: in OSMReader this is just registered by zeroCounter
//            LOGGER.warn("Very small distance: " + towerNodeDistance + " for points: " + pointList.toString());
            towerNodeDistance = 0.001;
        }

        double maxDistance = (Integer.MAX_VALUE - 1) / 1000d;
        if (Double.isNaN(towerNodeDistance)) {
            LOGGER.warn("Bug in OSM or GraphHopper. Illegal tower node distance " + towerNodeDistance + " reset to 1m, osm way " + id);
            towerNodeDistance = 1;
        }

        if (Double.isInfinite(towerNodeDistance) || towerNodeDistance > maxDistance) {
            // Too large is very rare and often the wrong tagging. See #435
            // so we can avoid the complexity of splitting the way for now (new tower nodes would be required, splitting up geometry etc)
            LOGGER.warn("Bug in OSM or GraphHopper. Too big tower node distance " + towerNodeDistance + " reset to large value, osm way " + id);
            towerNodeDistance = maxDistance;
        }
        return towerNodeDistance;
    }

    private PointList buildGeometry(List<ReaderNode> nodes) {
        PointList pointList = new PointList(nodes.size(), nodeAccess.is3D());
        for (ReaderNode node : nodes) {
            // todonow: we only use degree/eleToInt and ele/degreeToInt for backwards compatibility with
            // current tests. does it make any sense to do this? consider that these conversions will happen
            // once we actually store lat/lon/ele in node access
            double lat = Helper.intToDegree(Helper.degreeToInt(node.getLat()));
            double lon = Helper.intToDegree(Helper.degreeToInt(node.getLon()));
            if (nodeAccess.is3D()) {
                double ele = elevationProvider.getEle(node.getLat(), node.getLon());
                ele = Helper.intToEle(Helper.eleToInt(ele));
                pointList.add(lat, lon, ele);
            } else {
                pointList.add(lat, lon);
            }
        }

        // Smooth the elevation before calculating the distance because smoothing might change the distance
        if (config.isSmoothElevation())
            GraphElevationSmoothing.smoothElevation(pointList);

        // sample points along long edges
        if (config.getLongEdgeSamplingDistance() < Double.MAX_VALUE && pointList.is3D())
            pointList = EdgeSampling.sample(pointList, config.getLongEdgeSamplingDistance(), distCalc, elevationProvider);

        if (config.getMaxWayPointDistance() > 0 && pointList.size() > 2)
            simplifyAlgo.simplify(pointList);
        return pointList;
    }

    private void createBarrierEdge(int ghNodeFrom, int ghNodeTo, long nodeFlags, IntsRef edgeFlags, ReaderWay way) {
        // todonow: do we need this copy or are flags 'copied' into edge anyway?
        IntsRef copiedFlags = IntsRef.deepCopyOf(edgeFlags);
        // barrier edges are implemented as zero distance edges without access for the encoders that shall be blocked
        EdgeIteratorState edge = addEdgeToGraph(ghNodeFrom, ghNodeTo, 0, copiedFlags, way.getId());

        // block the barrier edge, but only for certain encoders
        for (BooleanEncodedValue accessEnc : encodingManager.getAccessEncFromNodeFlags(nodeFlags))
            edge.set(accessEnc, false, false);
        encodingManager.applyWayTags(way, edge);
    }

    private EdgeIteratorState addEdgeToGraph(int nodeFrom, int nodeTo, double towerNodeDistance, IntsRef edgeFlags, long wayId) {
        EdgeIteratorState edge = graphHopperStorage.edge(nodeFrom, nodeTo)
                .setDistance(towerNodeDistance)
                .setFlags(edgeFlags);
        assert edge.getEdge() == osmWayIDsByGHEdgeIds.size();
        osmWayIDsByGHEdgeIds.add(wayId);
        return edge;
    }

    private void checkCoordinates(int nodeIndex, GHPoint point) {
        final double tolerance = 1.e-6;
        if (Math.abs(nodeAccess.getLat(nodeIndex) - point.getLat()) > tolerance || Math.abs(nodeAccess.getLon(nodeIndex) - point.getLon()) > tolerance)
            throw new IllegalStateException("Suspicious coordinates for node " + nodeIndex + ": (" + nodeAccess.getLat(nodeIndex) + "," + nodeAccess.getLon(nodeIndex) + ") vs. (" + point + ")");
    }

    private void checkDistance(EdgeIteratorState edge) {
        final double tolerance = 1;
        final double edgeDistance = edge.getDistance();
        final double geometryDistance = distCalc.calcDistance(edge.fetchWayGeometry(FetchMode.ALL));
        if (edgeDistance > 2_000_000)
            LOGGER.warn("Very long edge detected: " + edge + " dist: " + edgeDistance);
        else if (Math.abs(edgeDistance - geometryDistance) > tolerance)
            throw new IllegalStateException("Suspicious distance for edge: " + edge + " " + edgeDistance + " vs. " + geometryDistance
                    + ", difference: " + (edgeDistance - geometryDistance));
    }

    private IntsRef extractEdgeFlags(ReaderWay way, List<ReaderRelation> relations) {
        // handle access
        // todonow: we already do this when filtering the ways, but probably not worth to get rid of it so just do it again
        EncodingManager.AcceptWay acceptWay = new EncodingManager.AcceptWay();
        boolean accepted = encodingManager.acceptWay(way, acceptWay);
        if (!accepted)
            throw new IllegalStateException("The way with ID " + way.getId() + " was supposed to be accepted");

        // handle relations
        IntsRef relationFlags = encodingManager.createRelationFlags();
        if (relationFlags.length != 2)
            throw new IllegalStateException("Cannot use relation flags with != 2 integers");
        for (ReaderRelation relation : relations) {
            // update current relation flags with those for current relation
            relationFlags = encodingManager.handleRelationTags(relation, relationFlags);
        }

        // parse duration tag.
        // todonow: why is this not done in handleWayTags?
        if (way.getTag("duration") != null) {
            try {
                long dur = OSMReaderUtility.parseDuration(way.getTag("duration"));
                // Provide the duration value in seconds in an artificial graphhopper specific tag:
                way.setTag("duration:seconds", Long.toString(dur));
            } catch (Exception ex) {
                LOGGER.warn("Parsing error in way with OSMID=" + way.getId() + " : " + ex.getMessage());
            }
        }

        // handle edge flags
        return encodingManager.handleWayTags(way, acceptWay, relationFlags);
    }

    private void handleTurnRelation(ReaderRelation relation) {
        if (turnCostStorage == null)
            return;

        List<OSMTurnRelation> turnRelations = OSMReader.createTurnRelations(relation);
        for (OSMTurnRelation turnRelation : turnRelations) {
            long osmViaNode = turnRelation.getViaOsmNodeId();
            int viaNode = ghNodeIdsByOSMNodeIds.getOrDefault(osmViaNode, -1);
            // todonow: get rid of external internal map
            TurnCostParser.ExternalInternalMap map = new TurnCostParser.ExternalInternalMap() {
                @Override
                public int getInternalNodeIdOfOsmNode(long nodeOsmId) {
                    if (nodeOsmId == osmViaNode) {
                        return viaNode;
                    } else {
                        throw new IllegalArgumentException("Unexpected nodeOsmId " + nodeOsmId);
                    }
                }

                @Override
                public long getOsmIdOfInternalEdge(int edgeId) {
                    return osmWayIDsByGHEdgeIds.get(edgeId);
                }
            };
            encodingManager.handleTurnRelationTags(turnRelation, map, graphHopperStorage);
        }
    }

    /**
     * Returns the GH node ID for a given OSM node. If no GH node ID was assigned for the given OSM node yet we create
     * the next GH node ID, set the according coordinates and save the association between the OSM node ID and the GH
     * node ID for later.
     */
    private int setAndGetGHNode(ReaderNode readerNode) {
        int index = ghNodeIdsByOSMNodeIds.indexOf(readerNode.getId());
        if (index < 0) {
            int ghNode = ghNodeIdsByOSMNodeIds.size();
            ghNodeIdsByOSMNodeIds.indexInsert(index, readerNode.getId(), ghNode);
            nodeAccess.setNode(ghNode, readerNode.getLat(), readerNode.getLon(),
                    nodeAccess.is3D() ? elevationProvider.getEle(readerNode.getLat(), readerNode.getLon()) : Double.NaN);
            return ghNode;
        } else {
            return ghNodeIdsByOSMNodeIds.indexGet(index);
        }
    }

    public static class Config {
        private double maxWayPointDistance = 1;
        private double elevationMaxWayPointDistance = Double.MAX_VALUE;
        private boolean smoothElevation = false;
        private double longEdgeSamplingDistance = Double.MAX_VALUE;
        private int workerThreads = 2;

        public double getMaxWayPointDistance() {
            return maxWayPointDistance;
        }

        public void setMaxWayPointDistance(double maxWayPointDistance) {
            this.maxWayPointDistance = maxWayPointDistance;
        }

        public double getElevationMaxWayPointDistance() {
            return elevationMaxWayPointDistance;
        }

        public void setElevationMaxWayPointDistance(double elevationMaxWayPointDistance) {
            this.elevationMaxWayPointDistance = elevationMaxWayPointDistance;
        }

        public boolean isSmoothElevation() {
            return smoothElevation;
        }

        public void setSmoothElevation(boolean smoothElevation) {
            this.smoothElevation = smoothElevation;
        }

        public double getLongEdgeSamplingDistance() {
            return longEdgeSamplingDistance;
        }

        public void setLongEdgeSamplingDistance(double longEdgeSamplingDistance) {
            this.longEdgeSamplingDistance = longEdgeSamplingDistance;
        }

        public int getWorkerThreads() {
            return workerThreads;
        }

        public void setWorkerThreads(int workerThreads) {
            this.workerThreads = workerThreads;
        }
    }
}
