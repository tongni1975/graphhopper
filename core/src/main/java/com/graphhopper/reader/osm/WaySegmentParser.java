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

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.graphhopper.util.Helper;
import com.graphhopper.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;

import static com.graphhopper.util.Helper.nf;

/**
 * Parses an OSM file (xml, zipped xml or pbf) to return all 'way segments' and turn relations. A way segment is a part
 * of an OSM way between two 'junctions'. A junction is an OSM node where two OSM ways intersect. The way segments can
 * be retrieved via a callback function that provides full information about the OSM way the segment belongs, the
 * relations this way belongs to and all OSM nodes that belong to the segment.
 * <p>
 * The parsing is done in two passes: During pass1 the OSM ways are scanned and filtered by a given filter function.
 * We keep all OSM node IDs that occur in at least one way (that is accepted by the filter) and also remember the node
 * IDs that are junctions (i.e. the ones that occur in more than one way). We also extract all the relations that belong
 * to the OSM ways we keep.
 * During pass2 we store all the nodes (including their coordinates and tags) that we found in pass1 in memory and scan
 * the ways again. This time we call the way segment handler callback for each segment we find and pass all the information
 * we obtained for the way segment. Turn restriction relations are returned via a separate callback function at the
 * end of pass2 (after all way segments have been processed).
 **/
// todo: rename OSMParser, OSMReader... ? This thing reads an OSM file and returns the way segments and turn relations
public class WaySegmentParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaySegmentParser.class);
    private File osmFile;
    private Predicate<ReaderWay> wayFilter;
    private Predicate<ReaderRelation> relationFilter;
    private WaySegmentHandler waySegmentHandler;
    private TurnRelationHandler turnRelationHandler;
    private int workerThreads;

    private final LongSet wayNodes = new LongHashSet();
    private final LongSet multiWayNodes = new LongHashSet();
    private final LongSet wayIds = new LongHashSet();
    private final LongObjectMap<ReaderNode> osmNodesByID = new LongObjectHashMap<>();
    private final LongObjectMap<List<ReaderRelation>> osmRelationsByWayID = new LongObjectHashMap<>();

    private Date timestamp;

    public void readOSM(File osmFile, Predicate<ReaderWay> wayFilter, Predicate<ReaderRelation> relationFilter,
                        WaySegmentHandler waySegmentHandler, TurnRelationHandler turnRelationHandler) {
        readOSM(osmFile, wayFilter, relationFilter, waySegmentHandler, turnRelationHandler, 2);
    }

    /**
     * @param osmFile             the OSM file to parse, supported formats include .osm.xml, .osm.gz and .xml.pbf
     * @param wayFilter           returns false for OSM that should be ignored, otherwise true
     * @param relationFilter      returns false for OSM relations that should be ignored, otherwise true. relations of
     *                            relations and relation members that are not ways are always ignored. relations of type
     *                            'restriction' are always accepted, but handled via a separate handle, see below.
     * @param waySegmentHandler   callback function to handle the extracted way segments
     * @param turnRelationHandler callback function used to handle relations of type 'restriction'. this callback is
     *                            guaranteed to be called after all way segments were processed
     * @param workerThreads       the number of threads used for the low level reading of the OSM file
     */
    public void readOSM(File osmFile, Predicate<ReaderWay> wayFilter, Predicate<ReaderRelation> relationFilter,
                        WaySegmentHandler waySegmentHandler, TurnRelationHandler turnRelationHandler, int workerThreads) {
        this.osmFile = osmFile;
        this.wayFilter = wayFilter;
        this.relationFilter = relationFilter;
        this.waySegmentHandler = waySegmentHandler;
        this.turnRelationHandler = turnRelationHandler;
        this.workerThreads = workerThreads;

        clearTemporaryData();

        if (!osmFile.exists())
            throw new IllegalStateException("The OSM file does not exist: " + osmFile.getAbsolutePath());

        LOGGER.info("Start reading OSM file: '" + osmFile + "', using " + workerThreads + " threads");

        LOGGER.info("pass1 - start");
        StopWatch swPass1 = StopWatch.started();
        readOSMFile(new Pass1Handler(), "pass1");
        LOGGER.info("pass1 - finished, took: " + swPass1.stop().getTimeString());

        LOGGER.info("Way nodes: " + nf(wayNodes.size()) + ", Multi-way nodes: " + nf(multiWayNodes.size()));
        LOGGER.info("Ways: " + nf(wayIds.size()) + ", ways with relations: " + nf(osmRelationsByWayID.size()));

        LOGGER.info("pass2 - start");
        StopWatch swPass2 = StopWatch.started();
        readOSMFile(new Pass2Handler(), "pass2");
        LOGGER.info("pass2 - finished, took: " + swPass2.stop().getTimeString());

        LOGGER.info("Finished reading OSM file" +
                ", pass1: " + swPass1.getTimeString() +
                ", pass2: " + swPass2.getTimeString() +
                ", total: " + (swPass1.getSeconds() + swPass2.getSeconds() + "s"));

        clearTemporaryData();
    }

    /**
     * @return date extracted from OSM file header, might be null and you need to call {@link #readOSM} first.
     */
    public Date getTimestamp() {
        return timestamp;
    }

    private void readOSMFile(ReaderElementHandler handler, String name) {
        try (OSMInput osmInput = openOsmInputFile()) {
            ReaderElement elem;
            while ((elem = osmInput.getNext()) != null) {
                handler.handleElement(elem);
            }
            // todonow: make final call so we can show exact number of relations that were parsed etc.?
        } catch (Exception e) {
            throw new RuntimeException("Error when working with OSM file during " + name + "'" + osmFile + "'", e);
        }
    }

    private class Pass1Handler implements ReaderElementHandler {
        private boolean handledWays;
        private boolean handledRelations;
        private long wayCounter = -1;
        private long relationCounter = -1;

        @Override
        public void handleWay(ReaderWay way) {
            if (!handledWays)
                LOGGER.info("pass1 - start reading OSM ways");
            handledWays = true;
            if (handledRelations)
                throw new IllegalStateException("OSM way elements must be located before relation elements in OSM file");

            if (++wayCounter % 10_000_000 == 0)
                LOGGER.info("pass1 - processed ways: " + nf(wayCounter) +
                        ", accepted ways: " + nf(wayIds.size()) + ", wayNodes: " + nf(wayNodes.size()) + ", multiWayNodes: " + nf(multiWayNodes.size()) + ", " + Helper.getMemInfo());

            if (!wayFilter.test(way))
                return;

            // we keep track of all nodes that occur in at least one way (wayNodes) or even multiple ways (multiWayNodes)
            for (LongCursor node : way.getNodes()) {
                if (!wayNodes.add(node.value)) {
                    multiWayNodes.add(node.value);
                }
            }
            // we keep track of all the ways we keep so we can later ignore OSM relations that do not include any
            // ways of interest
            wayIds.add(way.getId());
        }

        @Override
        public void handleRelation(ReaderRelation relation) {
            if (!handledRelations)
                LOGGER.info("pass1 - start reading OSM relations");
            handledRelations = true;

            if (++relationCounter % 1_000_000 == 0)
                LOGGER.info("pass1 - processed relations: " + nf(relationCounter) +
                        ", accepted relations: " + nf(osmRelationsByWayID.size()) + ", " + Helper.getMemInfo());

            if (!relationFilter.test(relation))
                return;

            if (relation.hasTag("type", "restriction"))
                // turn restrictions are handled in the second pass
                // todonow: what is the purpose of 'prepareRestrictionRelation' in pass1 of OSMReader?
                return;

            if (relation.isMetaRelation())
                // meta relations (relations of relations) are not supported
                // todonow: should we allow relations that contain relations but also ways?
                return;


            for (ReaderRelation.Member member : relation.getMembers()) {
                // we only support members that reference at least one of the ways we are interested in
                if (member.getType() == ReaderRelation.Member.WAY && wayIds.contains(member.getRef())) {
                    List<ReaderRelation> relations = osmRelationsByWayID.get(member.getRef());
                    if (relations == null) {
                        relations = new ArrayList<>();
                        osmRelationsByWayID.put(member.getRef(), relations);
                    }
                    relations.add(relation);
                }
            }
        }
    }

    private class Pass2Handler implements ReaderElementHandler {
        private boolean handledNodes;
        private boolean handledWays;
        private boolean handledRelations;
        private long nodeCounter = -1;
        private long wayCounter = -1;
        private long waySegmentCounter = 0;
        private long relationCounter = -1;
        private long turnRelationCounter = 0;

        @Override
        public void handleNode(ReaderNode node) {
            if (!handledNodes)
                LOGGER.info("pass2 - start reading OSM nodes");
            handledNodes = true;
            if (handledWays)
                throw new IllegalStateException("OSM node elements must be located before way elements in OSM file");
            if (handledRelations)
                throw new IllegalStateException("OSM node elements must be located before relation elements in OSM file");

            if (++nodeCounter % 10_000_000 == 0)
                LOGGER.info("pass2 - processed nodes: " + nf(nodeCounter) +
                        ", accepted nodes: " + nf(osmNodesByID.size()) + ", " + Helper.getMemInfo());

            if (!wayNodes.contains(node.getId()))
                // this node does not occur in any of the OSM ways we consider here, so we are not interested in it
                return;
            ReaderNode existingNode = osmNodesByID.put(node.getId(), node);
            if (existingNode != null) {
                LOGGER.warn("There are multiple OSM nodes with ID: " + node.getId() + ". " +
                        "GraphHopper only considers the last one it finds");
            }
        }

        @Override
        public void handleWay(ReaderWay way) {
            if (!handledWays)
                LOGGER.info("pass2 - start reading OSM ways");
            handledWays = true;
            if (handledRelations)
                throw new IllegalStateException("OSM way elements must be located before relation elements in OSM file");

            if (++wayCounter % 10_000_000 == 0)
                LOGGER.info("pass2 - processed ways: " + nf(wayCounter) +
                        ", accepted way segments: " + nf(waySegmentCounter) + ", " + Helper.getMemInfo());

            if (!wayFilter.test(way))
                return;

            List<ReaderRelation> relations = osmRelationsByWayID.get(way.getId());
            if (relations == null)
                relations = Collections.emptyList();

            // we collect all nodes along this way until we hit the next junction. then we start the next segment
            List<ReaderNode> waySegmentNodes = new ArrayList<>();
            for (LongCursor nodeId : way.getNodes()) {
                ReaderNode osmNode = osmNodesByID.get(nodeId.value);
                if (osmNode == null) {
                    LOGGER.warn("Node {} is used in way {}, but it does not exist. GraphHopper will ignore it.",
                            nodeId.value, way.getId());
                    continue;
                }
                waySegmentNodes.add(osmNode);
                if (waySegmentNodes.size() > 1 && nodeId.index != way.getNodes().size() - 1 && multiWayNodes.contains(nodeId.value)) {
                    // this is a junction, we need to start a new way segment here
                    waySegmentHandler.handleSegment(way, waySegmentNodes, relations);
                    waySegmentCounter++;
                    waySegmentNodes = new ArrayList<>();
                    waySegmentNodes.add(osmNode);
                }
            }
            waySegmentHandler.handleSegment(way, waySegmentNodes, relations);
            waySegmentCounter++;
        }

        @Override
        public void handleRelation(ReaderRelation relation) {
            if (!handledRelations)
                LOGGER.info("pass2 - start reading OSM relations");
            handledRelations = true;

            if (++relationCounter % 1_000_000 == 0)
                LOGGER.info("pass2 - processed relations: " + nf(relationCounter) +
                        ", accepted turn relations: " + nf(turnRelationCounter) + ", " + Helper.getMemInfo());

            if (relation.hasTag("type", "restriction")) {
                // todonow: filter based on accepted osm way ids here or leave this up to handler? maybe because we
                // do not filter so far we need to exclude turn relations with unknown via node in OSMTurnRelationParser atm?
                turnRelationHandler.handleRestrictionRelation(relation);
                turnRelationCounter++;
            }
        }

        @Override
        public void handleFileHeader(OSMFileHeader fileHeader) {
            try {
                timestamp = Helper.createFormatter().parse(fileHeader.getTag("timestamp"));
            } catch (ParseException e) {
                LOGGER.warn("Could not parse OSM file timestamp '" + fileHeader.getTag("timestamp") + "': " +
                        e.getMessage());
            }
        }
    }

    private OSMInput openOsmInputFile() {
        try {
            return new OSMInputFile(osmFile).setWorkerThreads(workerThreads).open();
        } catch (XMLStreamException | IOException e) {
            throw new RuntimeException("Could not open OSM file: '" + osmFile + "'");
        }
    }

    private void clearTemporaryData() {
        this.wayNodes.clear();
        this.multiWayNodes.clear();
        this.wayIds.clear();
        this.osmNodesByID.clear();
        this.osmRelationsByWayID.clear();
        // todonow: or better release()?
    }

    private interface ReaderElementHandler {
        default void handleElement(ReaderElement elem) {
            switch (elem.getType()) {
                case ReaderElement.NODE:
                    handleNode((ReaderNode) elem);
                    break;
                case ReaderElement.WAY:
                    handleWay((ReaderWay) elem);
                    break;
                case ReaderElement.RELATION:
                    handleRelation((ReaderRelation) elem);
                    break;
                case ReaderElement.FILEHEADER:
                    handleFileHeader((OSMFileHeader) elem);
                    break;
                default:
                    throw new IllegalStateException("Unknown reader element type: " + elem.getType());
            }
        }

        default void handleNode(ReaderNode node) {
        }

        default void handleWay(ReaderWay way) {
        }

        default void handleRelation(ReaderRelation relation) {
        }

        default void handleFileHeader(OSMFileHeader fileHeader) {
        }

    }

    public interface WaySegmentHandler {
        /**
         * @param way             the OSM way this way segment was extracted from
         * @param waySegmentNodes the OSM nodes that belong to this segment. typically there are less such nodes than
         *                        there are in the original OSM way, because the way is split at junctions and also
         *                        there might be nodes referenced in the way that are missing in the OSM file
         * @param relations       a list of relations the segment's way belongs to
         */
        void handleSegment(ReaderWay way, List<ReaderNode> waySegmentNodes, List<ReaderRelation> relations);
    }

    public interface TurnRelationHandler {
        /**
         * @param restrictionRelation OSM relation with type 'restriction'
         */
        void handleRestrictionRelation(ReaderRelation restrictionRelation);
    }
}
