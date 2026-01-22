// src/main/java/org/apache/hadoop/fs/azurebfs/contracts/services/BlobLayout.java
package org.apache.hadoop.fs.azurebfs.contracts.services;

import javax.xml.bind.annotation.*;
import java.util.List;

@XmlRootElement(name = "BlobLayout")
@XmlAccessorType(XmlAccessType.FIELD)
public class BlobLayoutSchema {

    @XmlElement(name = "DataView")
    public DataView dataView;

    @XmlElementWrapper(name = "Ranges")
    @XmlElement(name = "Range")
    public List<Range> ranges;

    @XmlElementWrapper(name = "Endpoints")
    @XmlElement(name = "Endpoint")
    public List<Endpoint> endpoints;

    @XmlElement(name = "NextMarker")
    public String nextMarker;

    @XmlAccessorType(XmlAccessType.FIELD)
    public static class DataView {
        @XmlAttribute(name = "Id")
        public String id;

        @XmlAttribute(name = "Expiry")
        public String expiry;

        @XmlElementWrapper(name = "ReadKeys")
        @XmlElement(name = "ReadKey")
        public List<ReadKey> readKeys;
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static class Range {
        @XmlAttribute(name = "Start")
        public String start;

        @XmlAttribute(name = "End")
        public String end;

        @XmlAttribute(name = "Endpoint")
        public String endpointIndex;

        @XmlAttribute(name = "ReadKeys")
        public String readKeys; // parse "0,1" to List<Integer> in post-processing if needed
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static class Endpoint {
        @XmlAttribute(name = "Id")
        public String id;

        @XmlAttribute(name = "Value")
        public String value;
    }

    @XmlAccessorType(XmlAccessType.FIELD)
    public static class ReadKey {
        @XmlAttribute(name = "Id")
        public String id;

        @XmlValue
        public String value;
    }
}