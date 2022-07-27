package yaml;

import java.util.List;

public class NebulaSink {

    public List<SinkTag> tagList;
    public List<SinkEdge> edgeList;


    public List<SinkTag> getTagList() {
        return tagList;
    }

    public void setTagList(List<SinkTag> tagList) {
        this.tagList = tagList;
    }

    public List<SinkEdge> getEdgeList() {
        return edgeList;
    }

    public void setEdgeList(List<SinkEdge> edgeList) {
        this.edgeList = edgeList;
    }
}
