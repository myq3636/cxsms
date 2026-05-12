package com.king.gmms.connectionpool.node;


/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public enum NodeType {
    TR,
    T,
    R;

    public static NodeType getNodeType(String name) {
        if (name == null || name.length() < 0) {
            return NodeType.TR;
        }
        else if (name.equalsIgnoreCase("T")) {
            return NodeType.T;
        }
        else if (name.equalsIgnoreCase("R")) {
            return NodeType.R;
        }
        else {
            return NodeType.TR;
        }
    }
}
