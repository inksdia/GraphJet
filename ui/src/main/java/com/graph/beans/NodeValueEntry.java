package com.graph.beans;

public class NodeValueEntry implements Comparable<NodeValueEntry> {
    private final long node;
    private final double value;

    public NodeValueEntry(long node, double value) {
        this.node = node;
        this.value = value;
    }

    public long getNode() {
        return node;
    }

    public double getValue() {
        return value;
    }

    @Override
    public int compareTo(NodeValueEntry other) {
        if (this.value < other.value) {
            return -1;
        }

        if (this.value > other.value) {
            return 1;
        }

        return this.node < other.node ? -1 : 1;
    }

    @Override
    public String toString() {
        return "(" + this.node + ", " + this.value + ")";
    }
}