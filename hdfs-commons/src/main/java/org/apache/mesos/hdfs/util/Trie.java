package org.apache.mesos.hdfs.util;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of Trie data structure.
 * See more at http://en.wikipedia.org/wiki/Trie
 */
public class Trie<K, M> implements Serializable, Cloneable {
    private K key;
    private M model;
    private Map<K, Trie<K, M>> children;

    public Trie() {
        this(null, null);
    }

    public Trie(K key, M model) {
        this.key = key;
        this.model = model;
        this.children = new HashMap<>();
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public M getModel() {
        return model;
    }

    public void setModel(M model) {
        this.model = model;
    }

    public Map<K, Trie<K, M>> getChildren() {
        return children;
    }

    public void removeChildren() {
        children.clear();
    }

    public Trie<K, M> getChild(K key) {
        return children.get(key);
    }

    public Trie<K, M> addChild(Trie<K, M> node) {
        if (children.containsKey(key)) {
            children.get(node.getKey()).setModel(node.getModel());
            node = children.get(node.getKey());
        } else {
            children.put(node.getKey(), node);
        }

        return node;
    }

    public Trie<K, M> removeChild(K key) {
        return children.remove(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Trie)) {
            return false;
        }

        Trie trie = (Trie) o;
        return new EqualsBuilder().append(key, trie.key).build();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(key).build();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE).append("key", key)
                                                                        .append("model", model)
                                                                        .append("children", children)
                                                                        .build();
    }
}
