package io.activej.dataflow.node;

public abstract class AbstractNode implements Node {
	private final int index;

	public AbstractNode(int index) {
		this.index = index;
	}

	@Override
	public int getIndex() {
		return index;
	}
}
