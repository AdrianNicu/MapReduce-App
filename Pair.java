class Pair<T1, T2> {
	T1 first;
	T2 second;
	public Pair(T1 f, T2 s) {
		this.first = f;
		this.second = s;
	}
	@Override
	public String toString() {
		return "" + first +" " + second;
	}
}