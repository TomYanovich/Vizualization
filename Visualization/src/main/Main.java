package main;

import java.io.IOException;

public class Main {

	public static void main(String[] args) throws IOException {

		WordMatrix.init("Israel", Utils.DOMESTIC).fill().exportTSV();

	}
}
