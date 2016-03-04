import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import com.sun.image.codec.jpeg.TruncatedFileException;

/**
 * Clasa ce reprezinta o solutie partiala pentru problema de rezolvat. Aceste
 * solutii partiale constituie task-uri care sunt introduse in workpool.
 */
abstract class PartialSolution {

	public String docName;
	
	public PartialSolution(String docName) {
		this.docName = docName;
	}
	
}

class PartialMapSolution extends PartialSolution {
	
	public int length;
	public long offset;
	public static ArrayList<Pair<String, ArrayList<HashMap<Integer,Integer>>>> mapResults;
	public static ArrayList<Pair<String, ArrayList<ArrayList<String>>>> maxWords;
	public HashMap<Integer, Integer> res;
	public ArrayList<String> bigWords;
	
	public PartialMapSolution(String docName, long offset, int length) {
		super(docName);
		this.offset = offset;
		this.length = length;
		
	}
	
	public static void initialize() {
		mapResults = new ArrayList<Pair<String, ArrayList<HashMap<Integer,Integer>> >>();
		maxWords = new ArrayList<Pair<String,ArrayList<ArrayList<String>>>>();
		for (int j = 0; j < ReplicatedWorkers.fNames.size(); j++) {
			mapResults.add(new Pair<String, ArrayList<HashMap<Integer,Integer>>>(ReplicatedWorkers.fNames.get(j),
					new ArrayList<HashMap<Integer,Integer>>()));
			maxWords.add(new Pair<String, ArrayList<ArrayList<String>>>(ReplicatedWorkers.fNames.get(j),
					new ArrayList<ArrayList<String>>()));
		}
	}
	
	@Override
	public String toString() {
		return "MapSolution " + docName + " " + offset + " " +length;
	}
}
class PartialReduceSolution extends PartialSolution {
	
	public static ArrayList<Pair<String,HashMap<Integer,Integer>>> reducedResults;
	public static ArrayList<Pair<String, ArrayList<String>>> reducedMaxWords;
	public static ArrayList<Pair<String,Double>> costs;
	
	public PartialReduceSolution(String docName) {
		super(docName);
	}
	
	public static void initialize() {
		reducedResults =  new ArrayList<Pair<String,HashMap<Integer,Integer>>>();
		reducedMaxWords = new ArrayList<Pair<String, ArrayList<String>>>();
		costs = new ArrayList<Pair<String,Double>>();
		
	}
	public String toString() {
		return "ReduceSolution " + docName;
	}
}
/**
 * Clasa ce reprezinta un thread worker.
 */
class Worker extends Thread {
	WorkPool wp;
	public static String separators = " ;:/?~\\.,><~`[]{}()!@#$%^&-_+'=*\"| \t\n\r";
	public static Boolean isMapFinished = false;
	
	public Worker(WorkPool workpool) {
		this.wp = workpool;
	}

	/**
	 * Procesarea unei solutii partiale. Aceasta poate implica generarea unor
	 * noi solutii partiale care se adauga in workpool folosind putWork().
	 * Daca s-a ajuns la o solutie finala, aceasta va fi afisata.
	 */
	void processPartialSolution(PartialMapSolution ps) throws IOException {
		
		String frag, word;
		byte[] bytes = new byte[ps.length];
		char c;
		boolean startInCenter;
		StringTokenizer st;
		int value, maxLength = -1, k;

		RandomAccessFile ra = new RandomAccessFile(ps.docName,"r");
		if (ps.offset > 1) {//daca nu e primul fragment
			ra.seek(ps.offset-1);
			if ( contineChar(separators, (char)(ra.readByte())) ) {//daca caracterul anterior e separator
				startInCenter = false;
			} else {
				startInCenter = true;
			}
		} else {
			startInCenter = false;//daca e primul fragment sigur nu incepe in centru
		}
		
		ra.read(bytes);
		frag = new String(bytes, "UTF-8");
		c = frag.charAt(frag.length()-1);
		if (!contineChar(separators, c)) {
			while (!contineChar(separators, c)) {
				try {
					c = (char)ra.readByte();
					if (!contineChar(separators, c)) {//daca e separator nu-l adaug
						frag+=c;
					}
				} catch (EOFException eof) {break;};
			}
		}

		ra.close();
		st = new StringTokenizer(frag, separators);
		if (startInCenter && st.hasMoreTokens()) {
			st.nextToken();
		}
		ps.res = new HashMap<Integer,Integer>();
		while (st.hasMoreTokens()) {
			word = st.nextToken().toLowerCase();
			if (word.length() == 0) {
				continue;
			}
			
			if (ps.res.containsKey(word.length())) {
				value = ps.res.get(word.length());
				ps.res.remove(word.length());
				value++;
				ps.res.put(word.length(), value);
			} else {
				ps.res.put(word.length(), 1);
			}
			if (word.length() > maxLength) {
				ps.bigWords = new ArrayList<String>();
				ps.bigWords.add(word);
				maxLength = word.length();
			} else {
				if (word.length() == maxLength) {
					if (!ps.bigWords.contains(word)) {
						ps.bigWords.add(word);
					}
				}
			}
		}
		
		synchronized(PartialMapSolution.mapResults) {
			for (k = 0; k < PartialMapSolution.mapResults.size(); k++) {
				if (PartialMapSolution.mapResults.get(k).first.equals(ps.docName)) {
					PartialMapSolution.mapResults.get(k).second.add(ps.res);
					break;
				}
			}
		}
		synchronized(PartialMapSolution.maxWords) {
			PartialMapSolution.maxWords.get(k).second.add(ps.bigWords);
		}
	}
	void processPartialSolution(PartialReduceSolution ps) throws IOException {
		int j, key, value, maxLen;
		HashMap<Integer, Integer> hm;
		Object[] keys;
		ArrayList<String> bigW = new ArrayList<String>();
		
		for (j = 0; j < ReplicatedWorkers.nrFiles; j++) {
			if (PartialMapSolution.mapResults.get(j).first.equals(ps.docName)) {
				break;
			}
		}
		hm = new HashMap<Integer,Integer>();
		for (int k = 0; k < PartialMapSolution.mapResults.get(j).second.size(); k++) {//trec prin ArrayListul de hashmapuri corespunzator docului
			keys = PartialMapSolution.mapResults.get(j).second.get(k).keySet().toArray();
			for (int r = 0; r < keys.length; r++) {
				if (hm.containsKey(keys[r])) {
					value = hm.get(keys[r]);
					hm.remove(keys[r]);
					value += PartialMapSolution.mapResults.get(j).second.get(k).get(keys[r]);
					hm.put((int)keys[r], value);
				} else {
					value = PartialMapSolution.mapResults.get(j).second.get(k).get(keys[r]);
					hm.put((int)keys[r], value);
				}
			}
		}
		synchronized(PartialReduceSolution.reducedResults) {
			PartialReduceSolution.reducedResults.add(new Pair<String,HashMap<Integer,Integer>>(ReplicatedWorkers.fNames.get(j), hm));
		}
		
		maxLen = 0;
		for (int k = 0; k < PartialMapSolution.maxWords.get(j).second.size(); k++ ) {
			if (PartialMapSolution.maxWords.get(j).second.get(k) != null) {
				if (PartialMapSolution.maxWords.get(j).second.get(k).get(0).length() > maxLen) {
					bigW = new ArrayList<String>();
					bigW.addAll(PartialMapSolution.maxWords.get(j).second.get(k));
					maxLen = PartialMapSolution.maxWords.get(j).second.get(k).get(0).length();
				} else {
					if (PartialMapSolution.maxWords.get(j).second.get(k).get(0).length() == maxLen) {
						bigW.addAll(PartialMapSolution.maxWords.get(j).second.get(k));
					}
				}
			}
		}
		bigW = elimDup(bigW);
		synchronized(PartialReduceSolution.reducedMaxWords) {
			PartialReduceSolution.reducedMaxWords.add(new Pair<String, ArrayList<String>>(ReplicatedWorkers.fNames.get(j), bigW));
		}
		//Etapa 2 din Reduce: calcul cost
		int totalCuv = 0;
		double cost = 0;
		keys = hm.entrySet().toArray();
		for (int r = 0; r < keys.length; r++) {//calculez nr total de cuvinte din fisier
			totalCuv += ((Map.Entry<Integer, Integer>)(keys[r])).getValue();
		}
		for (int r = 0; r < keys.length; r++) {
			cost += (this.fibbo(((Map.Entry<Integer, Integer>)(keys[r])).getKey()+1)  
					* ((Map.Entry<Integer, Integer>)(keys[r])).getValue() ) / totalCuv ;
		}
		synchronized(PartialReduceSolution.costs) {
			PartialReduceSolution.costs.add(new Pair<String,Double>(ps.docName, truncScndDecimal(cost)));
		}
	}
	
	public static double truncScndDecimal(double a) {
		return Math.floor(100 * a) / 100; 
	}
	
	public static double fibbo(int n) {
		if (n == 0) return 0;
		if (n == 1) return 1;
		return fibbo(n-1)+fibbo(n-2);
	}
	public static ArrayList<String> elimDup(ArrayList<String> al) {
		HashSet<String> hs = new HashSet<String>();
		hs.addAll(al);
		al.clear();
		al.addAll(hs);
		return al;
	}
	public void run() {
		
		while (true) {
			if (!isMapFinished) {
				PartialMapSolution ps = (PartialMapSolution) wp.getWork();
				if (ps == null) {
					break;
				}
					
				try {
					processPartialSolution(ps);
				} catch (IOException ioe) {ioe.printStackTrace();}
			} else {
				PartialReduceSolution ps = (PartialReduceSolution) wp.getWork();
				if (ps == null) {
					break;
				}
				try {
					processPartialSolution(ps);
				} catch (IOException ioe) {ioe.printStackTrace();}
			}
			
		}
	}
	public static boolean contineChar(String str, char c) {
		for (int j = 0; j < str.length(); j++) {
			if (str.charAt(j) == c) {
				return true;
			}
		}
		return false;
	}

}


public class ReplicatedWorkers {
	
	public static int nrWorkers, nrFiles, fragmentSize;
	public static ArrayList<String> fNames;
	public static ArrayList<Worker> workers;
	
	public static void citire(String args[]) throws IOException {

		nrWorkers = Integer.parseInt(args[0]);
		Scanner s = new Scanner(new File(args[1]));
		fragmentSize = s.nextInt();
		nrFiles = s.nextInt();
		workers = new ArrayList<Worker>(nrWorkers);
		fNames = new ArrayList<String>(nrFiles);
		s.nextLine();
		for (int i = 0; i < nrFiles; i++) {
			fNames.add(s.nextLine());
		}
		s.close();
		
		PartialMapSolution.initialize();
		
	}
	
	public static void scriere(String args[]) throws IOException {
	
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(args[2])));
		for (int j = 0; j < nrFiles; j++) {
			bw.write(PartialReduceSolution.costs.get(j).first+ ";" + String.format("%.2f", PartialReduceSolution.costs.get(j).second)+ ";");
			for (int k = 0; k < nrFiles; k++) {
				if (PartialReduceSolution.costs.get(j).first.equals(PartialReduceSolution.reducedMaxWords.get(k).first)) {
					bw.write("[" + PartialReduceSolution.reducedMaxWords.get(k).second.get(0).length()+ ",");
					bw.write(PartialReduceSolution.reducedMaxWords.get(k).second.size()+ "]\n");
					break;
				}
			}
			
		}
		
		bw.close();
	}
	public static void main(String args[]) throws IOException {
		// ..
		long t1 = System.nanoTime();
		long offset;
		long fSize;
		File f;
		citire(args);
		
		WorkPool wp = new WorkPool(nrWorkers);
		for (int i = 0; i < nrWorkers; i++) {
			workers.add(new Worker(wp));
		}
		
		for (int i = 0; i < nrFiles; i++) {
			f = new File(fNames.get(i));
			fSize = f.length();
			offset = 0;
			while (offset < fSize) {
				if (offset+fragmentSize <= fSize) {
					wp.putWork(new PartialMapSolution(fNames.get(i), offset, fragmentSize));
					offset += fragmentSize;
				} else {
					wp.putWork(new PartialMapSolution(fNames.get(i), offset, (int)(fSize-offset) ));
					offset += fSize-offset;
				}		
			}
		}
		for (int i = 0; i < nrWorkers; i++) {
			workers.get(i).start();
		}
		
		try {
			for (int i = 0; i < nrWorkers; i++) {
				workers.get(i).join();
			}
		} catch (InterruptedException e) { System.out.println("InterruptedException");};
		
		
		//////Etapa Reduce
		Worker.isMapFinished = true;
		PartialReduceSolution.initialize();
		workers = new ArrayList<Worker>(nrWorkers);
		for (int i = 0; i < nrWorkers; i++) {
			workers.add(new Worker(wp));
		}
		for (int j = 0; j < nrFiles; j++) {
			wp.putWork(new PartialReduceSolution(fNames.get(j)));
		}
		for (int i = 0; i < nrWorkers; i++) {
			workers.get(i).start();
		}
		try {
			for (int i = 0; i < nrWorkers; i++) {
				workers.get(i).join();
			}
		} catch (InterruptedException e) { System.out.println("InterruptedException");};
		
		Collections.sort(PartialReduceSolution.costs, new Comparator1());
		System.nanoTime();
		scriere(args);
		long t2 = System.nanoTime();
		System.out.println((t2-t1)/1000000);
	}
		
}