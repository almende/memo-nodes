package com.chap.memo.memoNodes;

//import java.util.Date;

import com.eaio.uuid.UUID;

public class MemoUtils {
	public static long gettime(UUID uuid){
		return gettime(uuid.time);
	}
	public static long gettime(long time){
		long result = time >> 32;
		result |=     (time & 0x00000000FFFF0000L) << 16;
		result |=     (time & 0x0000000000000FFFL) << 48;
//		System.out.println("time is:"+new Date((result-0x01B21DD213814000L)/10000).toString());
		return result;
	}
	public static final int binarySearch(long[] array, long key){
		int min = 0, max = array.length-1;
		long minVal = array[min], maxVal = array[max];
	    int nPreviousSteps = 0;
		
	    for (;;) {
	        if (key <= minVal) return key == minVal ? min : -1 - min;
	        if (key >= maxVal) return key == maxVal ? max : -2 - max;
	        
	        if (min == max) return -1;
	        int pivot;
	        // A typical binarySearch algorithm uses pivot = (min + max) / 2.
	        // The pivot we use here tries to be smarter and to choose a pivot close to the expectable location of the key.
	        // This reduces dramatically the number of steps needed to get to the key.
	        // However, it does not work well with a logaritmic distribution of values, for instance.
	        // When the key is not found quickly the smart way, we switch to the standard pivot.
	        if (nPreviousSteps > 2) {
	            pivot = (min + max) >> 1;
	            // stop increasing nPreviousSteps from now on
	        } else {
	            // NOTE: We cannot do the following operations in int precision, because there might be overflows.
	            //       long operations are slower than float operations with the hardware this was tested on (intel core duo 2, JVM 1.6.0).
	            //       Overall, using float proved to be the safest and fastest approach.
	            pivot = min + (int)((key - (float)minVal) / (maxVal - (float)minVal) * (max - min));
	            nPreviousSteps++;
	        }

	        long pivotVal = array[pivot];

	        // NOTE: do not store key - pivotVal because of overflows
	        if (key > pivotVal) {
	            min = pivot + 1;
	            max--;
	        } else if (key == pivotVal) {
	            return pivot;
	        } else {
	            min++;
	            max = pivot - 1;
	        }
	        maxVal = array[max];
	        minVal = array[min];
	    }
	}
}
