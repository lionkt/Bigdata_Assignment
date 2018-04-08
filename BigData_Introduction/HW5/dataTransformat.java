
//package BFS;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class dataTransformat {

    public static final int K = 20;

    public static void OutPut(String EdgeFile, String OutputFile, List TopODList) {
        try {
            BufferedReader edge = new BufferedReader(new FileReader(new File(EdgeFile)));
            BufferedWriter out = new BufferedWriter(new FileWriter(new File(OutputFile)));
            String line = null;
            int i = 1;
            while ((line = edge.readLine()) != null) {
                String[] subline = line.split("\t");
                if(TopODList.contains(subline[0])) {
                    subline[1] = subline[1] + "|0|GRAY";
                } else {
                    subline[1] = subline[1] + "|Integer.MAX_VALUE|WHITE";
                }
                out.write(subline[0] + "\t" + subline[1]);
                out.newLine();
                if (i%50000 == 0){
                    System.out.println(String.valueOf(i));
                }
                i++;
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String input_path = "G:\\graph_data\\";
        String output_path = "G:\\graph_data\\format_data\\";
        String EdgeFile = "case3-adj";
        String OutDegreeFile = "case3-od";
        String OutFile = EdgeFile + "-format";
        List TopODList = new ArrayList();
        System.out.println("=== build list accroding to out-degree ===");
        try {
            BufferedReader fBuf = new BufferedReader(new FileReader(new File(input_path + OutDegreeFile)));
            String line = null;

            for (int i = 0; i < K && (line = fBuf.readLine()) != null; i++) {
                String[] subline = line.split("\t");
                TopODList.add(subline[0]);
            }
            fBuf.close();
            System.out.println("Front:" + TopODList.size());
        } catch (IOException e){
            e.printStackTrace();
        }
        System.out.println("=== re-format the adjecent relation ===");
        OutPut(input_path + EdgeFile, output_path + OutFile, TopODList);
    }

}
