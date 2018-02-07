import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.io.File;
import java.util.Scanner;
import java.lang.Math;

public class simCompareV2 {

  final static double MAX_DIF = 0.2;

  public static void main(String[] args) {

    try {
      long starttime = System.nanoTime();
      File file = new File("gendPpl.txt");
      Scanner input =  new Scanner(file);
      ArrayList<double[]> trajectories = new ArrayList<double[]>();
      ArrayList<Double[]> scores = new ArrayList<Double[]>();
      double simUser = 0;
      int lvl=2;

      //READING IN DATA

      while(input.hasNextInt()) {
          int linelen = input.nextInt();
          double[] p1 = new double[linelen];
          for(int i = 0; i < linelen; i++) {
            p1[i] = input.nextInt();
          }
          int linelen2 = input.nextInt();
          double[] t1 = new double[linelen2];
          for(int i = 0; i < linelen2; i++) {
            t1[i] = input.nextDouble();
          }
          /*
          int linelen3 = input.nextInt();
          int[] p2 = new int[linelen3];
          for(int i = 0; i < linelen3; i++) {
            p2[i] = input.nextInt();
          }
          int linelen4 = input.nextInt();
          double[] t2 = new double[linelen4];
          for(int i = 0; i < linelen4; i++) {
            t2[i] = input.nextDouble();
          }*/

          //outputting 2 different test sequences
          System.out.println("person1 locations&staytimes");
          for(int i = 0; i < p1.length; i++) {
            System.out.print(p1[i] + " ");
          }
          System.out.println();

          for(int i = 0; i < t1.length; i++) {
            System.out.print(t1[i] + " ");
          }
          System.out.println();
          /*
          System.out.println("person2 location&staytimes");

          for(int i = 0; i < p2.length; i++) {
              System.out.print(p2[i] + " ");
          }
          System.out.println();

          for(int i = 0; i < t2.length; i++) {
            System.out.print(t2[i] + " ");
          }
          System.out.println();
          */
          trajectories.add(p1);
          trajectories.add(t1);


          //simUser = simScorePair(p1, t1, p2, t2, lvl);

          //list of matches (nodes for graphs)
          //for each layer of semantic meaning
        }


      long stime2 = System.nanoTime();

      if(!input.hasNextInt()) {
        long endtime2 = System.nanoTime();
        System.out.println("excluding IO: " + ((endtime2 - stime2)/1000000) );
      }


      for(int i=0; i < trajectories.size(); i += 2) {
        List<Double> currentScores = new ArrayList<Double>();
        for(int j=0; j < trajectories.size(); j += 2) {
          if(i != j) {
            simUser = simScorePair(trajectories.get(i), trajectories.get(i+1), trajectories.get(j), trajectories.get(j+1), lvl);
            currentScores.add(simUser);
          }
        }
        Double[] currentScoresArray = currentScores.toArray(new Double[currentScores.size()]);
        scores.add(currentScoresArray);
      }


      input.close();
      long endtime = System.nanoTime();

      System.out.println("entire program: " + ((endtime - starttime)/1000000));

    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }

  public static double simScorePair(double[] p1, double[] t1, double[] p2, double[] t2, int lvl) {

    double simsqscore=0;
    double simUser = 0;
    int[] shiftp1 = new int[p1.length];
    int[] shiftp2 = new int[p2.length];

    for (int m=0; m < lvl; m++) {
        System.out.println("LEVEL " + (lvl-m));
        if(m==1) {
          p1 = levelShift(p1);
          p2 = levelShift(p2);

          System.out.println("shiftedPerson1:");

          for (int i=0; i < shiftp1.length; i++) {
            System.out.println(p1[i]);
          }

          System.out.println("shiftedPerson2:");

          for (int i=0; i < shiftp2.length; i++) {
            System.out.println(p2[i]);
          }

        }



        ArrayList<int[]> matches = new ArrayList<int[]>();
        ArrayList<ArrayList<Integer>> maxMatches = new ArrayList<ArrayList<Integer>>();
        ArrayList<Integer> eachMatch = new ArrayList<Integer>();
        //checks for matches between them
        //adds matches to list i=index of match in p1
        //                     j=index of match in p2
        //third zero in current sets the node as white

        for (int i=0; i < p1.length; i++) {
          for (int j=0; j < p2.length; j++) {
            if(p1[i] == p2[j]) {
              int[] current = {0,0,0};
              current[0] = i;
              current[1] = j;
              matches.add(current);
            }
          }
        }

        //put in decreasing lexigraphical order
        Collections.reverse(matches);


        //printing out list of matches for testing
        System.out.println("number of matches: " + matches.size());
        System.out.println("Indexes of matches");
        for (int i=0; i < matches.size(); i++) {
          System.out.print(matches.get(i)[0] + " ");
          System.out.println(matches.get(i)[1]);
        }

        //create adjacency matrix to store graph
        //if no matches adds a 0 to avoid index out of bounds
        if(matches.size() == 0) {
          int[] fakedata = {0,0,0};
          matches.add(fakedata);
        }
        int[][] commonLoc = new int[matches.size()][matches.size()];


        //builds graph and blacks out nodes
        //

        for (int l = 1; l < matches.size(); l++) {
          for (int t=l-1; t > -1; t--) {
            if (matches.get(l)[2] == 0) {
              if (precTest(matches, l, t, t1, t2)) {
                if(commonLoc[l][t] != -1) {
                    commonLoc[l][t] = 1;
                }
                for (int i=0; i < matches.size(); i++) {
                  if(commonLoc[t][i] == 1) {
                    matches.get(i)[2] = -1;
                  }
                }
              }
            }
          }
        }


        //prints out the adjacency matrix

        System.out.println("//////////////////");
        System.out.print("   ");
        for (int i=0; i < matches.size(); i++)
          System.out.print((i+1) + " ");
        System.out.println();
        for (int i=0; i < matches.size(); i++) {
          System.out.print((i+1) + " " + (i < 9 ? " " : ""));
          for (int j=0; j < matches.size() ;j++ ) {
            System.out.print(commonLoc[i][j] + " ");
          }
          System.out.println();
        }

        //finding max match
        maximumMatch(commonLoc, commonLoc[0].length-1, commonLoc[0].length-1, maxMatches, eachMatch);
        System.out.println();

        /*for (int i=0; i < maxMatches.size(); i++) {
          System.out.print(maxMatches.get())
        }*/



          if(maxMatches.size() > 1) {
            int totalsize = 0;
            for (int j=1; j < maxMatches.size(); j++) {
              totalsize += maxMatches.get(j-1).size();
              for (int i=0; i < totalsize;i++) {
                maxMatches.get(j).remove(0);
              }
            }
          }

          System.out.println(maxMatches);

          double sgscore = 0;

          sgscore = simScore(maxMatches);

          simsqscore = sgscore / (double)(p1.length * p2.length);

          simUser += weightFunc(lvl-m) * simsqscore;

        }

    System.out.println(simUser);
    System.out.println(simUser/(simUser+1)); //normalized score


    return simUser;
  }

  public static double simScore(ArrayList<ArrayList<Integer>> maxMatches) {
    int score = 0;
    //eq 4 in paper
    for (int i=0; i < maxMatches.size(); i++) {
      score += weightFunc(maxMatches.get(i).size());
    }

    //eq3

    //score = score / (p1.length * p2.length);



    return score;
  }

  public static void maximumMatch(int[][] commonLoc, int srow, int scol, ArrayList<ArrayList<Integer>> maxMatches, ArrayList<Integer> eachMatch) {
    if(outDeg(commonLoc, srow)) {
      //System.out.print("start");
      eachMatch.add(srow+1);
      System.out.print(srow+1);
      System.out.println("end");
      eachMatch = new ArrayList<Integer>(eachMatch);
      maxMatches.add(eachMatch);
      return;
    }
    for(int i=scol; i > -1; i--) {
      if(commonLoc[srow][i] == 1) {
        eachMatch.add(srow+1);
        System.out.print((srow+1) + "(");
        maximumMatch(commonLoc, i, i /*can b i-1*/, maxMatches, eachMatch);
        //System.out.print(srow+1);
      }
    }



    return;
  }

  public static int weightFunc(int lvl) {
    return (int) Math.pow(2,(lvl-1));
  }

  //currently hard coded. Need to implement a search by value in tree

  public static double[] levelShift(double[] ogPers) {
    double[] shiftPers = new double[ogPers.length];

    for (int i=0; i < ogPers.length; i++) {
      if(ogPers[i] == 1 || ogPers[i] == 2 || ogPers[i] == 3) {
        shiftPers[i] = 11;
      }

      if(ogPers[i] == 4 || ogPers[i] == 5) {
        shiftPers[i] = 12;
      }
    }

    return shiftPers;
  }

  public static boolean outDeg(int[][] commonLoc, int srow) {
    int total = 0;
    for(int i=0; i<commonLoc[0].length; i++) {
      total += commonLoc[srow][i];
    }

    if(total==0)
      return true;

    return false;
  }

  public static boolean precTest(ArrayList<int[]> matches, int l, int t, double[] t1, double[] t2) {
    double total1 = 0;
    double total2 = 0;
    double travDif = 0;

    if(matches.get(l)[0] <= matches.get(t)[0] && matches.get(l)[1] < matches.get(t)[1]) {
      for(int i =(matches.get(l)[0] * 2) + 1; i < (matches.get(t)[0] * 2); i++) {
        total1 += t1[i];
      }
      for(int i =(matches.get(l)[1] * 2) + 1; i < (matches.get(t)[1] * 2); i++) {
        total2 += t2[i];
      }

      travDif = Math.abs(total1-total2)/(Math.max(total1, total2));
      //System.out.println("dif" + travDif);
      if( travDif <= MAX_DIF) {
        return true;
      }
    }
    return false;
  }
}
