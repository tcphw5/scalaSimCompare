import java.io.File;
import java.util.Scanner;
import java.util.Random;
import java.io.PrintWriter;
import java.io.IOException;

public class generatePeople {

  final static int MINPLACES = 4;
  final static int MAXPLACES = 7;
  final static int MINPLACEID = 1;
  final static int MAXPLACEID = 10;
  final static int MINSTAYTIME = 1;
  final static int MAXSTAYTIME = 5;
  final static int NUMOFPPL = 10; //must be even

  public static void main(String[] args) {
    try{
      PrintWriter writer = new PrintWriter("gendPpl.txt");
      Random rand = new Random();

      for(int j=0; j<NUMOFPPL; j++) {
        int numOfPlaces = rand.nextInt(MAXPLACES-MINPLACES + 1) + MINPLACES;
        int numOfTimes = numOfPlaces*2 -1;
        //int[] person1 = new int[numOfPlaces];
        //double[] times1 = new double[numOfPlaces*2 - 1];
        writer.print(numOfPlaces + " ");

        int prevNumID = 0;
        for (int i=0; i<numOfPlaces; i++) {
          int placeNumID = rand.nextInt(MAXPLACEID-MINPLACEID + 1) + MINPLACEID;
          while(placeNumID == prevNumID) {
            placeNumID = rand.nextInt(MAXPLACEID-MINPLACEID + 1) + MINPLACEID;
          }

          writer.print(placeNumID + " ");
          prevNumID = placeNumID;
        }
          writer.println();
          writer.print(numOfTimes + " 0 ");

        for (int i=0; i<numOfPlaces-1; i++) {
          double stayTime;
          double travTime;
          double halfhour = rand.nextDouble();
          int istayTime = rand.nextInt(MAXSTAYTIME-MINSTAYTIME + 1) + MINSTAYTIME;
          int itravTime = rand.nextInt(MAXSTAYTIME-MINSTAYTIME + 1) + MINSTAYTIME;
          stayTime = (halfhour>0.5) ? istayTime + 0.5 : (double)istayTime;
          travTime = (halfhour>0.5) ? itravTime + 0.5 : (double)itravTime;
          if (i == 0) {
              //do nothin
          } else {
            writer.print(stayTime + " ");
          }
          writer.print(travTime + " ");

        }

          writer.println("0");
      }
      writer.close();
    } catch(IOException e) {
      System.exit(0);
    }
  }
}
