package com.hadoop.mapReduce.wordcount;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

import org.apache.pdfbox.exceptions.COSVisitorException;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFMergerUtility;
 
public class PDFMerger {
 public static void main(String[] args) throws IOException, COSVisitorException {
  int maxPdf = 1500; // use this to troubleshoot java/lang/OutOfMemoryError exception
  String pdfDirPath = "/home/cloudera/ebooks/pdfs";
 
  File pdfDir = new File(pdfDirPath);
  if (pdfDir.isDirectory()) {
   Date startTime = new Date();
   System.out.println("Start time: " + startTime.toString());
 
   // proceed to crawl thru the folder and merge the pdf according to last mod date
   File[] pdfs = pdfDir.listFiles();
   int cnt = pdfs.length;
 
   if (cnt > 0) {
    // sort the pdfs by last mod date in desc order
    Arrays.sort(pdfs, new Comparator<File>() {
     public int compare(File f1, File f2) {
      return (int) (f2.lastModified()-f1.lastModified());
     }
    });
 
    if (maxPdf != 0 && maxPdf < cnt) {
     cnt = maxPdf;
    }
 
    // create a temp file for temp pdf stream storage
    String tempFileName = (new Date()).getTime() + "_temp";
    File tempFile = new File("/home/cloudera/ebooks/" + tempFileName);
    System.out.println("tempFile   : "+tempFile.getName());
    // proceed to merge
    PDDocument desPDDoc = null;
    PDFMergerUtility pdfMerger = new PDFMergerUtility();
    try {
     // traverse the files
     boolean hasCloneFirstDoc = false;
     for (int i = 0; i < cnt; i++) {
      File file = pdfs[i];
      PDDocument doc = null;
      try {
       if (hasCloneFirstDoc) {
        doc = PDDocument.load(file);
        pdfMerger.appendDocument(desPDDoc, doc);
       } else {
        desPDDoc = PDDocument.load(file, new RandomAccessFile(tempFile, "rw"));
        hasCloneFirstDoc = true;
       }
      } catch (IOException ioe) {
       System.out.println("Invalid PDF detected: " + file.getName());
       ioe.printStackTrace();
      } finally {
       if (doc != null) {
        doc.close();
       }
      }
     }
 
     System.out.println("Merging and saving the PDF to its destination");
     desPDDoc.save("/home/cloudera/ebooks/mergeDoc.pdf");
 
     Date endTime = new Date();
     System.out.println("Process Completed: " + endTime);
     long timeTakenInSec = endTime.getTime() - startTime.getTime();
     System.out.println("Time taken: " + (timeTakenInSec / 1000) + " secs " + (timeTakenInSec % 1000) + " ms");
 
    } catch (IOException  e) {
     e.printStackTrace(); // will encounter issues if it is more than 850 pdfs!!
    } finally {
     try {
      if (desPDDoc != null) {
       desPDDoc.close();
      }
     } catch (IOException ioe) {
      ioe.printStackTrace();
     }
     tempFile.delete();
    }
   } else {
    System.out.println("Target directory is empty.");
   }
  } else {
   System.out.println("Target is not a directory (" + pdfDirPath + ").");
  }
 }
}
