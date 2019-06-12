//Program Author: Harshavardhan Reddy Bommireddy

package cg.samples.cg.samples


import java.io.File
import java.io.FileInputStream
import org.apache.poi.hssf.usermodel.{HSSFWorkbook, HSSFSheet }
//import org.apache.poi.ss.usermodel.{Cell, Row, Workbook, Sheet}

object ReadExcelFile extends App {


  val file: FileInputStream = new FileInputStream(new File("/Applications/MySoftware/test.xls"))
  val workbook: HSSFWorkbook = new HSSFWorkbook(file)
  val sheet: HSSFSheet = workbook.getSheetAt(0)

  println(s"Sheet Name: ${workbook.getSheetName(0)}")

  println(sheet.getPhysicalNumberOfRows)

  val data = {for{i <- 0 until (sheet.getPhysicalNumberOfRows() ); j <- 0 until 4}
    yield ((sheet.getRow(i).getCell(j)))}.toList

  println(data)
}
