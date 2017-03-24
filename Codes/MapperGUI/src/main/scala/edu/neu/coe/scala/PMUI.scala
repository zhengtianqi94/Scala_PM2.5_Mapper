package edu.neu.coe.scala

// 导入scala.swing下所有的包
import java.awt.Dimension
import java.io.File

import scala.swing._
import scala.swing.event.ButtonClicked

object PMUI extends SimpleSwingApplication{

  val button = new Button{
    text = "Submit"
  }

  val label1 = new Label{
    text = "Ploty Username: "
  }

  val textf1 = new TextField(){
    maximumSize = new Dimension(200,30)
  }

  val label2 = new Label{
    text = "Ploty API Key: "
  }

  val textf2 = new TextField(){
    maximumSize = new Dimension(200,30)
  }

  val label3 = new Label{
    text = "File Location: "
  }

  val textf3 = new TextField(){
    maximumSize = new Dimension(200,30)
  }

  val panel1 = new GridPanel(1,2){
    contents += label1
    contents += textf1
    maximumSize = new Dimension(300,30)
  }

  val panel2 = new GridPanel(1,2){
    contents += label2
    contents += textf2
    maximumSize = new Dimension(300,30)
  }

  val panel3 = new GridPanel(1,2){
    contents += label3
    contents += textf3
    maximumSize = new Dimension(300,30)
  }

  val panel4 = new BoxPanel(Orientation.Vertical){
    contents += panel3
  }

  var fileChooser = new FileChooser(new File("."))

  fileChooser.title = "fileChooser"

  val Choose = new Button {
    text = "Choose a File from local 1";
  }

  val Choose2 = new Button {
    text = "Choose a File from local 2"
  }

  val Choose3 = new Button {
    text = "Choose a File from local 3"
  }

  val Choose4 = new Button {
    text = "Choose a File from local 4"
  }

  val Choose5 = new Button {
    text = "Choose a File from local 5"
  }


  val panel5 = new FlowPanel{
    contents += Choose
    maximumSize = new Dimension(300,30)
  }
  val panel6 = new FlowPanel{
    contents += Choose2
    maximumSize = new Dimension(300,30)
  }
  val panel7 = new FlowPanel{
    contents += Choose3
    maximumSize = new Dimension(300,30)
  }
  val panel8 = new FlowPanel{
    contents += Choose4
    maximumSize = new Dimension(300,30)
  }
  val panel9 = new FlowPanel{
    contents += Choose5
    maximumSize = new Dimension(300,30)
  }



  def top = new MainFrame{

    title = "PM2.5 Mapper"

    contents = new BoxPanel(Orientation.Vertical){
      contents += panel1
      contents += panel2
      contents += panel4
      contents += panel5
      contents += panel6
      contents += panel7
      contents += panel8
      contents += panel9
      contents += button
      border = Swing.EmptyBorder(20,20,20,20)
    }

    size = new Dimension(300,300)

    listenTo(button)
    listenTo(Choose)
    listenTo(Choose2)
    listenTo(Choose3)
    listenTo(Choose4)
    listenTo(Choose5)

    reactions += {
      case ButtonClicked(Choose) => {
        val result = fileChooser.showOpenDialog(panel5);
        if (result == FileChooser.Result.Approve) {
          Choose.text = fileChooser.selectedFile.getPath;
        }
      }
      case ButtonClicked(Choose2) => {
        val result = fileChooser.showOpenDialog(panel6);
        if (result == FileChooser.Result.Approve) {
          Choose2.text = fileChooser.selectedFile.getPath;
        }
      }
      case ButtonClicked(Choose3) => {
        val result = fileChooser.showOpenDialog(panel7);
        if (result == FileChooser.Result.Approve) {
          Choose3.text = fileChooser.selectedFile.getPath;
        }
      }
      case ButtonClicked(Choose4) => {
        val result = fileChooser.showOpenDialog(panel8);
        if (result == FileChooser.Result.Approve) {
          Choose4.text = fileChooser.selectedFile.getPath;
        }
      }
      case ButtonClicked(Choose5) => {
        val result = fileChooser.showOpenDialog(panel9);
        if (result == FileChooser.Result.Approve) {
          Choose5.text = fileChooser.selectedFile.getPath;
        }
      }
      case ButtonClicked(button) => {
        println("Ploty Username is " + textf1.text)
        println("Ploty API Key is " + textf2.text)
        println("File Location is " + textf3.text)
        println("File Location is " + Choose.text)
        println("File Location is " + Choose2.text)
        println("File Location is " + Choose3.text)
        println("File Location is " + Choose4.text)
        println("File Location is " + Choose5.text)
      }
    }
  }
}












