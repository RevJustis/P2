class MyMenu(options: List[String]) {
  //map of all the map options. The int is in the position of the menu option starting at 0. The string is the return value for when its selected
  val menuOptions = options;

  private def printMenuLine(): Unit = {
    //prints a line that goes on the top of bottom of a menu
    println("+-----------------------+")
  }

  private def printMenuOption(ind: Int, menVal: String): Unit = {
    //Prints out a section of the menu that is a set amount of characters long.
    val maxLength = 25;
    var tempStr = "| "
    tempStr += (ind + 1).toString + " -> " + menVal
    while (tempStr.length < (maxLength - 1)) {
      tempStr += " "
    }
    tempStr += "|"
    println(tempStr)
  }

  private def printEmptyMenuLine(): Unit = {
    //prints and empty menu line to avoid confusion
    println("|                                                |")
  }

  def printMenu(): Unit = {
    //prints the entire menu with all options
    println("+-Options---------------+")
    menuOptions.foreach(m => { printMenuOption(menuOptions.indexOf(m), m) })
    printMenuLine()
  }

  def selectOption(cho: Byte): String = {
    menuOptions(cho - 1);
  }
}
