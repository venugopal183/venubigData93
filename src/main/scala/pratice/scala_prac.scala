package pratice

object scala_prac {
  def main(args: Array[String]): Unit = {
    var name = "venu"
    var age = 23
    println("My name is " + name + " my age is " + age)
    println(s"My name is $name and my age is $age")
    println(f"My name is $name%s and my age is $age%d")
    println(s"Hello \n World")
    println(raw"hello \n World")

    var x = 0
    var res = ""

    println("if else statements")
    if (x == 20) {
      println("x is equal to 20");
      res = "x==20";


    }
    else {
      println("x is not equal to 20");
      res = "x!= 20";

    }
    println(res)

    println("while loops")


    while (x < 10) {
      println("x= " + x)
      x += 1 //increment x by 1

    }
    println("do while loop")

    var z = 25

    do {
      println("z " + z)
      x += 1


    } while (x < 26)

    println("for loops in scala")

    println(" for( var a a<- Range)")

    for (i <- 1 to 5) {
      println(i)
    }

    for (i <- 1.to(5)) { //using .to give the range
      println(i)
    }

    for (i <- 1.until(6)) { //using .until to specify the range
      println(i)
    }
    var result = 0;

    for( i <- 1.to(10); j <- 11.until(21)){
      result = i+j;
      println(result)
    }

    val lis =List(1.to(100))
    println(lis)
    val raw_lis = List(1,2,3,4,5,6,7,8,9)

    for (i <- raw_lis; if i < 6){
      println(i)

    }



  }

}
