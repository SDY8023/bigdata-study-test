object test {
  def main(args: Array[String]): Unit = {
    // 调用apply方法
    val persion = Persion("sdy", 26)

    // 使用unapply方法
    persion match {
      case Persion(name,24) => println(name)
      case _ => println("no match")
    }
  }

}
