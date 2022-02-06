class Persion(var name:String,var age:Int){

}
object Persion{
  def apply(name:String,age:Int):Persion = {
    new Persion(name,age)
  }
  def unapply(persion: Persion):Option[(String,Int)]={
    if(persion == null){
      None
    } else{
      Some(persion.name,persion.age)
    }
  }
}
