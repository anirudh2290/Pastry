
import scala.concurrent.Future
import akka.actor.Actor.Receive
import akka.actor._
import java.security.MessageDigest  
import Worker._
import scala.collection.mutable.ListBuffer
import scala.collection.TraversableOnce
import scala.util.control._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory

case class join(neighbourId: String)
case class updateTables(hopNo: Int, rTable: Array[String], lsMinus: ArrayBuffer[String], lsPlus: ArrayBuffer[String], finalNode: Boolean)
case class route(msg: String, neighbourNodeId: String, senderNodeId: String, join: Boolean, newNode: Boolean, hopNumber: Int, lastNode: Boolean)


object Worker {
    
  
  def props(ac: ActorSystem, senderBoss: ActorRef, numNodes:Int, base:Int): Props =
    Props(classOf[Worker], ac, senderBoss, numNodes, base)
    // NOTE: byteKey is a nodeId expressed simply as an array of bytes (no base conversion)
}

 class Worker(ac: ActorSystem, superBoss: ActorRef, numNodes:Int, b:Int) extends Actor {


  val RTrows = scala.math.pow(2, b).toInt
  val RTcols = 64/b

  // Each actor will have a routing table,  neighbour set, leaf set

  var routingTable = Array.ofDim[String](RTrows, RTcols) //, RTcols)       //of size log(numNodes)/log(2^b) rows X 2^b columns
  //var leafSetMinus = Array.ofDim[String](RTcols/2)
  //var leafSetPlus  = Array.ofDim[String](RTcols/2)
  var leafSetMinus:ArrayBuffer[String] = new ArrayBuffer[String]
  var leafSetPlus:ArrayBuffer[String] = new ArrayBuffer[String]
  var leafMinusIndex: Int = 0
  var leafPlusIndex: Int = 0
  /*
  var neighbourSet: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]	//of size 2*(2^b)
  var leafSetMinus: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]	//of size (2^b)/2
  var leafSetPlus: ArrayBuffer[ActorRef] = new ArrayBuffer[ActorRef]    //of size (2^b)/2
  */
  
  def receive = {
    
    case "test" => testCompare()
    case join(neighbourId: String) => join(sender, neighbourId)
    //case default => println("Entered default : Received message "+default);

    
  }


  def join(senderBoss: ActorRef, neighbourId: String):Unit = {
    println("Inside testInit")
    var i = 0
    var sum = 0
    /*
    for (i <- 0 to (16-1)) {
      println("Inside joinOnebyOne")
      sum = sum + i
    }
    */
    println("neighbourId is " + neighbourId)
    println("sum is " + sum)
    println("Completing testInit")
    if(neighbourId == "") {
      println("="*10)
      println("Starting node")
      println("="*10)
    } else {
      println("="*10)
      println("Node >= 1")
      println("="*10)
      /*Added by Anirudh Subramanian for testing Begin*/
      /*Tests for findMinimumLeafSet*/
      /*
      leafSetMinus += "03"
      leafSetMinus += "04"
      leafSetMinus += "05"
      leafSetMinus += "07"

      leafSetPlus += "A"
      leafSetPlus += "B"
      leafSetPlus += "C"
      leafSetPlus += "D"
      leafSetPlus += "E"
      println("="*10 + "findMinimumLeafSet output" + "="*10)
      println(findMinimumLeafSet(12).toString(16))
      println("="*10 + "findMinimumLeafSet output" + "="*10)
      */
      /*findMinimumLeafSet working well*/
      /*
      leafSetMinus += "03"
      leafSetMinus += "04"
      leafSetMinus += "05"
      leafSetMinus += "07"

      leafSetPlus += "A"
      leafSetPlus += "B"
      leafSetPlus += "C"
      leafSetPlus += "D"
      leafSetPlus += "E"
      println("="*10 + "findMinimumLeafSet output" + "="*10)
      println(findMinimumLeafSet(12).toString(16))
      println("="*10 + "findMinimumLeafSet output" + "="*10)
      */
      /*Added by Anirudh Subramanian for testing End*/
      /*Commented by Anirudh Subramanian for testing Begin*/
      //route("timepass", neighbourId, self.path.name, true, true, 0, false)
      /*Commented by Anirudh Subramanian for testing End*/
    }

    senderBoss ! sum
  }

  def updateTables(hopNo: Int, rTable: Array[String], lsMinus: ArrayBuffer[String], lsPlus: ArrayBuffer[String], finalNode: Boolean): Unit = {
      if(finalNode) {
        leafSetMinus ++= lsMinus
        leafSetPlus ++= lsPlus
        println("Node setup done !")
      }
      if(hopNo > 0) {
        rTable.copyToArray(routingTable(hopNo))
      }
  }

  def route(msg: String, neighbourNodeId: String, senderNodeId: String, join: Boolean, newNode: Boolean, hopNumber: Int, lastNode: Boolean): Unit = {
       var currentNodeName: String = self.path.name
       println("currentNodeName is " + currentNodeName )

       if(lastNode) {
         var updateHopsLast = hopNumber + 1
         var senderNode    = context.actorSelection(senderNodeId)
         senderNode ! updateTables(updateHopsLast - 1, routingTable(updateHopsLast - 1), leafSetMinus, leafSetPlus, true)
       }

       if(join) {
          if(newNode) {
            val neighbouringActor = context.actorSelection(neighbourNodeId)
            print("neighbouringActor is " + neighbouringActor)
            neighbouringActor ! route(msg, "",senderNodeId, join, false, hopNumber, false)
          }
          else {
            var updatedHopNumber = hopNumber + 1
            var findRoute:(BigInt, Boolean) = searchInTables(senderNodeId, currentNodeName)
            //Leafset true
            if(findRoute._2) {
              /*If not null route to that node with minimum */
              if(findRoute._1 != null) {
                var nextInRouteId = findRoute._1.toString(16)
                var nextInRoute   = context.actorSelection(nextInRouteId)
                var senderNode    = context.actorSelection(senderNodeId)
                senderNode ! updateTables(updatedHopNumber - 1, routingTable(updatedHopNumber - 1), leafSetMinus, leafSetPlus, false)
                nextInRoute ! route(msg, neighbourNodeId, senderNodeId, join, false, updatedHopNumber, true)
              } else {
                var senderNode    = context.actorSelection(senderNodeId)
                senderNode ! updateTables(updatedHopNumber - 1, routingTable(updatedHopNumber - 1), leafSetMinus, leafSetPlus, true)
                println("Nearest key " + currentNodeName)
              }
              /*If null then print the hopping ends here*/
              println("")
            } else {
              if (findRoute._1 != null) {
                var nextInRouteId = findRoute._1.toString(16)
                var nextInRoute = context.actorSelection(nextInRouteId)
                var senderNode = context.actorSelection(senderNodeId)
                senderNode ! updateTables(updatedHopNumber - 1, routingTable(updatedHopNumber - 1), leafSetMinus, leafSetPlus, false)
                nextInRoute ! route(msg, neighbourNodeId, senderNodeId, join, false, updatedHopNumber, false)
              } else {
                var senderNode    = context.actorSelection(senderNodeId)
                senderNode ! updateTables(updatedHopNumber - 1, routingTable(updatedHopNumber - 1), leafSetMinus, leafSetPlus, true)
                println("Nearest key " + currentNodeName)
              }

            }
          }

        }

  }

  def searchInTables(senderNodeId: String, currentNodeName: String): (BigInt, Boolean) = {
   var searchInMinLeaf: Boolean = false
   var searchInMaxLeaf: Boolean = false
   var min: BigInt = Long.MaxValue
   var max: BigInt = Long.MinValue
   var numericallyClosest: BigInt = null
   var inLeaf = false
   if (senderNodeId == null)
    return (null, false)
   var key: BigInt = BigInt.apply(senderNodeId, 16)
    if(leafSetMinus.size > 0) {
      min = BigInt.apply(leafSetMinus(leafMinusIndex), 16)
      searchInMinLeaf = true
    }

    if(leafSetPlus.size > 0) {
      max = BigInt.apply(leafSetPlus(leafPlusIndex), 16)
      searchInMaxLeaf = true
    }

    if(searchInMinLeaf && searchInMaxLeaf) {
      if (min <= key && key <= max) {
        numericallyClosest = findMinimumLeafSet(key)
        if(numericallyClosest != null) {
          return (numericallyClosest, true)
        }
      }
    }
    //Longest Common Prefix size
    var length:Int =  senderNodeId.zip(currentNodeName).takeWhile(Function.tupled(_ == _)).map(_._1).mkString.size
    var column:BigInt = BigInt.apply(senderNodeId.charAt(length).toString(), 16)
    if(routingTable(length)(column.intValue()) != null) {
      numericallyClosest = BigInt.apply(routingTable(length)(column.intValue()), 16)
    }

    return(numericallyClosest, false)
  }

   private def findMinimumLeafSet(key: BigInt): BigInt = {
      var rightMostOfLeft: Int = leafSetMinus.size - 1
      var leftMostOfRight: Int = 0
      var maxLeft = BigInt.apply(leafSetMinus(rightMostOfLeft), 16)
      var minRight = BigInt.apply(leafSetPlus(leftMostOfRight), 16)
      var output: BigInt = null
      println("maxLeft is " + maxLeft)
      println("minRight is " + minRight)
      if(maxLeft <= key && key <= minRight) {
        println("Inside key greater than maxLeft and less than minRight")
        var diff: BigInt = key - maxLeft
        var diff2: BigInt = key - minRight
        if(key == maxLeft)
          output = maxLeft
        if(key == minRight)
          output = minRight
        if(output == null) {
          if (diff.abs < diff2.abs) {
            output = maxLeft
          } else {
            output = minRight
          }
        }
      }
      if(key < maxLeft) {
        println("Inside maxLeft")
        output = findNearestNumericalLeaf(leafSetMinus,key)
        println("output is " + output)
      }
      if(key > minRight) {
        //TODO compare between two smallest elements
        println("Inside minRight")
        output = findNearestNumericalLeaf(leafSetPlus, key)
      }

      output
   }

   private def findNearestNumericalLeaf(leafSet: ArrayBuffer[String],key: BigInt): BigInt = {
     println("Inside findNearestNumericalLeaf")
     var leafSetSize: Int = leafSet.size - 1
     println("leafSetSize is " + leafSetSize)
     var output: BigInt = null
     if(leafSetSize < 0) return output
     if(leafSetSize == 0 || leafSetSize == 1) return BigInt.apply(leafSet(leafSetSize), 16)
     for(i <- 0 to leafSetSize) {
        println("Inside for")
        if(BigInt.apply(leafSet(i), 16) >= key ) {
          println("Inside if of leafSet")
          if(i == 0)
            return BigInt.apply(leafSet(i), 16)
          var diff1: BigInt = key - BigInt.apply(leafSet(i), 16)
          var diff2: BigInt = key - BigInt.apply(leafSet(i - 1), 16)
          if(diff1.abs  < diff2.abs) {
            output = BigInt.apply(leafSet(i), 16)
          } else {
            output = BigInt.apply(leafSet(i - 1), 16)
          }
          return output
        }

     }
     return output
   }

 /* private def join(actr: ActorRef, newActr: ActorRef) {
    if (actr == newActr) {
      // actor already exists
      // forward message to same
    }
    else if((newActr >= leafSetMinus(leafSetMinus.length-1)) && newActr < actr) {
      var i = 0
      var min = 0
      var minActr
      for (i <- 0 to leafSetMinus.length-1) {
        // find minimum difference key
        if(min! = 0 && (abs(leafSetMinus(i)-newActr) < min ) {
           min = abs(leafSetMinus(i)-newActr)
           minActr = leafSetMinus(i)
        }
        if (minActr != null) {
          minActr ! "ROUTE" 
        }    
          
      }
    }
      
    
  }*/
  
  
  // DEBUG: For testing byteArrayCompare. Tested OK
   
  def testCompare()
  {
    //var a:Array[Byte] = Array(15,0,3,4)
    //var b:Array[Byte] = Array(4,92,0,4)
    /*
    var result:Array[Byte] = byteArrayAbsDiff(a,b)
    var j=0
    for(j<-0 to result.length-1) {
    println("Result "+ a(j)+"-"+b(j)+"="+result(j))
    }
    */
  }
  
 def byteArrayDiff(a: Array[Byte], b: Array[Byte]) : Array[Byte] = {
    var ret:Array[Byte] = Array.fill(a.length)(0)
    
    var retTrunc:Array[Byte] = null
    var temp : Array[Byte] = null
    var temp1 : Int = 0
    
    if ((a == null) || (b == null) || (a.length != b.length)) {
      return ret
    }
      
    temp = a.clone
    var i = 0
    var j = 0
    
          for(j <- a.length-1 to 0 by -1) {
            
    	    if (temp(j) < b(j)) {
    	      println("in if. j= "+j)
    	      if(temp(j-1) != 0) {
    	      temp1 = a(j-1)
    	      temp1 -= 1
    	      temp(j-1) = temp1.toByte
    	      temp(j) = (temp(j) + 10).toByte
    	    }
    	      else {
    	        var k = j-1
    	       
    	        while( temp(k)== 0) {
    	          // keep going until you find non-zero value
    	          k -= 1
    	        } 
    	        temp(k) = (temp(k)-1).toByte
    	        k+=1
    	        while(k != j) {
    	          temp(k) = (temp(k) + 9).toByte
    	          k+=1
    	        }
    	        temp(j) = (temp(j) + 10).toByte
    	    }
    	      ret(j) = (temp(j)-b(j)).toByte
    	    }
    	      else {
    	         println("in else. j= "+j)
    	         println("in else. temp(j)= "+temp(j))
    	         println("in else. ret(j)= "+ret(j))
    	      ret(j) = (temp(j)-b(j)).toByte
    	      println("in else. ret(j)= "+ret(j))
           	}
          }
    /*
    // Truncate ret
    	 j = ret.length-1
    	 var k1 = 0
    	 var k2 = 0
    	  if(ret(j) == 0) {
    	  while (ret(j) == 0) {
    	    j -= 1
    	  }
    	  for(k1 <- j to ret.length-1) {
    	    retTrunc(k2) = ret(k1)
    	    k2 += 1
    	  }
    	  } 
    	  return retTrunc
    	  */  
    	return ret
 }
 
  
  
 def byteArrayAbsDiff(a: Array[Byte], b: Array[Byte]) : Array[Byte] = {
  
    val temp : Array[Byte] = Array(0.toByte)
    println("byteArrayCompare(a, b, 0)"+byteArrayCompare(a, b, 0))
    
    if ((a == null) || (b == null) || (a.length != b.length)) {
        return null
    }
      
       if(byteArrayCompare(a, b, 0) == 1) {
    	  // then a > b
         return byteArrayDiff(a,b)
       }     
      else if (byteArrayCompare(a, b, 0) == -1) {
      // then b > a
        return byteArrayDiff(b,a)
      }
      else {
     // then a=b
      return temp
      }
    
    }
     
 
 def byteArrayCompare(a: Array[Byte], b: Array[Byte], index : Int) : Int = {
    var ret:Int = 255
    if ((a == null) || (b == null) || (a.length != b.length)) {
      return ret
    }
      
    var i = 0
    for(i <- index to (a.length)-1) {
     if(a(i) == b(i)) {
    	 if (index != a.length-1) {
    	    return byteArrayCompare(a, b, i+1)
    	 }
    	 else {
    	   return 0
    	 } 
     }
     else if(a(i) > b(i)) {
       var j = 0
       return 1
     }
     else if (a(i) < b(i)) {
       return -1
     }       
    }
    return ret
   }
  
  def byteCompare(a: Byte, b : Byte) : Int = {
    val ret = 255
    if (a == null || b == null) {
      return ret
    }
    else if (a == b) {
      return 0
    }
    else if (a > b) {
      return 1
    }
    else if (b > a) {
      return -1
    }
    return ret
  }
  
 }  