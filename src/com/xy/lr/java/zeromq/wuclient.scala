package com.xy.lr.java.zeromq

import org.zeromq.ZMQ

/**
 * Created by xylr on 15-4-7.
 */
object wuclient {

  def main(args : Array[String]) {
    val context = ZMQ.context(1)

    //  Socket to talk to server
    println("Collecting updates from weather server…")
    val subscriber = context.socket(ZMQ.SUB)
    subscriber.connect("tcp://localhost:5556")

    //  Subscribe to zipcode, default is NYC, 10001
    val filter = {if (args.length > 0)  args(0) else "10001 "}
    subscriber.subscribe(filter.getBytes())

    //  Process 100 updates
    val update_nbr = 100
    var total_temp = 0
    for (i <- 1 to update_nbr ) {
      //  Use trim to remove the tailing '0' character
      val sscanf = new String(subscriber.recv(0)).trim.split(' ').map(_.toInt)
      val zipcode = sscanf(0)
      val temperature = sscanf(1)
      val relhumidity = sscanf(2)
      total_temp += temperature
    }
    println("Average temperature for zipcode '" + filter + "' was " +  (total_temp / update_nbr))
  }
}
