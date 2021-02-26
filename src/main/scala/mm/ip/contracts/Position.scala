package mm.ip.contracts

trait Position { val value: Long }
case class Start(value: Long) extends Position
case class End(value: Long) extends Position
