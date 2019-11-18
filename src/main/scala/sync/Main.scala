package sync

object Main extends App {
  println("Starting event sync")

  def run(): Unit = {
    val filepath = args.headOption.getOrElse(throw new Exception("Missing source filepath"))

    val job = new Job()

    job.execute(filepath)
  }

  run()
}

