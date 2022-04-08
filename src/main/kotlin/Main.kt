import com.azure.core.util.Context
import com.azure.storage.queue.QueueClient
import com.azure.storage.queue.QueueClientBuilder
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.stream.IntStream

val  client: QueueClient = QueueClientBuilder()
//        To test add connection string here
    .connectionString("")
    .queueName("swan")
    .buildClient()

fun main(args: Array<String>) {
    IntStream.range(0, 100)
        .parallel()
        .forEach{
            runBlocking {
                launch {
                    producer(it)
                    consumerA()
                    consumerB()
                }
            }
        }
}

suspend fun producer(number: Number){
    randomSleep()
   client.sendMessage("Message : $number")
}

suspend fun consumerA() {
    randomSleep()
    consume("consumerA")
}

suspend fun consumerB() {
    randomSleep()
    consume("consumerB")
}

fun randomSleep() {
    val randomLong = (0..4_000).random().toLong()
    TimeUnit.MILLISECONDS.sleep(randomLong)
}

fun consume(consumer: String) {
    if (client.properties.approximateMessagesCount > 0) {
        val messages = client.receiveMessages(2, Duration.ofSeconds(5), Duration.ofSeconds(2), Context("", ""))
        messages.forEach {
            if (it?.body != null) {
                if ((0..6).random() == 5) {
                    println("$consumer failed to consume : ${it.body}")
                } else {
                    client.deleteMessage(it.messageId, it.popReceipt)
                    println("$consumer consume : ${it.body}")
                }
            }
        }
    }
}
