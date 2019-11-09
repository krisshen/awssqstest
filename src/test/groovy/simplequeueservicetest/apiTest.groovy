package simplequeueservicetest

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model.CreateQueueRequest
import com.amazonaws.services.sqs.model.Message
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import com.amazonaws.services.sqs.model.SendMessageBatchRequest
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry
import groovy.util.logging.Slf4j
import org.elasticmq.rest.sqs.SQSRestServerBuilder
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test

/**
 * this test class contains sample test cases for testing both AWS SQS server and a local Elastic MQ server
 * by using a local SQS Client (provided by AWS SDK).
 */
@Slf4j
class apiTest {

    static queueType = "elasticmq" // from gradle command line, default to elasticmq, can also be aws

    static elasticmqServer
    static endpoint = "http://localhost:9324"
    static region = "elasticmq"
    static accessKey = "x"
    static secretKey = "x"

    static AmazonSQS client

    static clientId = ""

    @BeforeAll
    static void beforeAll() {
        log.info "before all..."
        if (System.getProperty("queueType")) {
            queueType = System.getProperty("queueType").toLowerCase()
        }

        if (queueType == 'aws') {
            log.info "initializing AWS SQS client..."
            endpoint = "https://sqs.ap-southeast-2.amazonaws.com"
            clientId = "427604497617"
            client = AmazonSQSClientBuilder.defaultClient()
        } else if (queueType == 'elasticmq') {
            log.info "initialize Elastic MQ server and AWS SQS client..."
            elasticmqServer = SQSRestServerBuilder.start()
            endpoint = "http://localhost:9324"
            clientId = "queue"
            client = AmazonSQSClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                    .build()
        } else {
            def errorMessage = "invalid queue type provided: $queueType"
            log.info "errorMessage"
            Assertions.fail(errorMessage)
        }
    }

    @AfterAll
    static void afterAll() {
        log.info "after all...cleaning up..."
        if (elasticmqServer) {
            elasticmqServer.stopAndWait()
        }
    }

    @AfterEach
    void afterEach() {
        log.info "after each...cleaning up..."
        client.listQueues().getQueueUrls().each {
            log.info "deleting queue: $it"
            client.deleteQueue(it)
        }
        waitForQueuesToBeDeleted(client)
    }

    @Test
    void 'create queue and get queue'() {

        def queueName = "MyQueue" + new Date().getTime()

        assert "$endpoint/$clientId/$queueName" == client.createQueue(queueName).getQueueUrl()
    }

    @Test
    void 'list queues'() {

        def queueName1 = "MyQueue1" + new Date().getTime()
        def queueName2 = "MyQueue2" + new Date().getTime()
        def queueName3 = "MyQueue3" + new Date().getTime()

        def queues = [queueName1, queueName2, queueName3]

        queues.each {
            log.info "creating queue: " + client.createQueue(it).getQueueUrl()
        }

        waitForQueuesToBeCreated(client, queues)

        def actualQueueUrls = client.listQueues().getQueueUrls()
        assert actualQueueUrls.size() == queues.size()

        def expectedQueueUrl
        queues.each {
            expectedQueueUrl = "$endpoint/$clientId/$it"
            assert actualQueueUrls.toString().contains(expectedQueueUrl)
        }
    }

    @Test
    void 'delete queue'() {

        def queueName1 = "MyQueue1" + new Date().getTime()
        def queueName2 = "MyQueue2" + new Date().getTime()
        def queueName3 = "MyQueue3" + new Date().getTime()

        def queues = [queueName1, queueName2, queueName3]

        queues.each {
            log.info "creating queue: " + client.createQueue(it).getQueueUrl()
        }
        waitForQueuesToBeCreated(client, queues)

        def queueName2Url = "$endpoint/$clientId/$queueName2"
        log.info "deleting queue: $queueName2Url"
        client.deleteQueue(queueName2Url)
        queues.remove(queueName2)
        waitForQueuesToBeDeleted(client, queues)

        assert !client.listQueues().getQueueUrls().toString().contains(queueName2Url)
    }

    @Test
    void 'set and get queue attributes'() {

        def queueName = "MyQueue" + new Date().getTime()
        def queueUrl = "$endpoint/$clientId/$queueName"

        client.createQueue(queueName)
        waitForQueuesToBeCreated(client, [queueName])

        def attributes = [
                'DelaySeconds'      : '10',
                'MaximumMessageSize': '1024' // this property is not supported by Elastic MQ
        ]

        client.setQueueAttributes(queueUrl, attributes)

        assert client.getQueueAttributes(queueUrl, ['DelaySeconds']).getAttributes().get('DelaySeconds') == '10'

        if (queueType == "aws") {
            assert client.getQueueAttributes(queueUrl, ['MaximumMessageSize']).getAttributes().get('MaximumMessageSize') == '1024'
        }
    }

    @Test
    void 'send and receive one message'() {

        def queueName = "MyQueue" + new Date().getTime()
        def queueUrl = "$endpoint/$clientId/$queueName"

        client.createQueue(queueName)
        waitForQueuesToBeCreated(client, [queueName])

        def testMessage = "test message"
        client.sendMessage(queueUrl, testMessage)

        Message message = client.receiveMessage(queueUrl).getMessages()[0]
        assert message.getBody() == testMessage
    }

    @Test
    void 'send and receive multiple messages'() {

        def queueName = "MyQueue" + new Date().getTime()
        def queueUrl = "$endpoint/$clientId/$queueName"

        CreateQueueRequest createQueueRequest = new CreateQueueRequest()
                .withQueueName(queueName)
                .addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20") // long polling queue
        client.createQueue(createQueueRequest)
        waitForQueuesToBeCreated(client, [queueName])

        def testMessage = "test message"

        def entries = []
        def messageCount = 5
        def expectedMessageCount = messageCount
        while (messageCount) {
            entries << new SendMessageBatchRequestEntry("messageId_${messageCount.toString()}", testMessage)
            messageCount -= 1
        }

        SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest(queueUrl, entries)
        client.sendMessageBatch(sendMessageBatchRequest)

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(10) // receive up to 10 messages a time
                .withWaitTimeSeconds(3) // long polling request

        def loop = 10
        List<Message> allMessages = client.receiveMessage(receiveMessageRequest).getMessages()
        def messages
        while (loop) {
            messages = client.receiveMessage(receiveMessageRequest).getMessages()
            if (messages.size() == 0) {
                log.info "all messages have been received"
                break
            }
            allMessages += messages
            log.info "let's see if there's more to receive..."
            Thread.sleep(1 * 1000)
            loop -= 1
        }

        log.info "all messages size: ${allMessages.size()}"
        assert expectedMessageCount == allMessages.size()
    }

    @Test
    void 'delete message'() {

        def queueName = "MyQueue" + new Date().getTime()
        def queueUrl = "$endpoint/$clientId/$queueName"

        client.createQueue(queueName)
        waitForQueuesToBeCreated(client, [queueName])

        def testMessage = "test message"
        client.sendMessage(queueUrl, testMessage)

        List<Message> messages = client.receiveMessage(queueUrl).getMessages()
        assert messages.size() == 1

        if (queueType == "aws") { // Elastic MQ automatically removes messages after receiveMessage()
            client.deleteMessage(queueUrl, messages[0].getReceiptHandle())
        }

        messages = client.receiveMessage(queueUrl).getMessages()
        assert messages.size() == 0
    }

    def waitForQueuesToBeCreated(AmazonSQS client, ArrayList<String> queues) {
        def retryTimes = 6
        def interval = 10 // seconds
        def createdQueueSize

        while (retryTimes) {
            createdQueueSize = client.listQueues().getQueueUrls().size()
            if (createdQueueSize == queues.size()) {
                log.info "all queues created..."
                break
            }
            log.info "not all queues are created yet, wait for $interval seconds..."
            Thread.sleep(interval * 1000)
            retryTimes -= 1
        }
    }

    def waitForQueuesToBeDeleted(AmazonSQS client, ArrayList<String> queues = []) {
        def retryTimes = 6
        def interval = 10 // seconds

        while (retryTimes) {
            if (client.listQueues().getQueueUrls().size() == queues.size()) {
                if (queues.size() == 0) {
                    log.info "all queues deleted..."
                } else {
                    log.info "queue(s) deleted..."
                }
                break
            }
            log.info "queues deletion not done yet, wait for $interval seconds..."
            Thread.sleep(interval * 1000)
            retryTimes -= 1
        }
    }

}
