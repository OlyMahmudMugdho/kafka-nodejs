const express = require('express')
const morgan = require('morgan')
const { Kafka } = require("kafkajs")
const clientId = "mock-up-kafka-consumer-client"
const brokers = ["kafka:29092"]
const topic = "events"
const kafka = new Kafka({ clientId, brokers })
const consumer = kafka.consumer({ groupId: clientId })
const producer = kafka.producer();

const app = express()

app.use(express.json())

app.use(morgan('common'))

const PORT = 3000;

app.get("/", (req, res) => {
    return res.status(200).json({
        "ok": true,
        "message": "hello world"
    })
})



app.get("/produce", (req,res) => {
    const produce = async () => {
        await producer.connect();
        await producer.send({
            topic,
            messages: [
              { key: "key1", value: "hello world", partition: 0 },
              { key: "key2", value: "hey hey!", partition: 1 },
            ],
          })
        }
      
      produce()
        .then(() => {
          console.log("produced successfully");
        })
        .then(() => res.json("produced successfully"))
        .catch((err) => console.log("error occurred"));
    
})

let data = [];

app.get("/hit", (req,res) => {
    const consume = async () => {
        await consumer.connect()
        await consumer.subscribe({ topic })
        await consumer.run({
            eachMessage: ({ message }) => {
                res.json(message.value)
                console.log(`received message: ${message.value}`)
                data.push(message);
            },
        })
        //return data;
    }
    
    consume().then(() => { console.log("produced successfully") }).catch(err => console.log(err));
})

app.get("/consume", async (req, res) => {
    res.json(data[0].value.toString())
})

app.listen(PORT, () => {
    console.log(`server is running on port ${PORT}`)






})