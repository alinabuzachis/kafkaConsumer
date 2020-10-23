from kafka-consumer import Worker, Consumer

MAX_BATCH_TIME_MILLIS = 60000


if __name__ == "__main__":
    Consumer(Worker(), MAX_BATCH_TIME_MILLIS).run()
