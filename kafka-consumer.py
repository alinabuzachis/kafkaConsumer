from kafka import KafkaConsumer
from kafka import TopicPartition
import time
import json
import configparser

config = configparser.ConfigParser()
config.read('config.conf')

FRAME_FREQUENCY_DELAY_MILLIS = 5000 # 5 seconds expressed in milliseconds 

class Worker(object):
    '''
    The Consumer requires an instance of this class to handle processing raw frames and flushing
    processed batches.
    '''
    def __init__(self):
        self.__processed = {} 
        self.__flushed = []
        self.__shutdown_calls = 0

    def process_frame(self, message, lastUpdated):
        '''
        Called with each frame, allowing the worker to extract the fields of interest (ts and uid). The object returned
        is put into the batch maintained by the Consumer. The Worker maintains a historical dictionary storing the last
        timestamp received from the Consumer and a set of uids.
        '''
        message = json.loads(message.value)
        
        self.__processed['lastUpdated'] = lastUpdated
        if 'uid' not in self.__processed:
            self.__processed['uid'] = set()
            self.__processed['uid'].add(message['uid'])
        else:
            self.__processed['uid'].add(message['uid'])

        return self.__processed, message['ts']*1000, message['uid']

    def flush_batch(self, batch):
        '''
        Called with a list of pre-processed (by process_frame) objects.
        The Worker also maintains a history with the processed bathches.
        '''
        self.__flushed.append(batch)
        self.__processed = {}

    def shutdown(self):
        '''
        Called when the Consumer is shutting down (because it
        was signalled to do so). Provides the Worker a chance to do any final
        cleanup.
        '''
        self.__shutdown_calls += 1


class Consumer(object):
    def __init__(self, worker, max_batch_time):
        self.worker = worker
        self.__metrics = []
        self.max_batch_time = max_batch_time 
        '''
        All the mestrics gathered during the experiment for each batch are stored here (it also includes
        the count of the unique users inside a batch).
        '''        
        self.__batch_results = {} # Contains all the frames in a batch.
        self.__batch_offsets = {}  # (topic, partition) = [low, high]
        self.__batch_deadline = None
        self.__batch_frames_processed_count = 0 # The total number of frames processed in a batch.
        
        '''
        The total amount of time (in milliseconds), that it took to process
        the frames in this batch (it does not included time spent waiting for
        new frames.
        '''
        self.__batch_processing_time_ms = 0.0
        
        self.consumer = self.create_consumer() # The kafka-consumer is created.
        self.shutdown = False
    
    
    def create_consumer(self):
        consumer = KafkaConsumer(
                    bootstrap_servers = config['kafka-consumer']['KAFKA_BOOTSTRAP_SERVERS'],
                    auto_offset_reset = config['kafka-consumer']['AUTO_OFFSET_RESET'],
                    enable_auto_commit = config['kafka-consumer']['ENABLE_AUTO_COMMIT_CONFIG'],
                    group_id = config['kafka-consumer']['KAFKA_GROUP_ID'])

        consumer.assign([TopicPartition(config['kafka-consumer']['KAFKA_RECEIVE_TOPIC'], int(config['kafka-consumer']['KAFKA_TOPIC_PARTITION']))])
        return consumer
    
    def _getCurrentTimeMillis(self):
        '''
        Get the current time expressed in millis.
        '''
        return int(round(time.time() * 1000))

    def _flush(self, force=False):
        '''
        Decides whether the Consumer should flush because of the batch deadline
        has been reached. If so, delegate to the worker, clear the current batch.
        '''

        if not self.__batch_frames_processed_count > 0:
            return  # No messages were processed, so there's nothing to do.

        '''
        If batch_by_time is True (if there exists as batch_deadline and the current time is grater than the batch_deadline), 
        then the batch deadline is reached and the batch must be processed and then flushed. Otherwise, the batch_deadline 
        is not reached and the batch must be conserved. 
        '''
        batch_by_time = self.__batch_deadline and self._getCurrentTimeMillis() > self.__batch_deadline
        
        if not (force or batch_by_time):
            return
        
        batch_results_length = len(self.__batch_results) # Stores the length of the batch.
        
        if batch_results_length > 0: 
            
            '''
            Count the number of unique users present inside a batch every minute.
            '''
            unique_uid = len(self.__current_processed['uid'])
            print("\nNumber of unique users: ", unique_uid)
            '''
            The throughput expressed in frames per second is computed every minute.
            '''
            throughput = self.__batch_frames_processed_count / 60
            
            flush_start = self._getCurrentTimeMillis()
            self.worker.flush_batch(self.__batch_results)
            flush_duration = self._getCurrentTimeMillis() - flush_start #  Takes the time necessary to make the flush.
                        
            self._record_timing('batch.flush', flush_duration)
            self._record_timing('batch.flush.normalized', flush_duration / batch_results_length)
            self._record_timing("batch duration", flush_duration / batch_results_length)

            '''
            Stores the total batch processing time normalized by the number of frames.
            '''
            self._record_timing(
                'process_frame.normalized',
                self.__batch_processing_time_ms / self.__batch_frames_processed_count,
            )
            self._record_timing("batch.throughput", throughput)
            self._record_timing("batch.unique.users", unique_uid)

        self._reset_batch()

    def _reset_batch(self):
        '''
        Reset the batch.
        '''
        self.__batch_results = {}
        self.__batch_offsets = {}
        self.__current_processed = {}
        self.__batch_deadline = None
        self.__batch_frames_processed_count = 0
        self.__batch_processing_time_ms = 0.0
    
    def _handle_frame(self, msg, topic_partition):
        start = self._getCurrentTimeMillis()

        '''
        Set the deadline only after the first message for this batch is seen.
        '''
        if not self.__batch_deadline:
            self.__batch_deadline = self.max_batch_time + start + FRAME_FREQUENCY_DELAY_MILLIS
            
        '''
        Each received frame is processed by the worker in order to extract "ts" and "uid". The worker also maintains
        a historical dictionary containing the timestamp at which the last change has been applied and a set containing
        the uids (this can be then retrieved inside __current_processed).   
        '''
        result, ts, uid = self.worker.process_frame(msg, start)
        self.__current_processed = result
        
        '''
        For each frame, in order to manage the fact more users can arrive at the same time, there is maintained a dictionary
        __batch_results that contains as key the "ts" expressed in milliseconds as a set containing the uid of all the users
        arrived at the same time.
        '''
        if result is not None:            
            if ts not in self.__batch_results:
                self.__batch_results[ts] = {uid} 
            else:
                self.__batch_results[ts].add(uid)
                        
            duration = self._getCurrentTimeMillis() - start

            self.__batch_frames_processed_count += 1
            '''
            __batch_processing_time_ms is used to get the time necessary to process an entire batch. Is is the sum of the 
            individual processed frames.
            '''
            self.__batch_processing_time_ms += duration
            self._record_timing('process_frame', duration)

        topic_partition_key = (topic_partition.topic, topic_partition.partition)

        if topic_partition_key in self.__batch_offsets:
            self.__batch_offsets[topic_partition_key][1] = msg.offset
        else:
            self.__batch_offsets[topic_partition_key] = [msg.offset, msg.offset]
    
    
    def run(self):
        '''
        The main run loop.
        '''       
        while not self.shutdown:
            self._run_once()

        self._shutdown()


    def _run_once(self):
        self._flush()

        frames = self.consumer.poll(timeout_ms=int(config['kafka']['KAFKA_CONSUMER_TIMEOUT_MILLIS']))
        for topic_partition, frame in frames.items():
            self._handle_frame(frame[0], topic_partition)
    

    def _record_timing(self, metric, value, tags=None):

        return self.__metrics.append({
            'metric': metric,
            'value' : value,
            'tags': tags
        }
        )
    
    def signal_shutdown(self):
        '''Tells the Consumer to shutdown on the next run loop iteration.
        Typically called from a signal handler.
        '''
        self.shutdown = True

      
    def _shutdown(self):
        self._reset_batch()
        '''
        Tells the Consumer to shutdown, and close the Consumer.
        '''
        self.worker.shutdown()
        self.consumer.close()
