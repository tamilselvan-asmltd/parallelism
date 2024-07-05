import logging
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream.functions import ReduceFunction, MapFunction, FlatMapFunction
from pyflink.common.typeinfo import Types  # Import Types
from pyflink.datastream.stream_execution_environment import RuntimeExecutionMode
import threading

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Define functions for sum and average calculation
class SumReduceFunction(ReduceFunction):
    def reduce(self, value1, value2):
        return value1 + value2

class CollectSumFunction(MapFunction):
    def map(self, value):
        print(f"Final Sum: {value}")
        return value

class SumAndCountMapFunction(MapFunction):
    def map(self, value):
        return (value, 1)

class AvgReduceFunction(ReduceFunction):
    def reduce(self, value1, value2):
        total_sum = value1[0] + value2[0]
        count = value1[1] + value2[1]
        return (total_sum, count)

class CollectAvgFunction(MapFunction):
    def map(self, value):
        total_sum, count = value
        avg = total_sum / count
        print(f"Final Average: {avg}")
        return avg

class ParseJsonArrayFunction(FlatMapFunction):
    def flat_map(self, value):
        try:
            numbers = json.loads(value)
            if isinstance(numbers, list):
                for number in numbers:
                    yield number
            else:
                logger.error(f"Expected list but got: {type(numbers)}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON: {e}")

class SourceData:
    def __init__(self, env):
        self.env = env
        jar_path = "file:///Users/tamilselvans/Downloads/flink-sql-connector-kafka-1.17.1.jar"
        self.env.add_jars(jar_path)
        logger.info(f"Added JAR: {jar_path}")
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.set_parallelism(2)  # Set parallelism level
        logger.info("Initialized Flink environment with streaming mode and parallelism 2")

    def get_data(self):
        logger.info("Setting up Kafka source")
        source = KafkaSource.builder() \
            .set_bootstrap_servers("localhost:9092") \
            .set_topics("pwdtopic") \
            .set_group_id("my-group") \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        logger.info("Kafka source set up complete")

        logger.info("Adding Kafka source to the environment")
        stream = self.env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source") \
            .flat_map(ParseJsonArrayFunction(), output_type=Types.INT())
        logger.info("Kafka source added to the environment")

        # Calculate the sum
        sum_stream = stream.key_by(lambda x: 1).reduce(SumReduceFunction()).map(CollectSumFunction())

        # Calculate the average
        sum_and_count_stream = stream.map(SumAndCountMapFunction(), output_type=Types.TUPLE([Types.INT(), Types.INT()]))
        avg_stream = sum_and_count_stream.key_by(lambda x: 1).reduce(AvgReduceFunction()).map(CollectAvgFunction())

        logger.info("Starting Flink job")
        self.env.execute("pyflink_sum_avg_example")
        logger.info("Flink job started")

if __name__ == "__main__":
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        source_data = SourceData(env)
        job_thread = threading.Thread(target=source_data.get_data)
        job_thread.start()
        job_thread.join()
        logger.info("Flink job execution complete")
    except Exception as e:
        logger.error(f"Failed to execute Flink job: {e}")
