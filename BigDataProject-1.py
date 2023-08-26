from pykafka import KafkaClient
import threading
import pandas as pd
from kafka import KafkaConsumer
import ClearDataFrameCodes
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, CountVectorizer, IDF
from pyspark.ml.classification import NaiveBayes, RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Create a Kafka consumer instance

KAFKA_HOST = "localhost:9092" # Or the address you want

client = KafkaClient(hosts = KAFKA_HOST)
topic = client.topics["secim"]


parse_char = '¬'

label_names = ['created_at', 'username', 'follower_count', 'verified', 'tweet_date', 'description', 'location', 'retweet_count', 'tweet', 'label']

df = None
kafka_df = pd.DataFrame(columns = label_names)


def WriteDataToKafka():
    with topic.get_sync_producer() as producer:
        for i in range(df['tweet'].count()):
            message = ""
            for key in label_names:
                message = message + str(df[key][i]) + "¬"
            message = message[:-1] 
            encoded_message = message.encode("utf-8")
            producer.produce(encoded_message)

def ReadDataFromKafka():
    
    consumer = KafkaConsumer(
        'secim',  # Name of the Kafka topic to consume from
        bootstrap_servers='localhost:9092',  # Kafka broker addresses
        auto_offset_reset='earliest',  # Offset reset strategy
        enable_auto_commit=True,  # Enable automatic offset commit
        consumer_timeout_ms=3000
    )
    
    for message in consumer:
        #Decode and print the message value
        if not consumer:
            break
        ParseKafkaMessage(message.value.decode())
    



def ReadCSV(name : str):
    datas = pd.read_csv(name)
    # Create the pandas DataFrame
    global df
    df = pd.DataFrame(datas, columns = label_names)

def ParseKafkaMessage(message:str):
    global kafka_df
    parts = message.split(parse_char)
    dict_kafka_message = {col: val for col, val in zip(kafka_df.columns, parts)}
    new_row_df = pd.DataFrame([dict_kafka_message])
    kafka_df = pd.concat([kafka_df, new_row_df], ignore_index=True)

def RemoveDataFrameNaN():
    global kafka_df
    kafka_df = kafka_df[kafka_df["label"] != "nan"]
    kafka_df = kafka_df.drop_duplicates(subset=["tweet"])
    kafka_df['label'] = kafka_df['label'].astype(str)
    for index, row in kafka_df.iterrows():
        if (row['label'] != "0" and row['label'] != "1") and (row['label'] != "1.0" and row['label'] != "0.0"):
            #verilerde problem var ondan kaynakli bir if
            kafka_df = kafka_df.drop(index)



def ClearDataFrame():
    global kafka_df
    for index, row in kafka_df.iterrows():
        new_tweet, new_description = ClearDataFrameCodes.CleanLinkandEmojies([row['tweet'], row['description']])
        new_tweet_V2, new_description_V2 = ClearDataFrameCodes.ClearPunctuations([new_tweet, new_description])
        new_tweet_V3 = ClearDataFrameCodes.RemoveStopwords(new_tweet_V2)
        kafka_df['tweet'][index] = new_tweet_V3
        kafka_df['description'][index] = new_description_V2
        if not ClearDataFrameCodes.CheckTweetLanguage('tr', new_tweet_V3):
            kafka_df = kafka_df.drop(index)
        if row['label'] == "0.0":
            kafka_df['label'][index] = "0"
        elif row['label'] == "1.0":
            kafka_df['label'][index] = "1"

def SparkML():
    global kafka_df
    spark = SparkSession.builder.appName("MLlibLanguageProcessingExample").getOrCreate()
    kafka_df['label'] = kafka_df['label'].astype(int)
    spark_df = spark.createDataFrame(kafka_df)
    tokenizer = Tokenizer(inputCol="tweet", outputCol="words")
    cv = CountVectorizer(inputCol="words", outputCol="raw_features")
    idf = IDF(inputCol="raw_features", outputCol="features")
    nb = NaiveBayes()
    rf = RandomForestClassifier()

    pipeline = Pipeline(stages=[tokenizer, cv, idf, rf])

    # Split the data into training and test sets
    trainData, testData = spark_df.randomSplit([0.75, 0.25])

    # Train the model
    model = pipeline.fit(trainData)

    # Make predictions on the test set
    predictions = model.transform(testData)

    # Evaluate the model
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print(accuracy)





    # Stop the SparkSession
    spark.stop()

def PrintHeadOfDF(df:pd.DataFrame):
    print(df.head())

# for i in range(0,11):
#     name = ""
#     name = "secim" + str(i) + ".csv"
#     ReadCSV(name)
#     WriteDataToKafka()

ReadDataFromKafka()
RemoveDataFrameNaN()
ClearDataFrame()
print(len(kafka_df))
SparkML()
PrintHeadOfDF(kafka_df)