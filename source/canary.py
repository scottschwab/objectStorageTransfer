import boto3
import logging
import os
import timeit


class AwsConfigException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class Canary(object):

    def __init__(self):
        boto3.set_stream_logger("boto3", logging.WARN)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        if os.getenv('AWS_ACCESS_KEY_ID') is None:
            raise AwsConfigException('AWS_ACCESS_KEY_ID not defined')
        if os.getenv('AWS_SECRET_ACCESS_KEY') is None:
            raise AwsConfigException('AWS_SECRET_ACCESS_KEY not defined')

    def send_file(self, source, bucket_name, file_key):
        self.logger.info("starting send transfer {}".format(source))
        start = timeit.timeit()
        s3 = boto3.resource('s3')
        s3.Bucket(bucket_name).upload_file(source, file_key)
        finish = timeit.timeit()
        self.logger.info("Time for send {} seconds".format(finish-start))
        return finish - start

    def get_file(self, dest, bucket_name, file_key):
        self.logger.info("starting send transfer {}".format(dest))
        start = timeit.timeit()
        s3 = boto3.resource('s3')
        s3.Bucket(bucket_name).download_file(file_key, dest)
        finish = timeit.timeit()
        self.logger.info("Time to get {} seconds".format(finish-start))
        return finish - start

    def round_trip(self, filename, bucket_name, file_key):
        up = self.send_file(filename, bucket_name, file_key)
        down = self.get_file(filename + ".bak", bucket_name, file_key)
        total_time = up + down
        self.logger.info("total time in seconds {}".format(total_time))
        return total_time


if __name__ == '__main__':
    c = Canary()
    c.round_trip('transfer_file', 'objectcanary', 'foo.bin')

