import boto3
import botocore
import argparse
import logging
import os
import timeit


class AwsConfigException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class Canary(object):
    """
    This is a class built to allow for an object to be created in object storage
    and then retrieve, returning the time the process takes.
    """
    def __init__(self):
        boto3.set_stream_logger("boto3", logging.WARN)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        if os.getenv('AWS_ACCESS_KEY_ID') is None:
            raise AwsConfigException('AWS_ACCESS_KEY_ID not defined')
        if os.getenv('AWS_SECRET_ACCESS_KEY') is None:
            raise AwsConfigException('AWS_SECRET_ACCESS_KEY not defined')

    def send_file(self, source, bucket_name, file_key):
        """
        Pull a file form the object store and save it in a local file
        :param str source: The local file read from
        :param str bucket_name: The bucket holding the object to transfer
        :param str file_key: object's name in the bucket
        """
        self.logger.info("starting send transfer {}".format(source))
        start = timeit.timeit()
        s3 = boto3.resource('s3')
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError:
            s3.create_bucket(Bucket=bucket_name)

        s3.Bucket(bucket_name).upload_file(source, file_key)
        finish = timeit.timeit()
        self.logger.info("Time for send {} seconds".format(finish-start))
        return finish - start

    def get_file(self, dest, bucket_name, file_key):
        """
        Pull a file form the object store and save it in a local file
        :param str dest: The local file to write into
        :param str bucket_name: The bucket holding the object to transfer
        :param str file_key: object's name in the bucket
        """
        self.logger.info("starting send transfer {}".format(dest))
        start = timeit.timeit()
        s3 = boto3.resource('s3')
        s3.Bucket(bucket_name).download_file(file_key, dest)
        finish = timeit.timeit()
        self.logger.info("Time to get {} seconds".format(finish-start))
        return finish - start

    def round_trip(self, filename, bucket_name, file_key):
        """
        Make a call to both send and receive a file
        :param str filename: file to test with
        :param str bucket_name: where to store the object in
        :param str file_key: name to write the object into, in the bucket"""
        up = self.send_file(filename, bucket_name, file_key)
        down = self.get_file(filename + ".bak", bucket_name, file_key)
        total_time = up + down
        self.logger.info("total time in seconds {}".format(total_time))
        return total_time


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="This sends and retrieves and object to and from object storage")
    parser.add_argument('transfer_file', action='store')
    parser.add_argument('bucket_name', action='store')
    parser.add_argument('stored_name', action='store')
    arguments = parser.parse_args()

    c = Canary()
    c.round_trip(arguments.transfer_file, arguments.bucket_name, arguments.stored_name)


