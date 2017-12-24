from mock import patch
from pytest import fixture
from time import sleep
from os import environ
from canary import Canary, AwsConfigException

TEST_KEY = "AKIAI2YFWQBRNUJLEPZQ"
TEST_SECRET = "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"

TEST_BUCKET = 'objectcanary'
TEST_FILENAME = "testFile_{}".format(environ.get('HOSTNAME', 'unknown'))



class BucketType:
    """Mocking the S3 Bucket object"""
    def upload_file(self, x, y):
        sleep(1)

    def download_file(self, x, y):
        sleep(2)

class S3:
    def Bucket(self,x):
        return BucketType()


@fixture(scope='module')
def env():
    # this logic runs before the testing start

    old_key = environ.get('AWS_ACCESS_KEY_ID',None)
    old_secret = environ.get('AWS_SECRET_ACCESS_KEY', None)
    yield "env"   # env is the argument, needed to be passed to test that need the setup / teardown
    # this logic runs after the testing finish
    if old_key is not None:
        environ['AWS_ACCESS_KEY_ID'] = old_key
    if old_secret is not None:
        environ['AWS_SECRET_ACCESS_KEY'] = old_secret

# in python 2.7, using os.putenv() does not change the underlining
# os.environ, so we go directly through the environ


def configure_for_process_test():
    environ['AWS_ACCESS_KEY_ID'] = TEST_KEY
    environ['AWS_SECRET_ACCESS_KEY'] = TEST_SECRET


def test_now_no_env_key(env):
    try:
        Canary()
    except AwsConfigException as e:
        assert e.message == 'AWS_ACCESS_KEY_ID not defined'
    else:
        assert 0


def test_now_no_env_secret(env):
    try:
        environ['AWS_ACCESS_KEY_ID'] = TEST_KEY
        Canary()
    except AwsConfigException as e:
        assert e.message == 'AWS_SECRET_ACCESS_KEY not defined'
    else:
        assert 0


@patch(target='boto3.resource', return_value=S3())
def test_loading_a_file(env):
    configure_for_process_test()
    c = Canary()
    time_taken = c.send_file('transfer_file', TEST_BUCKET, TEST_FILENAME)
    assert time_taken > 0.0


@patch(target='boto3.resource', return_value=S3())
def test_reading_a_file(env):
    configure_for_process_test()
    c = Canary()
    time_taken = c.get_file('transfer_file.bk', TEST_BUCKET, TEST_FILENAME)
    assert time_taken > 0.0


@patch(target='boto3.resource', return_value=S3())
def test_reading_round_trip(env):
    configure_for_process_test()
    c = Canary()
    time_taken = c.round_trip('transfer_file', TEST_BUCKET, TEST_FILENAME)
    assert time_taken > 0.0