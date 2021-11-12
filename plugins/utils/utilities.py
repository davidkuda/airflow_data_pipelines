import os
from configparser import ConfigParser

from airflow.models.variable import Variable


def get_aws_creds(profile: str = 'udacity'):
    home_dir = os.environ.get('HOME')
    config = ConfigParser()
    config.read(f'{home_dir}/.aws/credentials')
    aws_access_key_id = config[profile]['aws_access_key_id']
    aws_secret_access_key = config[profile]['aws_secret_access_key']
    aws_creds = { "aws_access_key_id": aws_access_key_id,
                  "aws_secret_access_key": aws_secret_access_key }
    return aws_creds


def get_aws_creds_from_airflow():
    aws_creds = { "aws_access_key_id": Variable.get('AWS_ACCESS_KEY_ID'),
                  "aws_secret_access_key": Variable.get('AWS_SECRET_ACCESS_KEY') }
    return aws_creds


def parse_config_file(config_file: str = 'plugins/utils/dwh.cfg') -> dict:
    config = ConfigParser()
    config.read_file(open(config_file))
    sections = config.sections()
    configs = {}
    for section in sections:
        for item in config[section].items():
            k = item[0].upper()
            v = item[1]
            configs.update({k: v})
    return configs
