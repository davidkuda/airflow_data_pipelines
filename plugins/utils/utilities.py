import os
from configparser import ConfigParser


def get_aws_creds():
    home_dir = os.environ.get('HOME')
    config = ConfigParser()
    config.read(f'{home_dir}/.aws/credentials')
    aws_access_key_id = config['udacity']['aws_access_key_id']
    aws_secret_access_key = config['udacity']['aws_secret_access_key']
    aws_creds = { "aws_access_key_id": aws_access_key_id,
                  "aws_secret_access_key": aws_secret_access_key }
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
