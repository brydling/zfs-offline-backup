#!/usr/bin/python3

import argparse
import os

import common

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument(        "-c", "--config-file",          help="Config file (default=backup-config.json).", default=os.path.dirname(os.path.realpath(__file__)) + "/backup-config.json")
    
    args=parser.parse_args()

    # settings
    common.read_settings(args.config_file)
    settings = common.get_settings()
    name = settings["approve-method-mail-settings"]["sender-name"]
    
    print(name)
