#!/usr/bin/env python
# coding: utf-8

from dotenv import load_dotenv
import os


load_dotenv()
port = int(float(os.getenv('POSTGRES_PORT')))
print(port)
