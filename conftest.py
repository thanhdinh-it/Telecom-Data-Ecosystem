# conftest.py — pytest adds project root to sys.path
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "spark_jobs"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "data_quality"))
