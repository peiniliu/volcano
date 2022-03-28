"""
hpcc benchmarking tool
"""
import os
import sys
from scipy.stats import poisson

ary = poisson.rvs(mu=3, size=10)

print(ary)