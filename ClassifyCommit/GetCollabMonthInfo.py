# -*- coding: utf-8 -*-
"""
Created on Fri Jan 31 15:11:32 2020

@author: pmedappa

New code to find the collaborators for each project and date of their first contribution.
Using the merge events sheet C:\Users\pmedappa\Dropbox\HEC\Project 5 - Roles and Coordination\Data\MergeEvents 
"""


import pandas as pd
import numpy as np
import ast


COMMIT2_XLSX ="C:/Data/092019 CommitInfo/uptestRepoCommit1_287_1.xlsx"
CLEAN_XLSX = "C:/Data/092019 CommitInfo/CleanRepoCommit1_287_1.xlsx"
TEST_XLSX = "C:/Data/092019 CommitInfo/Test.xlsx"
MC_XLSX = "C:/Data/092019 CommitInfo/MC_RepoCommit1_287_1.xlsx"