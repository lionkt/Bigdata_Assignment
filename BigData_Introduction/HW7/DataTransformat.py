#!/usr/bin/python
# -*- coding: UTF-8 -*- 
import numpy as np
import scipy as sp

if __name__ == '__main__':
    fold = open('G:\BigData_class\HW7\kddcup.data_10_percent', 'r')
    fnew = open('G:\BigData_class\HW7\kddcup_convert.data_10_percent', 'w')

    seg2_pool = []
    seg3_pool = []
    seg4_pool = []
    seg42_pool = []

    # count lines in file
    LineNum = -1
    for LineNum, content in enumerate(fold): pass   # 这种做法会改变fold
    LineNum += 1
    fold.close()

    fold = open('G:\BigData_class\HW7\kddcup.data_10_percent', 'r')
    countLineNow = 0
    outputThreshold = 0.1
    for line in fold:
        seg_line = line.split(',')
        out_seg = seg_line.copy()
        # process segment 2
        try:
            ix = seg2_pool.index(seg_line[1])
        except:     # 出现异常
            out_seg[1] = str(len(seg2_pool))
            seg2_pool.append(seg_line[1])
        else:       # 没出现异常
            out_seg[1] = str(ix)

        # process segment 3
        try:
            ix = seg3_pool.index(seg_line[2])
        except:  # 出现异常
            out_seg[2] = str(len(seg3_pool))
            seg3_pool.append(seg_line[2])
        else:  # 没出现异常
            out_seg[2] = str(ix)

        # process segment 4
        try:
            ix = seg4_pool.index(seg_line[3])
        except:  # 出现异常
            out_seg[3] = str(len(seg4_pool))
            seg4_pool.append(seg_line[3])
        else:  # 没出现异常
            out_seg[3] = str(ix)

        # process segment 42
        try:
            ix = seg42_pool.index(seg_line[41])
        except:  # 出现异常
            out_seg[41] = str(len(seg42_pool))
            seg42_pool.append(seg_line[41])
        else:  # 没出现异常
            out_seg[41] = str(ix)

        # write files
        out_str = ""
        for i in range(len(out_seg)-1):
            out_str = out_str + out_seg[i] + ','

        out_str = out_str + out_seg[-1] + '\n'
        fnew.write(out_str)
        # check process
        countLineNow += 1
        if countLineNow/LineNum >= outputThreshold:
            print('Finish %.2f' %(countLineNow/LineNum*100) + '% of lines')
            outputThreshold += 0.1

    # close file
    fold.close()
    fnew.close()






