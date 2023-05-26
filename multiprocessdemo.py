'''
Description: 
Author: jiangsheng
Date: 2023-05-25 21:50:41
LastEditors: jiangsheng
LastEditTime: 2023-05-26 10:50:15
'''
from multiprocessing import Pool, cpu_count, Value, Manager
import loguru

loguru.logger.add("./logs/multiprocess_template.log", enqueue=True, rotation="10 MB")

#---------------------设置函数---------------------
def real_work(param):
    pass

#-------------------------------------------------

def assign_task(cpu_count, task_list):
    task_list = sorted(task_list)
    assign_task_list = []
    unit = int(len(task_list) / cpu_count)
    for x in range(cpu_count):
        min_v = x * unit
        max_v = (x + 1) * unit
        if x == cpu_count - 1:
            assign_task_list.append(task_list[min_v:])
        else:
            assign_task_list.append(task_list[min_v:max_v])
    return assign_task_list

def multi_function(child_task,begin_i,count_total):
    begin = begin_i
    child_total = len(child_task)
    child_count = 1
    result_list=[]
    for param in child_task:
        loguru.logger.info("processing pid process_count process_total_count,{},{},{}, {}/{}".format(begin,child_count,child_total,begin_i,count_total))
        child_count=child_count+1
        r=real_work(param)
        result_list.append(r)
        begin_i = begin_i+1
    return result_list


if __name__=="__main__":
    #-------------------------------设置任务---------------------------------
    total_task_list = []
    use_cpu = int(cpu_count() / 3.0)



    total_task_list=[ i for i in range(1,9999,1)]
    use_cpu=10










    #  勿动！
    #-----------------------------------------------------------------------------------------
 
    count_total = len(total_task_list)
    result = []
    if use_cpu == 1:
        begin_i = 1
        r = multi_function(total_task_list, begin_i , count_total)
        result.append(r)
    else:
        begin_i = 1
        total_task_list = assign_task(use_cpu,total_task_list)
        pool = Pool(use_cpu)
        for child_task_list in total_task_list:
            r = pool.apply_async(multi_function,
                                args=(
                                    child_task_list,begin_i, count_total))
            begin_i = begin_i + len(child_task_list)
            result.append(r)
        pool.close()
        pool.join()
    #------------------------------------------------------------------------------------------

    # import polars as pl
    # df = pl.read_csv("./logs/multiprocess_template.log")
    # df.columns=["no","pid","process_count","process_total","all_total"]
    # df = df.groupby("pid").agg([
    #     pl.col("process_count").max().alias("current"),
    #     pl.col("process_total").last().alias("max")]
    # ).sort("pid")