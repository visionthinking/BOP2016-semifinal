# coding=utf8

import time
import requests
import concurrent.futures

# 并行不同多个func, 返回第一个成功执行的结果
def threads_jobs_first_result(func_list, n=10):
    n = min(n, 1000)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=n)
    futures = [executor.submit(func) for func in func_list]
    first_result = None
    for future in concurrent.futures.as_completed(futures):
        try:
            first_result = future.result()
            break
        except Exception as e:
            print e
    for _future in futures:
        _future.cancel()
    executor.shutdown(wait=False)
    return first_result

# 并行多个不同func, 返回有序结果列表
def threads_jobs_orderly(func_list, n=10):
    n = min(n, 1000)
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as executor:
        def _func(func):
            return func()
        results = list(executor.map(_func, func_list))
        return results

# 并行多个相同func, 每个func取arg_list的一个参数
def threads_same_job(func, arg_list, n=10):
    n = min(n, 1000)
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as executor:
        results = list(executor.map(func, arg_list))
        return results

def threads_jobs(func_list, args=[], n=10):
    n = min(n, 1000)
    with concurrent.futures.ThreadPoolExecutor(max_workers=n) as executor:
        futures = [executor.submit(func, *args) for func in func_list]
        results = []
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception, e:
                pass
        return results

if __name__ == "__main__":

    def job1():
        time.sleep(1)
        print 'job1'
        return 1
    
    def job2():
        time.sleep(1)
        print 'job2'
        return 2
    def job3(x):
        time.sleep(1)
        return x+1
    def test_threads_jobs():
        print threads_jobs([job1, job2], [])
    
    def test_threads_same_job():
        print threads_same_job(job3, xrange(10), 10)

    def test_threads_jobs_orderly():
        print threads_jobs_orderly([job1, job2])

    # test_threads_same_job()
    # test_threads_jobs_orderly()
    print threads_jobs_first_result([job1, job2])