import multiprocessing


# 3. 创建一个测试程序
def test(idx, test_dict):
    test_dict[idx] = idx


if __name__ == '__main__':
    # 1. 创建一个Manger对象
    manager = multiprocessing.Manager()
    # 2. 创建一个dict
    temp_dict = manager.dict()
    # 4. 创建进程池进行测试
    pool = multiprocessing.Pool(4)
    for i in range(100):
        pool.apply_async(test, args=(i, temp_dict))
    pool.close()
    pool.join()
    print(temp_dict)
