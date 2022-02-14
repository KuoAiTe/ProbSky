import os
import yaml
import datetime
import itertools

def load(local = True):
    with open('config/configs.yaml') as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)
        server_configs = configs['experiments']
        if local:
            server_configs = server_configs['local']
        else:
            server_configs = server_configs['cluster']
        configs = configs['configs']
        #print(configs)


    default_args = [['spark-submit']]
    files_location = ''
    if 'target-class' in configs:
        default_args.append(["--class", configs['target-class']])
    if 'master' in server_configs:
        default_args.append(["--master", server_configs['master']])
    if 'files-location' in server_configs:
        files_location = server_configs['files-location']
    if 'driver' in server_configs:
        driver_config = server_configs['driver']
        if 'cores-per-driver' in driver_config:
            default_args.append(["--driver-cores", driver_config['cores-per-driver']])
        if 'memory-per-driver' in driver_config:
            default_args.append(["--driver-memory", driver_config['memory-per-driver']])
    if 'executor' in server_configs:
        executor_config = server_configs['executor']
        if 'total' in executor_config:
            default_args.append(["--num-executors", executor_config['total']])
        if 'cores-per-executor' in executor_config:
            default_args.append(["--executor-cores", executor_config['cores-per-executor']])
        if 'memory-per-executor' in executor_config:
            default_args.append(["--executor-memory", executor_config['memory-per-executor']])
    if 'jars' in configs:
        default_args.append(["--jars", ','.join(configs['jars'])])
    if 'packages' in server_configs:
        default_args.append(["--packages", ','.join(server_configs['packages'])])

    if 'log-properties' in configs and 'target-jar' in configs:
        log4jPath = 'file:///{}'.format(os.path.abspath(configs['log-properties']))
        default_args.append(["--conf", 'spark.driver.extraJavaOptions=-Dlog4j.debug=true'])
        default_args.append(["--conf", 'spark.executor.extraJavaOptions=-Dlog4j.debug=true'])
        default_args.append(["--conf", 'spark.driver.extraJavaOptions=-Dlog4j.configuration={}'.format(log4jPath)])
        default_args.append(["--conf", 'spark.executor.extraJavaOptions=-Dlog4j.configuration={}'.format(log4jPath)])
        default_args.append(["--conf", 'spark.memory.offHeap.enabled=true'])
        if 'memory-per-executor' in executor_config:
            default_args.append(["--conf", 'spark.memory.offHeap.size={}'.format(executor_config['memory-per-executor'])])
        if 'memory-fraction' in executor_config:
            default_args.append(["--conf", 'spark.memory.fraction={}'.format(executor_config['memory-fraction'])])
        default_args.append(["--conf", 'spark.serializer=org.apache.spark.serializer.KryoSerializer'])
        default_args.append(["--conf", 'spark.kryo.registrationRequired=true'])
        default_args.append(["--conf", 'spark.kryo.registrator=probSkyline.ProbSkyKryoRegistrator'])
        default_args.append(["--conf", 'spark.kryoserializer.buffer.max=2047m'])
        default_args.append(["--conf", 'spark.kryoserializer.buffer=256k'])
        default_args.append(["--files", configs['log-properties'], configs['target-jar']])
    default_args = [str(value) for args in default_args for value in args]
    commands = []
    if 'runs' in server_configs:
        test_config = server_configs['runs']
        if 'files' in test_config:
            files = list(itertools.chain(*test_config['files']))
            for file in files:
                repeat_times = file['repeat-times']
                for index in range(repeat_times):
                    rand_seed = int(datetime.datetime.now().timestamp() * 1000)
                    src = "{}{}".format(files_location, file['src'])
                    dimension = file['dimension']
                    probability_threshold_tested = file['probability-threshold']
                    methods = file['methods']
                    #print(file)
                    num_partition_tested = file['num-partition']
                    for num_partition in num_partition_tested:
                        #print('numPartition', num_partition)
                        for probability_threshold in probability_threshold_tested:
                            base_dict = {
                                'src': src,
                                'dimension': dimension,
                                'num-partition': num_partition,
                                'probability-threshold': probability_threshold,
                                'rand-seed': rand_seed,
                            }
                            for method, method_config in methods.items():
                                if 'enabled' not in method_config or not method_config['enabled']:
                                    continue

                                test = []
                                for key, value in method_config.items():
                                    if key.endswith('-test'):
                                        originalKey = key[:-5]
                                        test.append((originalKey, value))
                                        continue

                                arg_dict = {**base_dict, **method_config, **{'method': method}}
                                if len(test) > 0:
                                    for (test_key, test_value) in test:
                                        test_dict = arg_dict.copy()
                                        if test_key in arg_dict:
                                            for value in test_value:
                                                args = []
                                                test_dict[test_key] = value
                                                for key, value in test_dict.items():
                                                    if key.endswith('-test'):
                                                        continue
                                                    newKey = '--{}'.format(key)
                                                    args.append(newKey)
                                                    args.append(value)
                                                spark_args = default_args + args
                                                spark_args = [str(arg) for arg in spark_args]
                                                commands.append(spark_args)
                                else:
                                    args = []
                                    for key, value in arg_dict.items():
                                        if key.endswith('-test'):
                                            continue
                                        newKey = '--{}'.format(key)
                                        args.append(newKey)
                                        args.append(value)
                                    spark_args = default_args + args
                                    spark_args = [str(arg) for arg in spark_args]
                                    commands.append(spark_args)
    return commands
