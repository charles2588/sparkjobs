import pyspark.rdd
import pyspark.sql.functions as func


conf = SparkConf().setAppName('spark_test').setMaster('local')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


def set_hadoop_config(creds):
    prefix = "fs.swift.service." + creds['name']
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set(prefix + ".auth.url", creds['auth_url'] + '/v3.0/tokens')
    hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
    hconf.set(prefix + ".tenant", creds['project_id'])
    hconf.set(prefix + ".username", creds['user_id'])
    hconf.set(prefix + ".password", creds['password'])
    hconf.setInt(prefix + ".http.port", 8080)
    hconf.set(prefix + ".region", creds['region'])
    hconf.setBoolean(prefix + ".public", True)


# object storage credentials
objectStorageCreds = {

    'username': 'Admin_5b8faff871d87700d342130e7cc52e06a7019feb',
    'password': 'y]6fCD{bYrqPnI8T',
    'auth_url': 'https://identity.open.softlayer.com',
    'project': 'object_storage_216c032f_3f57_4763_ae97_5c6a83a0d523',
    'project_id': 'e097bbd898534ed1ad0e45c82baedb2d',
    'region': 'dallas',
    'user_id': 'a493676500794827b020874099c5ee1c',
    'domain_id': 'da5b6dd1c8374f67b1050172badbef8c',
    'domain_name': '837523',
    'filename': 'sample.txt',
    'container': 'notebook',
    'tenantId': 'sb03-405c52350912e8-2c631c8ff999'

}
objectStorageCreds['name'] = 'TEST'

set_hadoop_config(objectStorageCreds)

raw_data = sc.textFile("swift://notebook." + objectStorageCreds['name'] + "/sample.txt")
raw_data.take(5)
