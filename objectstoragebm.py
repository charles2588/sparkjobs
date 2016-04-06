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

    'username': 'xxxx',
    'password': 'xxxxx',
    'auth_url': 'https://identity.open.softlayer.com',
    'project': 'xxxxxx',
    'project_id': 'xxxxxx',
    'region': 'dallas',
    'user_id': 'xxxxx',
    'domain_id': 'xxxxx',
    'domain_name': '837523',
    'filename': 'sample.txt',
    'container': 'notebook',
    'tenantId': 'xxxxx'

}
objectStorageCreds['name'] = 'TEST'

set_hadoop_config(objectStorageCreds)

raw_data = sc.textFile("swift://notebook." + objectStorageCreds['name'] + "/sample.txt")
raw_data.take(5)
