import os, sys, getopt, datetime, shutil
from utils26 import *

nt_tables = set([
	"d_placement", # already handle
	"f_transactional_summary",
	"f_transactional_daily",
	"f_custom_portfolio_daily", # need to add two column
	])

od_tables = set([
	"d_od_placement_current", # add one column
	])

od_remaining_tables = set([
	"d_od_placement",
	"d_od_placement_trait",
	"d_od_placement_nielsen_demographic_assignment",
	"d_od_placement_comscore_demographic_assignment",
	"d_od_placement_rating_based_buying",
	"d_od_rbb_placement_trait",
	"d_od_ad_tree_node",
	"d_od_ad_tree_node_trait",
	"d_od_ad_delivery_curve",
	"d_od_ad_delivery_custom_pacing",
	"d_od_upfront_plan",
	"d_od_upfront_plan_period",
	"d_od_upfront_plan_component",
	"d_od_upfront_plan_component_period",
	"d_od_upfront_plan_component_period_placement_assignment",
	"d_od_placement_abstract_event_assignment",
	"d_od_forecast_portfolio",
	"d_od_forecast_portfolio_dimension_item_assignment",
	"f_od_transactional_placement_summary",
	"f_od_transactional_placement_daily",
	"f_od_transactional_ad_summary",
	"f_od_transactional_ad_competition",
	"f_od_portfolio_ad_daily",
	"f_od_portfolio_interval",
	"f_od_transactional_rbb_summary",
	"f_od_transactional_rbb_daily",
	"f_od_transactional_rbb_demographic",
	"f_od_portfolio_rbb_demographic",
	"f_od_portfolio_rbb_demographic_daily",
	"f_od_portfolio_rbb_demographic_interval",
	"f_od_transactional_summary",
	"f_od_transactional_daily",
	"f_od_transactional_final",
	"f_od_custom_portfolio_daily",
	])

nightly_path = "/tmp/data/nt"
od_path = "/tmp/data/od"
fc_job_ids = {}
fc_job_ids[1] = "20150728103705_2c9db13d378a0c4c72348a9a015952f9"
fc_job_ids[2] = "dc0c74718d76a996602d35caf396f5f1"

# DSN_IB[instance][shard]
DSN_IB={}
DSN_IB[1]={}
DSN_IB[2]={}
DSN_IB[1][1] = "host=192.168.0.205&port=3306&user=forecast&passwd=fwadmin_af&db=xz_fwmrm_forecast1"
DSN_IB[2][1] = "host=192.168.0.205&port=3306&user=forecast&passwd=fwadmin_af&db=xz_fwmrm_forecast2"
DSN_IB[1][2] = "host=192.168.0.205&port=3306&user=forecast&passwd=fwadmin_af&db=xz_fwmrm_forecast3"
DSN_IB[2][2] = "host=192.168.0.205&port=3306&user=forecast&passwd=fwadmin_af&db=xz_fwmrm_forecast4"

STG_IB={}
STG_IB[1]={}
STG_IB[2]={}
STG_IB[1][1] = "host=AFDB01.stg&port=3306&user=forecast&passwd=fwadmin_af&db=fwmrm_forecast"
STG_IB[2][1] = "host=AFDB02.stg&port=3306&user=forecast&passwd=fwadmin_af&db=fwmrm_forecast"
STG_IB[1][2] = "host=AFDB03.stg&port=3306&user=forecast&passwd=fwadmin_af&db=fwmrm_forecast"
STG_IB[2][2] = "host=AFDB04.stg&port=3306&user=forecast&passwd=fwadmin_af&db=fwmrm_forecast"


SQL_FILES = set(["dimension_ondemand_ddl.sql", "dimension_overnight_ddl.sql", "fact_ondemand_ddl.sql", "fact_overnight_ddl.sql",
				"ib_ondemand_view.sql", "ib_overnight_view.sql"])

sql_path = "/tmp/IB_SCHEMA"

def create_all_table():
	for (instance, r) in DSN_IB.items():
		for (shard_id, dsn) in r.items():
			db_config = dict([f.split('=') for f in str(dsn).split('&')])
			conn_sql = "-h %s -u%s -p%s -P %s" % (db_config['host'], db_config['user'], db_config['passwd'], db_config['port'])
			db = db_config['db']
			for sql_file in SQL_FILES:
				os.popen("mysql %s %s -e 'source %s'" % (conn_sql, db, os.path.join(sql_path, sql_file)))

def dump_all_nt_table(logger):
	shard_id = 1
	for ib in STG_IB[1].values():
		ib_instance = connect(ib)
		ib_instance._dsn_ = ib
		path = os.path.join(nightly_path, str(shard_id))
		if not os.path.exists(path):
			os.makedirs(path)
		for table in nt_tables:
			print "begin to dump table %s\n" % (table)
			to_file = os.path.join(path, table)
			sql = "select * from %s limit 10" % table
			dump(ib_instance, sql, to_file, logger)
			print "dump table %s done\n" % (table)
		shard_id += 1

def load_all_nt_table(logger):
	for (instance, r) in DSN_IB.items():
		for (shard_id, dsn) in r.items():
			ib_instance = connect(dsn)
			ib_instance._dsn_ = dsn
			for table in nt_tables:
				print "begin to load table %s \n" % (table)
				file_path = os.path.join(nightly_path, str(shard_id), table)
				load(ib_instance, file_path, table, logger)
				print "load table %s done \n" % (table)

def dump_delta_od_table(instance, fc_job_ids, logger):
	# fc_job_ids="e39ff9ef311a1c70606d946e370f4e4e, 9bf8a4d3ab803c3fd8fd9cea7a178a39"
	for (shard_id, dsn) in STG_IB[instance].items():
		ib_instance = connect(dsn)
		ib_instance._dsn_ = dsn
		path = os.path.join(od_path, str(shard_id))
		if not os.path.exists(path):
			os.makedirs(path)
		for table in od_tables | od_remaining_tables:
			to_file = os.path.join(path, table)
			sql = "select * from %s where fc_job_id in ('%s')" % (table, fc_job_ids[shard_id])
			dump(ib_instance, sql, to_file, logger)

def load_delta_od_table(instance, logger):
	for (shard_id, dsn) in DSN_IB[instance].items():
		ib_instance = connect(dsn)
		ib_instance._dsn_ = dsn
		for table in od_tables | od_remaining_tables:
			file_path = os.path.join(od_path, str(shard_id), table)
			if os.path.exists(file_path):
				load(ib_instance, file_path, table, logger)
			


if __name__ == '__main__':
	opts, args = getopt.getopt(sys.argv[1:], '', [
		"self=",
		"job=",
		"shard=",
		])
	self_instance = 1
	for opt, arg in opts:
		if opt == "--self": self_instance = int(arg)
		elif opt == "--job": job = arg
		elif opt == "--shard": shard_id = int(arg)
	logger = job.find("nt") >= 0 and init_logger("/tmp/nt.log") or init_logger("/tmp/od.log")

	if job == "dump_nt":
		dump_all_nt_table(logger)
		print "dump all nightly table done!\n"
	elif job == "load_nt":
		load_all_nt_table(logger)
		print "load all nightly table done!\n"
	elif job == "dump_delta_od":
		dump_delta_od_table(self_instance, fc_job_ids, logger)
	elif job == "load_delta_od":
		load_delta_od_table(self_instance, logger)
	elif job =="create_table":
		create_all_table()

