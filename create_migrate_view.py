import os, re, getopt, sys
import scheduler_config
import collections

SCHEMA_SAVE_PATH = "./schema_save"
HISTORY_START_VERSION = 62
PRE_VERSION = int(scheduler_config.PRE_VERSION)
VERSION = int(scheduler_config.VERSION)


def get_table_name(sql):
	m = re.match("CREATE\sTABLE\s\W?(\w+)\W", sql)
	if m:
		return m.group(1)

def get_field_name(sql):
	sql = sql.strip()
	m = re.match("\W?(\w+)\W?", sql)
	if m:
		return m.group(1)

def ensure_dir(dir):
	if not os.path.exists(dir): os.makedirs(dir)

class Table_Schema:
	def __init__(self, sql = ""):
		self.table_name = ""
		self.columns = []
		self.create_sql = ""
		self.parse(sql)

	def parse(self, sql):
		for s in sql.split("\n"):
			s = s.strip()
			if len(s) < 3: continue
			if s.startswith("--") or s.startswith("("): continue
			if s.find("CREATE TABLE") >= 0: self.table_name = get_table_name(s)
			else:
				column_name = get_field_name(s)
				if column_name: self.columns.append(column_name)
		self.create_sql = sql

	def merge_table(self, table):
		self.table_name = table.table_name
		existing_columns = set(self.columns)
		for column in table.columns:
			if column in existing_columns: continue
			self.columns.append(column)

def load_table_schemas(version, file, table_schemas):
	f = open(os.path.join(SCHEMA_SAVE_PATH, str(version), file), "r").read()
	for sql in f.split(";"):
		if sql.startswith("--") or sql.find("DROP") >= 0: continue
		if sql.find("CREATE TABLE") < 0: continue
		table_schema = Table_Schema(sql)
		print "init table %s for version %d" % (table_schema.table_name, version)
		if not table_schema.table_name: continue
		table_schemas[table_schema.table_name] = table_schema
	return table_schemas

def generate_view_schema(schemas):
	view_schema = {}
	for version in range(VERSION, PRE_VERSION - 1, -1):
		if version not in schemas: continue
		for table_name, schema in schemas[version].iteritems():
			if table_name not in view_schema: view_schema[table_name] = Table_Schema()
			view_schema[table_name].merge_table(schema)

	return view_schema

def generate_pre_sql(view_schema, schemas):
	file = open(os.path.join(SCHEMA_SAVE_PATH, str(PRE_VERSION) + ".sql"), "wb")
	for table in view_schema.iterkeys():
		if table in schemas[PRE_VERSION]: continue
		if table not in schemas[VERSION]: continue
		sql = schemas[VERSION][table].create_sql.replace(table, "%s_%d" % (table, PRE_VERSION))
		file.write("%s;\n\n" % sql)
	file.close()

def generate_current_sql(schemas):
	file = open(os.path.join(SCHEMA_SAVE_PATH, str(VERSION) + ".sql"), "wb")
	for table, schema in schemas[VERSION].iteritems():
		sql = schema.create_sql.replace(table, "%s_%d" % (table, VERSION))
		file.write("%s;\n\n" % sql)
	file.close()

def generate_view_sql(view_schema, schemas):
	file = open(os.path.join(SCHEMA_SAVE_PATH, "view.sql"), "wb")
	for table, schema in view_schema.iteritems():
		sub_select = []
		for version in range(HISTORY_START_VERSION, VERSION + 1):
			if table.find("_od_") < 0 and version < PRE_VERSION: continue
			if table not in schemas[version]: continue
			table_columns = set(schemas[version][table].columns)
			columns = []
			for column in schema.columns:
				columns.append("0 AS %s" % column if column not in table_columns else column)
			columns = ", ".join(columns)
			sub_select.append("SELECT %s FROM %s_%d" % (columns, table, version))
		sub_select = "\nUNION\n".join(sub_select)
		sql = """DROP VIEW IF EXISTS %s;
CREATE VIEW %s AS
%s;\n\n""" % (table, table, sub_select)
		file.write(sql)
	file.close()
		
def generate_view():
	files = ['dimension_ondemand_ddl.sql', 'dimension_overnight_ddl.sql', 'fact_ondemand_ddl.sql', 'fact_overnight_ddl.sql']
	new_table_struct = {}
	pre_table_struct = {}
	view_struct = {}
	# get view structs
	schemas = collections.defaultdict(dict)
	for version in range(HISTORY_START_VERSION, VERSION + 1):
		for file in files:
			if file.find("ondemand") >= 0 and version < PRE_VERSION: continue
			load_table_schemas(version, file, schemas[version])

	view_schema = generate_view_schema(schemas)

	generate_pre_sql(view_schema, schemas)
	generate_current_sql(schemas)
	generate_view_sql(view_schema, schemas)

def copy_schema_from_svn(history_start_version):
	# will copy from start version
	for version in range(history_start_version, VERSION + 1):
		save_folder = os.path.join(SCHEMA_SAVE_PATH, str(version))
		ensure_dir(save_folder)
		svn_path = "https://svn.dev.freewheel.tv/repos/common/schema/trunk/Forecast/schema/" + str(int(version) / 10.0)
		cmd = "svn co " + svn_path + " " + str(save_folder)
		print cmd
		os.popen(cmd).readline()

if __name__ == '__main__':
	opts, args = getopt.getopt(sys.argv[1:], '', [
		"job=",
		])
	
	job = "generate_view"
	for opt, arg in opts:
		if opt == "--job": job = arg

	if job == "generate_view":
		copy_schema_from_svn(HISTORY_START_VERSION)
		generate_view()
	elif job == "svn_co":
		copy_schema_from_svn(HISTORY_START_VERSION)
	elif job == "generate_view_without_svn_co":
		generate_view()

