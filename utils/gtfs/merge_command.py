#!/usr/bin/env python2

from transit_analysis import schema

template = """
BEGIN;

CREATE TEMP TABLE source(LIKE %(target)s INCLUDING ALL) ON COMMIT DROP;

COPY source FROM STDIN;
ANALYZE source;
LOCK TABLE %(target)s IN SHARE ROW EXCLUSIVE MODE;

%(update_query)s

INSERT INTO %(target)s
SELECT %(allcols)s FROM source
LEFT JOIN %(target)s ON %(pk_compare)s
WHERE %(pk_antijoin)s;

COMMIT;
"""

update_template="""
UPDATE %(target)s
SET %(column_update)s
FROM source
WHERE %(pk_compare)s;
"""

def generate(table_name, update=False):
	table = schema.metadata.tables[table_name]
	pks = map(str, table.primary_key.columns)
	pks = [k.split('.')[-1] for k in pks]
	
	allcols = map(str, table.columns)
	allcols = [k.split('.')[-1] for k in allcols]
	cols = [c for c in allcols if c not in pks]
	
	param = {}

	param['target'] = table_name
	param['pk_compare'] = " AND ".join(
		"%s.%s=source.%s"%(table_name, pk, pk) for pk in pks)
	param['column_update'] = ", ".join(
		"%s=source.%s"%(c, c) for c in cols)
	param['allcols'] = ", ".join("source."+c for c in allcols)

	param['pk_antijoin'] = "%s.%s IS NULL"%(table_name, pks[0])
	if update and len(cols) > 0:
		param['update_query'] = update_template
	else:
		param['update_query'] = ""
	
	print template%param

if __name__ == '__main__':
	import argh
	argh.dispatch_command(generate)
