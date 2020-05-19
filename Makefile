

extract_sample:
	CAPDEMO_HOME=`pwd` PYTHONPATH=`pwd` xfile --config config/extract_orders.yaml --delimiter ',' --map orders order_log.csv --limit 5


extract_full:
	CAPDEMO_HOME=`pwd` PYTHONPATH=`pwd` xfile --config config/extract_orders.yaml --delimiter ',' --map orders order_log.csv


init_db:
	cat sql/capital_ddl.sql | pgexec --target bxlogic_db --db binary_test -s
	cat sql/capital_initial_data.sql | pgexec --target bxlogic_db --db binary_test -s