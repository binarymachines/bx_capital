

extract_sample:
	CAPDEMO_HOME=`pwd` PYTHONPATH=`pwd` xfile --config config/extract_orders.yaml --delimiter ',' --map orders order_log.csv --limit 5


extract_full:
	CAPDEMO_HOME=`pwd` PYTHONPATH=`pwd` xfile --config config/extract_orders.yaml --delimiter ',' --map orders order_log.csv


generate_dim_year:
	./dgenr8.py --plugin-module year_generator --sql --schema capdb --dim-table dim_date_year --columns id value


generate_dim_month:
	./dgenr8.py --plugin-module month_generator --sql --schema capdb --dim-table dim_date_month --columns id value


generate_dim_day:
	./dgenr8.py --plugin-module day_generator --sql --schema capdb --dim-table dim_date_day --columns id value


init_db:
	cat sql/capital_ddl.sql | pgexec --target bxlogic_db --db binary_test -s
	cat sql/capital_initial_data.sql | pgexec --target bxlogic_db --db binary_test -s