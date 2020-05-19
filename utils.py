#!/usr/bin/env python

from contextlib import ContextDecorator


class retry_on_exception(ContextDecorator):
    def __init__(self, target_exception_count, target_function, retry_hook):
        super().__init__()
        self.target_function = target_function
        self.retry_hook = retry_hook
        self.target_exception_count = target_exception_count
        self.num_retries = 0
        self.result = None

    def __enter__(self):
        print('Entering retry context with target count %s and %s retries.' % 
              (self.target_exception_count, self.num_retries), file=sys.stderr)
        while True:
            try:
                print('>>> calling target...', file=sys.stderr)
                self.result = self.target_function()
                break
            except Exception as err:
                if self.num_retries == self.target_exception_count:
                    print('Exiting retry context in FAIL mode with target count %s and %s retries.' % 
                          (self.target_exception_count, self.num_retries), file=sys.stderr)
                    raise err
                else:
                    self.retry_hook()
                    self.num_retries += 1
   
        return self

    def __exit__(self, *exc):
        print('Exiting retry context with target count %s and %s retries.' % (self.target_exception_count, self.num_retries))
        return False


def exec_sql(sql_cmd, service_registry):
    secret_svc = service_registry.lookup('credentials')    
    db_svc = None
    connect_func = lambda: test_services.PostgreSQLService(**secret_svc.data('postgres'))
    with retry_on_exception(1, connect_func, secret_svc.reload_from_aws) as retry_context:
        db_svc = retry_context.result

    connection = db_svc.engine.connect()
    txn = connection.begin()

    try:
        sql = text(sql_cmd)
        connection.execute(sql)
        txn.commit()
    except:
        txn.rollback()
        raise
    finally:
        connection.close()
