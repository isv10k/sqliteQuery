import pickle
from sqlite_conn import SqliteConn
import pprint

DB_PATH = 'Chinook_Sqlite.sqlite'


def execute_query(query_string):
    """Executes query and returns data from data base"""
    with SqliteConn(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(query_string)
        data = cursor.fetchall()
        return data


def first_query():
    query_string = '''
        SELECT DISTINCT c.CustomerId, c.FirstName, c.Phone, c.Company
        FROM Customer as c
        INNER JOIN Employee as e on e.EmployeeId = c.SupportRepId
        LEFT JOIN Invoice as i on i.CustomerId = c.CustomerId
        LEFT JOIN InvoiceLine as il on il.InvoiceId = i.InvoiceId
        LEFT JOIN Track as t on t.TrackId = il.TrackId
        LEFT JOIN Genre as g on g.GenreId = t.GenreId
        WHERE g.Name != 'Rock' 
            AND strftime('%Y','now') - strftime('%Y',e.BirthDate) > 50   
        ORDER BY c.City ASC, e.Email DESC
        LIMIT 10 
    '''
    return execute_query(query_string)


def second_query_to_pickle(file_name):
    query_string = '''
        SELECT ee.FirstName, ee.LastName, ee.Phone, er.FirstName, er.LastName, er.Phone
        FROM Employee as ee
        INNER JOIN Employee as er on er.EmployeeId = ee.ReportsTo        
    '''
    query_result = execute_query(query_string)
    data = {}
    key = 0
    for entry in query_result:
        data[key] = {
            'employee_FirstName': entry[0],
            'employee_LastName': entry[1],
            'employee_Phone': entry[2],
            'reports_to_FirstName': entry[3],
            'reports_to_LastName': entry[4],
            'reports_to_Phone': entry[5],
        }
        key += 1

    with open(file_name, 'wb+') as fp:
        pickle.dump(data, fp)
    return data


def third_query():
    query_string = '''
        SELECT DISTINCT c.FirstName, c.Phone
        FROM Customer as c
        LEFT JOIN Invoice as i on i.CustomerId = c.CustomerId
        LEFT JOIN InvoiceLine as il on il.InvoiceId = i.InvoiceId
        WHERE il.UnitPrice in (
            SELECT max(UnitPrice) as maxim
            FROM InvoiceLine
        )
        ORDER BY c.FirstName            
    '''
    return execute_query(query_string)


if __name__ == '__main__':
    pprint.pprint(first_query(), width=100)
    print('-' * 80)
    pprint.pprint(second_query_to_pickle('saved.pickle'), width=100)
    print('-' * 80)
    pprint.pprint(third_query())



