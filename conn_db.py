
from django.db import connection as conn

# function to Return all rows from a cursor as a dict
from django.http import JsonResponse
from utilities.Constants import TOKEN_KEY, STATUS_KEY, MESSAGE_KEY, DB_ERROR
from utilities.api_response import HTTP_500_INTERNAL_SERVER_ERROR


def dictfetchall(cursor):
    """Return all rows from a cursor as a dict"""
    columns = [col[0] for col in cursor.description]
    print("descript",cursor.description)
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def dictfetchone(cursor):
    """Return all rows from a cursor as a dict"""
    columns = [col[0] for col in cursor.description]
    final_data = [
        dict(zip(columns, row)) for row in cursor.fetchone()
    ]
    if len(final_data) is 0:
        return None
    else:
        return final_data[0]

def insertone(cursor):
    columns = [col[0] for col in cursor.insert]
    return [
        dict(zip(columns, row) for row in cursor.insert())
    ]

# def dictinsertrow(cursor):



# Function to get the data from table without any role
def django_search_query_one(sql):
    if conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            data = dictfetchone(cursor)
            return data
    else:
        return JsonResponse({STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,   MESSAGE_KEY: DB_ERROR})



def django_search_query_all(sql):
    if conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            data = dictfetchall(cursor)
            return data
    else:
        return JsonResponse({STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,   MESSAGE_KEY: DB_ERROR})

def django_query_insert_specific_role(sql):

        if conn:
            with conn.cursor() as cursor:
                try:
                    for each_sql in sql:
                        cursor.execute(each_sql)
                    conn.commit()
                    return conn, 0
                except Exception as err:
                    return 0, err
        else:
            return JsonResponse({STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,   MESSAGE_KEY: DB_ERROR})

