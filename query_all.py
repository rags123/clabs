from bigquery import get_client

project_id = 'crypto-will-95713'

service_account = '89889769326-u9cubur9japr4ncgn13jsm5c3c1cmeqp@developer.gserviceaccount.com'

key = 'clabs-da9b4e83b6fa.p12'

client = get_client(project_id, service_account=service_account, 
    private_key_file=key, readonly=False)


fields = ['Username', 'Visits', 'Location', 'Device', 'Screensize', 'Browser', 'Products', 'Category']
if client:
    print "Connection to big query established."
    datasets = client.get_datasets()
    print datasets

    """Code for creating a table with the fields."""
    # dataset = client.create_dataset('development', friendly_name="clab_dev", description="Test database for CLABS")
    # schema_1 = [
    # {'name': 'Username', 'type': 'STRING', 'mode': 'required'},
    # {'name': 'Email', 'type': 'STRING', 'mode': 'nullable'},
    # {'name': 'Visits', 'type': 'INTEGER', 'mode': 'nullable'},
    # {'name': 'Password', 'type': 'STRING', 'mode': 'nullable'},
    # {'name': 'Location', 'type': 'STRING', 'mode': 'nullable'},
    # {'name': 'Device', 'type': 'STRING', 'mode': 'nullable'},
    # {'name': 'Screensize', 'type': 'INTEGER', 'mode': 'nullable'},
    # {'name': 'Browser', 'type': 'STRING', 'mode': 'nullable'},
    # {'name': 'Products', 'type': 'STRING', 'mode': 'nullable'},
    # {'name': 'Category', 'type': 'STRING', 'mode': 'nullable'}
    # ]
    
    # created = client.create_table('development', 'user_table', schema_1)
    # if created:
    #   print "table created."

    """Delete a particular dataset."""
    # if datasets:
    #   client.delete_dataset('ap')
    #   client.delete_dataset('ap', delete_contents=True)

    """Insert rows."""
    # rows =[
    #   {'Username': 'Mick', 'Location': 'Bangalore', 'Device': 'Mobile', 
    #   'Visits': '5', 'Screensize': '4', 'Browser': 'Chrome', 'Products': 'clear', 'Category': 'shampoo'},
    #   {'Username': 'Nick', 'Location': 'Bangalore', 'Device': 'Mobile', 
    #   'Visits': '7', 'Screensize': '5', 'Browser': 'Firefox', 'Products': 'kurkure', 'Category': 'snack'},
    #   {'Username': 'Sam', 'Location': 'Delhi', 'Device': 'Desktop', 
    #   'Visits': '6', 'Screensize': '21', 'Browser': 'Chrome', 'Products': 'allout', 'Category': 'repellant'},
    #   {'Username': 'Linda', 'Location': 'Mumbai', 'Device': 'Tablet', 
    #   'Visits': '4', 'Screensize': '12', 'Browser': 'Safari', 'Products': 'clear', 'Category': 'shampoo'},
    #   {'Username': 'Shaun', 'Location': 'Chennai', 'Device': 'Desktop', 
    #   'Visits': '7', 'Screensize': '24', 'Browser': 'Opera', 'Products': 'liril', 'Category': 'soap'},
    # ]

    # inserted = client.push_rows('development', 'user_table', rows)

    # if inserted:
    #   print "Rows created."

    data = {}
    user = {}
    for field in fields:
        print "Querying w.r.t %s"%(field)
        if field == 'Username':
            print "In: ",field
            job_id, _results = client.query('SELECT %s, Visits FROM development.user_table'%(field))
        elif field != 'Username' and field != 'Visits':
            print "In: ", field
            job_id, _results = client.query('SELECT %s FROM development.user_table'%(field))
        else:
            pass
        # Do other stuffs

        # Poll for query completion.
        complete, row_count = client.check_job(job_id)

        # Retrieve the results.
        if complete:
            data_list = []
            results = client.get_query_rows(job_id)
            print results

            if results:
                # visits = 0
                for result in results:
                    if field != 'Username' and field != 'Visits':
                        print result[field]
                        data_list.append(result[field])
                    if field == 'Username':
                        print result['Username']
                        print result['Visits']
                        user[result['Username']] = result['Visits']

                    # print result['Username']
                    # print result['Visits']

                    # if result['Visits'] > visits:
                    #   visits = result['Visits']
                    #   most_visited = result['Username']
                if field != 'Username' or field != 'Visits':
                    data[field] = data_list
                    data_list = []
                elif field == 'Username':
                    data['user'] = user
                    user = {}
            # print most_visited, visits

    print data
    temp_dict = {}
    count = 1
    for key in data:
        if key != "Username" and key != 'Visits':
            print key
            for value in data[key]:
                if value not in temp_dict:
                    temp_dict[value] = count
                else:
                    temp_dict[value] = count+1
            print temp_dict
            temp_dict = {}


    """Delete an existing table."""
    # deleted = client.delete_table('development', 'user_table')
    # exists = client.check_table('development', 'user_table')
    # print exists
else:
    print "Try again."