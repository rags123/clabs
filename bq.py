# from bigquery import get_client
# sys.path.insert(1, os.path.join(os.path.abspath('.'), '/gae'))
from bigquery import get_client

from flask import Flask, request, redirect, url_for, send_from_directory
from werkzeug import secure_filename #, SharedDataMiddleware

project_id = 'crypto-will-95713'

service_account = '89889769326-u9cubur9japr4ncgn13jsm5c3c1cmeqp@developer.gserviceaccount.com'

key = 'clabs-da9b4e83b6fa.p12'

client = get_client(project_id, service_account=service_account, 
    private_key_file=key, readonly=False)

# fields = ['Username', 'Visits', 'Location', 'Device', 'Screensize', 'Browser', 'Products', 'Category']

app = Flask(__name__)

name = {}

# The HTML file to be rendered
s = """
            <html>
              <head>
                <!--Load the AJAX API-->
                <script type="text/javascript" src="https://www.google.com/jsapi"></script>
                <script type="text/javascript">

                  // Load the Visualization API and the piechart package.
                  google.load('visualization', '1.0', {'packages':['corechart']});

                  // Set a callback to run when the Google Visualization API is loaded.
                  google.setOnLoadCallback(drawChart);

                  // Callback that creates and populates a data table,
                  // instantiates the pie chart, passes in the data and
                  // draws it.
                  function drawChart() {

                    // Create the data table.
                    var data = new google.visualization.DataTable();
                    data.addColumn('string', 'Topping');
                    data.addColumn('number', 'Slices');
                    data.addRows([
                      ['%(1)s', %(M1)s],
                      ['%(2)s', %(M2)s],
                      ['%(3)s', %(M3)s],
                      ['%(4)s', %(M4)s],
                      ['%(5)s', %(M5)s]
                    ]);
                    
                    // Create the data table.
                    var data2 = new google.visualization.DataTable();
                    data2.addColumn('string', 'Topping');
                    data2.addColumn('number', 'Slices');
                    data2.addRows([
                      ['%(6)s', %(M6)s],
                      ['%(7)s', %(M7)s],
                      ['%(8)s', %(M8)s],
                      ['%(9)s', %(M9)s],
                      ['%(10)s', %(M10)s]
                    ]);
                    
                    // Create the data table.
                    var data3 = new google.visualization.DataTable();
                    data3.addColumn('string', 'Topping');
                    data3.addColumn('number', 'Slices');
                    data3.addRows([
                      ['%(11)s', %(M11)s],
                      ['%(12)s', %(M12)s],
                      ['%(13)s', %(M13)s],
                      ['%(14)s', %(M14)s],
                      ['%(15)s', %(M15)s]
                    ]);

                    // Set chart options
                    var options = {'title':'Users vs the number of visits',
                                   'width':400,
                                   'height':300};

                    var options2 = {'title':'Products vs the number of users',
                                   'width':400,
                                   'height':300};

                    var options3 = {'title':'Location vs the number of users',
                                   'width':400,
                                   'height':300};


                    // Instantiate and draw our chart, passing in some options.
                    var chart = new google.visualization.BarChart(document.getElementById('chart_div'));
                    chart.draw(data, options);
                    var chart2 = new google.visualization.BarChart(document.getElementById('chart_div2'));
                    chart2.draw(data2, options2);
                    var chart3 = new google.visualization.BarChart(document.getElementById('chart_div3'));
                    chart3.draw(data3, options3);
                  }
                </script>
              </head>

              <body>
                <!--Div that will hold the pie chart-->
                <div id="chart_div"></div>
                <div id="chart_div2"></div>
                <div id="chart_div3"></div>
                <form action = "/" method=post enctype=multipart/form-data>
                  <select name="category">
                      <option value="location">Location</option>
                      <option value="products">Products</option>
                      <option value="device">Device</option>
                      <option value="browser">Browser</option>
                    </select><br />
                  <input type=submit value=change>
                </form>
              </body>
            </html>
     """

# Route the requests to handlers created using flask
@app.route('/', methods=['GET', 'POST'])
def render():
    if request.method == 'GET':

        if client:
            print "Connection to big query established."
            datasets = client.get_datasets()
            print datasets

            visits()
            print name
            print "length is: ", len(name)

            general('Products', 'other_prod')
            print name
            print "length is: ", len(name)

            general('Location', 'other_loc')
            print name
            print "length is: ", len(name)

            return s%name

        else:
            print "Try again."
        
    else:
        cat = request.form.get('category')
        # if cat in category_list:
        if cat == 'location':
            delete_keys()
            general('Location', 'other_location')
            return s%name

        elif cat == 'device':
            delete_keys()
            general('Device', 'other_devices')
            return s%name

        elif cat == 'browser':
            delete_keys()
            general('Browser', 'other_browsers')
            return s%name

        elif cat == 'screensize':
            delete_keys()
            general('Screensize', 'other_sizes')
            return s%name

        else:
            print "Invalid category"
        print "post request"


# Function for querying the database specific to tables
def visits():
    job_id, _results = client.query('SELECT Username, Visits FROM development.user_table')

    # Poll for query completion.
    complete, row_count = client.check_job(job_id)

    # Retrieve the results.
    if complete:
        results = client.get_query_rows(job_id)
        print results

        if results:
            for i in range(5):
                singleton = results[i]
                print singleton
                for k, v in singleton.iteritems():
                    print k, v
                    if type(v) == int:
                        key = 'M' + str(i+1)
                        name[key] = v
                    else:
                        key = str(i+1)
                        name[key] = v


# Generalised function for querying the database and extracting data in usable form
def general(field, oth):
    job_id, _results = client.query('SELECT %s FROM development.user_table'%(field))
    complete, row_count = client.check_job(job_id)

    if complete:
        results = client.get_query_rows(job_id)
        print results

        temp_list = []
        # count = 1
        others = oth
        if results and len(name) <= 20:
            dict_len = len(name)/2
            j = 0
            print "dict_len", dict_len
            for i in range(dict_len, dict_len+5):
                singleton = results[j]
                print singleton
                for k, v in singleton.iteritems():
                    print k, v

                    if v not in temp_list:
                        key = str(i+1)
                        name[key] = v
                        num = 'M' + str(i+1)
                        name[num] = 1
                        temp_list.append(v)
                        temp_list.append(num)
                    else:
                        indx = temp_list.index(v)
                        there = temp_list[indx+1]
                        name[there] += 1
                        if others not in temp_list:
                            key = str(i+1)
                            name[key] = others
                            num = 'M' + str(i+1)
                            name[num] = 0
                            temp_list.append(others)
                    print temp_list
                j += 1
            if len(temp_list) <= 7:
                others = others + '1'
                name['15'] = others
                name['M15'] = 0 

def delete_keys():
    to_delete = ['11','12', '13', '14', '15', 'M11', 'M12', 'M13', 'M14', 'M15']
    length = len(name)
    if length > 20:
        loop = len(to_delete)
        for i in range(loop):
            if to_delete[i] in name:
                del name[to_delete[i]]
    print "Current dictionary length: ", len(name)
    print "Current dictionary: \n", name

app.run(debug=True)