# IronSwallow

IronSwallow consumes National Rail's Darwin push port for passenger rail data,
with a RESTful API intended to offer a practical (and much more generously
billed) alternative to the official SOAP LDB(SV)WS that National Rail also offers

## Licence
This project is licenced under the GNU GPL, version 3 (for now)

## Setup
In order to use IronSwallow, you'll need a National Rail open data account
(sign up [here](https://opendata.nationalrail.co.uk/)). Make sure Darwin is
enabled in your account, copy `secret.json.example` to `secret.json`, and fill
in the credentials there. This may be a little tedious.

Make sure you've got a PostgreSQL database on hand, fill in the connection
string in the secret file.

To initialise the database, use `psql -f structure.sql database_name_goes_here`

If you don't already have the dependencies, installing them might be useful
(`pip3 install --user -r requirements.txt`)

There's presently two components you may wish to run. The first is the consumer
(`./iron-swallow.py`), and the second is the flask app which provides the REST
and web endpoints (`./web.py`)

## Dependencies
* [psycopg2](https://pypi.org/project/psycopg2/)
* [stomp.py](https://pypi.org/project/stomp.py/)
* [boto3](https://pypi.org/project/boto3/)
* [pytz](https://pypi.org/project/pytz/)
