import pandas as pd
import psycopg2
import json
import pytz
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import washmetrix

location_timezones = {
    '269529698b913dfb25f26ceace416fac': 'America/Chicago', # Plainfield
    '718cfd435065af468cae6cfa655448dc': 'America/Chicago', # Hub Office (Fed Hub | DRB [WSHUIL-HQ1])
    '441b92a084e0d23ac1f1efc1d4eb7367': 'America/Chicago', # Query Server (ECOM | DRB [WSHUIL-HQ2])
    '57a752239ecde15d76f4d0710a408e9f': 'America/Chicago', # Villa Park
    'da4f80608864892504784372113ba322': 'America/Chicago', # Burbank
    '0be9301df5b11c389b4214d48d7e7935': 'America/Chicago', # Carol Stream
    '88f23b2f258753f3a53cc223727a9189': 'America/Chicago', # Des Plaines
    '88a2b3c8e7d1b7f1073d73de6b5a56f5': 'America/Chicago', # Berwyn
    '39584e8cd2a8e19da2c9406faac47c2e': 'America/Chicago', # Joliet
    'd9febfa73cd20efaf95b28ff30d8e050': 'America/Chicago', # Naperville
    '8a656bc1f58397ff7e026a3076411420': 'America/Chicago', # Evergreen

    '5203385e07d8589c1b5d07da8865e015': 'America/Los_Angeles', # Fiesta
    'cf2257e6113a6298f68113a16929cad8': 'America/Los_Angeles', # Centennial (CENT)
    '1bd5671fa94b8023846b2935f590cc25': 'America/Los_Angeles', # Centennial (SITE2)
    '04814323fd60ab73484bf25cce5a2d68': 'America/Los_Angeles', # Centennial (DRB | WSHUIL-101)
}

washmetrix = washmetrix.WashMetrixKPIs(pytz.timezone('America/Chicago'), location_timezones) # Pass local-machine timezone and the location_timezones dict to API upon init.

today = datetime.today() - timedelta(days=1)
start_date = today.replace(hour=0, minute=0, second=0, microsecond=0)  # Today at 00:00:00
end_date = today.replace(hour=23, minute=59, second=59, microsecond=0)  # Today at 23:59:59

# Call functions
print(washmetrix.membership_recharge_income_and_count(start_date=start_date, end_date=end_date, location_key='269529698b913dfb25f26ceace416fac'))
print(washmetrix.churn_rate(year=2024, month=8, location_key='269529698b913dfb25f26ceace416fac'))
print(washmetrix.growth_rate(year=2024, month=8, location_key='269529698b913dfb25f26ceace416fac'))