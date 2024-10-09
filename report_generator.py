import base64
from io import BytesIO
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pytz
import washmetrix
from jinja2 import Environment, FileSystemLoader

# Use the existing washmetrix setup
location_timezones = {
    '269529698b913dfb25f26ceace416fac': 'America/Chicago',  # Plainfield
    '718cfd435065af468cae6cfa655448dc': 'America/Chicago',  # Hub Office (Fed Hub | DRB [WSHUIL-HQ1])
    '441b92a084e0d23ac1f1efc1d4eb7367': 'America/Chicago',  # Query Server (ECOM | DRB [WSHUIL-HQ2])
    '57a752239ecde15d76f4d0710a408e9f': 'America/Chicago',  # Villa Park
    'da4f80608864892504784372113ba322': 'America/Chicago',  # Burbank
    '0be9301df5b11c389b4214d48d7e7935': 'America/Chicago',  # Carol Stream
    '88f23b2f258753f3a53cc223727a9189': 'America/Chicago',  # Des Plaines
    '88a2b3c8e7d1b7f1073d73de6b5a56f5': 'America/Chicago',  # Berwyn
    '39584e8cd2a8e19da2c9406faac47c2e': 'America/Chicago',  # Joliet
    'd9febfa73cd20efaf95b28ff30d8e050': 'America/Chicago',  # Naperville
    '8a656bc1f58397ff7e026a3076411420': 'America/Chicago',  # Evergreen
    '5203385e07d8589c1b5d07da8865e015': 'America/Los_Angeles',  # Fiesta
    'cf2257e6113a6298f68113a16929cad8': 'America/Los_Angeles',  # Centennial (CENT)
    '1bd5671fa94b8023846b2935f590cc25': 'America/Los_Angeles',  # Centennial (SITE2)
    '04814323fd60ab73484bf25cce5a2d68': 'America/Los_Angeles',  # Centennial (DRB | WSHUIL-101)
}

location_names = {
    'Plainfield':       '269529698b913dfb25f26ceace416fac',
    'Villa Park':       '57a752239ecde15d76f4d0710a408e9f',
    'Burbank':          'da4f80608864892504784372113ba322',
    'Carol Stream':     '0be9301df5b11c389b4214d48d7e7935',
    'Des Plaines':      '88f23b2f258753f3a53cc223727a9189',
    'Berwyn':           '88a2b3c8e7d1b7f1073d73de6b5a56f5',
    'Joliet':           '39584e8cd2a8e19da2c9406faac47c2e',
    'Naperville':       'd9febfa73cd20efaf95b28ff30d8e050',
    'Evergreen Park':   '8a656bc1f58397ff7e026a3076411420',
    'Fiesta':           '5203385e07d8589c1b5d07da8865e015',
    'Centennial':       'cf2257e6113a6298f68113a16929cad8',
}

location_managers = {
    'Plainfield':       'Julia@washucarwash.com',
    'Villa Park':       'WSalkeld@washucarwash.com',
    'Burbank':          'RFlannigan@washucarwash.com',
    'Carol Stream':     'nathan.nocerino@washucarwash.com',
    'Des Plaines':      'moustafa@washucarwash.com',
    'Berwyn':           'angelica@washucarwash.com',
    'Joliet':           'GThompson@washucarwash.com',
    'Naperville':       'ahasenjaeger@washucarwash.com',
    'Evergreen Park':   'jjenkins@washucarwash.com',
    'Fiesta':           'antoniovega@washucarwash.com',
    'Centennial':       'ysneath@washucarwash.com',
}

washmetrix_api = washmetrix.WashMetrixKPIs(pytz.timezone('America/Chicago'), location_timezones)

def generate_sparkline(data, color='blue'):
    # Convert data to float, removing '$' if present
    numeric_data = [float(x.replace('$', '')) if isinstance(x, str) else float(x) for x in data]
    
    fig, ax = plt.subplots(figsize=(2, 0.5))
    ax.plot(numeric_data, color=color)
    ax.axis('off')
    ax.set_ylim(min(numeric_data) * 0.9, max(numeric_data) * 1.1)  # Add some padding
    
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=100, bbox_inches='tight', pad_inches=0)
    buffer.seek(0)
    image_png = buffer.getvalue()
    graph = base64.b64encode(image_png).decode('utf-8')
    plt.close(fig)
    
    return f"data:image/png;base64,{graph}"

def get_7day_kpi_data(location_id, end_date):
    start_date = end_date - timedelta(days=6)
    daily_data = []

    current_date = start_date
    while current_date <= end_date:
        start_day = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = current_date.replace(hour=23, minute=59, second=59, microsecond=0)

        daily_data.append({
            'date': current_date.strftime('%Y-%m-%d'),
            'cars': int(washmetrix_api.total_cars(start_date=start_day, end_date=end_day, location_key=location_id)),
            'sales': round(washmetrix_api.total_sales(start_date=start_day, end_date=end_day, location_key=location_id), 2),
            'blended_awp': round(washmetrix_api.blended_awp(start_date=start_day, end_date=end_day, location_key=location_id), 2),
            'retail_cars': int(washmetrix_api.retail_car_count(start_date=start_day, end_date=end_day, location_key=location_id)),
            'retail_awp': round(washmetrix_api.retail_awp(start_date=start_day, end_date=end_day, location_key=location_id), 2),
            'redeemed_memberships': int(washmetrix_api.membership_redemptions(start_date=start_day, end_date=end_day, location_key=location_id)),
            'new_memberships': int(washmetrix_api.new_memberships_sold(start_date=start_day, end_date=end_day, location_key=location_id)),
            'cancelled_members': int(washmetrix_api.memberships_cancelled(start_date=start_day, end_date=end_day, location_key=location_id)),
            'conversion_rate': round(washmetrix_api.conversion_rate(start_date=start_day, end_date=end_day, location_key=location_id) * 100, 2),
        })
        current_date += timedelta(days=1)

    return daily_data

def get_daily_and_mtd_data(location_id, start_of_month, today):
    """
    Fetch total cars and net sales for each day up to 'today', along with cumulative MTD totals.
    """
    daily_data = []
    mtd_total_cars = 0
    mtd_total_sales = 0.0

    # Loop through each day from the start of the month to today
    current_date = start_of_month
    while current_date <= today:
        # Set time range for the current day
        start_day = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = current_date.replace(hour=23, minute=59, second=59, microsecond=0)

        # Fetch daily totals
        daily_cars = washmetrix_api.total_cars(start_date=start_day, end_date=end_day, location_key=location_id)
        daily_sales = washmetrix_api.total_sales(start_date=start_day, end_date=end_day, location_key=location_id)

        # Update MTD totals
        mtd_total_cars += daily_cars
        mtd_total_sales += daily_sales

        # Store daily data with cumulative MTD totals
        daily_data.append({
            'date': current_date.strftime('%m-%d'),
            'cars': int(daily_cars),
            'sales': f"${daily_sales:.2f}",
            'mtd_total_cars': int(mtd_total_cars),
            'mtd_total_sales': f"${mtd_total_sales:.2f}"
        })

        # Move to the next day
        current_date += timedelta(days=1)

    return daily_data

def generate_kpi_report(location_name, template_file):
    # Set up dates
    today = datetime.now(pytz.timezone('America/Chicago')) - timedelta(days=1)
    start_of_month = today.replace(day=1)
    start_of_yesterday = today.replace(hour=0, minute=0, second=0)
    end_of_yesterday = today.replace(hour=23, minute=59, second=59)

    # Get location ID
    location_id = location_names.get(location_name)
    if not location_id:
        raise ValueError(f"Invalid location name: {location_name}")

    # Fetch data using washmetrix API functions for the main KPIs
    total_cars = washmetrix_api.total_cars(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id)
    total_sales = washmetrix_api.total_sales(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id)
    blended_awp = washmetrix_api.blended_awp(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id)
    retail_cars = washmetrix_api.retail_car_count(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id)
    retail_awp = washmetrix_api.retail_awp(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id)
    redeemed_memberships = washmetrix_api.membership_redemptions(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id)
    new_memberships = washmetrix_api.new_memberships_sold(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id)
    cancelled_members = washmetrix_api.memberships_cancelled(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id)
    redemption_rate = washmetrix_api.membership_redemption_rate(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id) * 100
    labor_percentage = washmetrix_api.labor_percentage(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id) * 100
    churn_rate = washmetrix_api.churn_rate(year=today.year, month=today.month, location_key=location_id) * 100
    growth_rate = washmetrix_api.growth_rate(year=today.year, month=today.month, location_key=location_id) * 100
    conversion_rate = washmetrix_api.conversion_rate(start_date=start_of_yesterday, end_date=end_of_yesterday, location_key=location_id) * 100

    # Get daily and MTD data for the table
    daily_data = get_daily_and_mtd_data(location_id, start_of_month, today)

    # Generate sparklines for each KPI
    cars_data = [day['cars'] for day in daily_data]
    sales_data = [day['sales'] for day in daily_data]
    mtd_cars_data = [day['mtd_total_cars'] for day in daily_data]
    mtd_sales_data = [day['mtd_total_sales'] for day in daily_data]

    cars_sparkline = generate_sparkline(cars_data, 'blue')
    sales_sparkline = generate_sparkline(sales_data, 'green')
    mtd_cars_sparkline = generate_sparkline(mtd_cars_data, 'red')
    mtd_sales_sparkline = generate_sparkline(mtd_sales_data, 'purple')

    # Prepare the context for Jinja2 template rendering
    context = {
        'location_name': location_name,
        'report_date': today.strftime("%Y-%m-%d"),
        'total_cars': str(total_cars),
        'total_sales': f"{total_sales:.2f}",
        'blended_awp': f"{blended_awp:.2f}",
        'retail_cars': str(retail_cars),
        'retail_awp': f"{retail_awp:.2f}",
        'redeemed_memberships': str(redeemed_memberships),
        'new_memberships': str(new_memberships),
        'cancelled_memberships': str(cancelled_members),
        'conversion_rate': f"{conversion_rate:.2f}",
        'redemption_rate': f"{redemption_rate:.2f}",
        'labor_percentage': f"{labor_percentage:.2f}",
        'churn_rate': f"{churn_rate:.2f}",
        'growth_rate': f"{growth_rate:.2f}",
        'daily_data': daily_data,
        'daily_data': daily_data,
        'cars_sparkline': cars_sparkline,
        'sales_sparkline': sales_sparkline,
        'mtd_cars_sparkline': mtd_cars_sparkline,
        'mtd_sales_sparkline': mtd_sales_sparkline,
    }

    # Set up Jinja2 environment and render the template
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template(template_file)
    rendered_report = template.render(context)

    return rendered_report

# Send the data to the masses
import requests
import base64
for location in location_names:
    location_name = location
    template_file = "template.html"

    print(f"Generating report for {location}...")

    report = generate_kpi_report(location_name, template_file)

    
    def send_file_to_power_automate(report, url):
        # Prepare the payload
        payload = {
            'emailContent': report,
            'subject': f"{location_name} KPI Report {datetime.now().strftime('%Y-%m-%d')}",
            'location': location,
            'generalManagerEmail': location_managers[location]
        }

        # Send the POST request
        response = requests.post(url, json=payload)

        # Print the response from the server
        print(response.status_code)
        print(response.text)

    # Example of how to call send_file_to_power_automate
    send_file_to_power_automate(report, "https://prod-148.westus.logic.azure.com:443/workflows/b8fc45168e654c988a50c8560a77fc4f/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=JALTLF2jAkgSFe5Zujplz2coX5Vt_avUuVqoEqlp_8Y")
    

    '''
    # Save the report to an HTML file
    output_file = f"{location_name}_KPI_Report_{datetime.now().strftime('%Y-%m-%d')}.html"
    with open(output_file, 'w') as file:
        file.write(report)
    '''
#print(f"KPI report for {location_name} has been generated and saved as {output_file}")
