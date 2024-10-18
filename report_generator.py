import base64
import os
import threading
from io import BytesIO
import matplotlib
matplotlib.use('Agg')  # Use the 'Agg' backend which is thread-safe
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pytz
import washmetrix
from jinja2 import Environment, FileSystemLoader
import json

# Set up logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    'Plainfield':       'NTrujillo@washucarwash.com',
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

# Thread-local storage for Matplotlib
thread_local = threading.local()

def get_matplotlib_fig():
    if not hasattr(thread_local, 'fig'):
        thread_local.fig, thread_local.ax = plt.subplots(figsize=(2, 0.5))
    return thread_local.fig, thread_local.ax

def generate_sparkline(data, color='blue'):
    # Convert data to float, removing '$' and ',' if present
    numeric_data = [float(x.replace('$', '').replace(',', '')) if isinstance(x, str) else float(x) for x in data]
    
    fig, ax = get_matplotlib_fig()
    ax.clear()  # Clear previous plot
    ax.plot(numeric_data, color=color)
    ax.axis('off')
    ax.set_ylim(min(numeric_data) * 0.9, max(numeric_data) * 1.1)  # Add some padding
    
    buffer = BytesIO()
    fig.savefig(buffer, format='png', dpi=100, bbox_inches='tight', pad_inches=0)
    buffer.seek(0)
    image_png = buffer.getvalue()
    graph = base64.b64encode(image_png).decode('utf-8')
    
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
            'cars': f"{int(washmetrix_api.total_cars(start_date=start_day, end_date=end_day, location_key=location_id)):,}",
            'sales': f"{washmetrix_api.total_sales(start_date=start_day, end_date=end_day, location_key=location_id):,.2f}",
            'blended_awp': f"{washmetrix_api.blended_awp(start_date=start_day, end_date=end_day, location_key=location_id):,.2f}",
            'retail_cars': f"{int(washmetrix_api.retail_car_count(start_date=start_day, end_date=end_day, location_key=location_id)):,}",
            'retail_awp': f"{washmetrix_api.retail_awp(start_date=start_day, end_date=end_day, location_key=location_id):,.2f}",
            'redeemed_memberships': f"{int(washmetrix_api.membership_redemptions(start_date=start_day, end_date=end_day, location_key=location_id)):,}",
            'new_memberships': f"{int(washmetrix_api.new_memberships_sold(start_date=start_day, end_date=end_day, location_key=location_id)):,}",
            'cancelled_members': f"{int(washmetrix_api.memberships_cancelled(start_date=start_day, end_date=end_day, location_key=location_id)):,}",
            'conversion_rate': f"{washmetrix_api.conversion_rate(start_date=start_day, end_date=end_day, location_key=location_id) * 100:.2f}"
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
            'cars': f"{int(daily_cars):,}",
            'sales': f"${daily_sales:,.2f}",
            'mtd_total_cars': f"{int(mtd_total_cars):,}",
            'mtd_total_sales': f"${mtd_total_sales:,.2f}"
        })
        # Move to the next day
        current_date += timedelta(days=1)

    return daily_data

def generate_kpi_report(location_name, template_file):
    logger.info(f"Starting KPI report generation for {location_name}")
    try:
        # Set up dates
        today = datetime.now(pytz.timezone('America/Chicago')) - timedelta(days=1)
        start_of_month = today.replace(day=1)
        start_of_yesterday = today.replace(hour=0, minute=0, second=0)
        end_of_yesterday = today.replace(hour=23, minute=59, second=59)
        logger.debug(f"Date range: {start_of_yesterday} to {end_of_yesterday}")

        # Get location ID
        location_id = location_names.get(location_name)
        if not location_id:
            logger.error(f"Invalid location name: {location_name}")
            raise ValueError(f"Invalid location name: {location_name}")
        logger.debug(f"Location ID: {location_id}")

        # Fetch data using washmetrix API functions for the main KPIs
        logger.info("Fetching KPI data from washmetrix API")
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
        logger.debug("KPI data fetched successfully")

        # Get daily and MTD data for the table
        logger.info("Fetching daily and MTD data")
        daily_data = get_daily_and_mtd_data(location_id, start_of_month, today)
        logger.debug(f"Daily data entries: {len(daily_data)}")

        # Generate sparklines for each KPI
        logger.info("Generating sparklines")
        cars_data = [day['cars'] for day in daily_data]
        sales_data = [day['sales'] for day in daily_data]
        mtd_cars_data = [day['mtd_total_cars'] for day in daily_data]
        mtd_sales_data = [day['mtd_total_sales'] for day in daily_data]

        cars_sparkline = generate_sparkline(cars_data, 'blue')
        sales_sparkline = generate_sparkline(sales_data, 'green')
        mtd_cars_sparkline = generate_sparkline(mtd_cars_data, 'red')
        mtd_sales_sparkline = generate_sparkline(mtd_sales_data, 'purple')
        logger.debug("Sparklines generated successfully")

        # Prepare the context for Jinja2 template rendering
        logger.info("Preparing context for template rendering")
        context = {
            'location_name': location_name,
            'report_date': today.strftime("%Y-%m-%d"),
            'total_cars': '{:,}'.format(int(total_cars)),
            'total_sales': '{:,.2f}'.format(float(total_sales)),
            'blended_awp': '{:,.2f}'.format(float(blended_awp)),
            'retail_cars': '{:,}'.format(int(retail_cars)),
            'retail_awp': '{:,.2f}'.format(float(retail_awp)),
            'redeemed_memberships': '{:,}'.format(int(redeemed_memberships)),
            'new_memberships': '{:,}'.format(int(new_memberships)),
            'cancelled_memberships': '{:,}'.format(int(cancelled_members)),
            'conversion_rate': '{:.2f}'.format(float(conversion_rate)),
            'redemption_rate': '{:.2f}'.format(float(redemption_rate)),
            'labor_percentage': '{:.2f}'.format(float(labor_percentage)),
            'churn_rate': '{:.2f}'.format(float(churn_rate)),
            'growth_rate': '{:.2f}'.format(float(growth_rate)),
            'daily_data': daily_data,
            'cars_sparkline': cars_sparkline,
            'sales_sparkline': sales_sparkline,
            'mtd_cars_sparkline': mtd_cars_sparkline,
            'mtd_sales_sparkline': mtd_sales_sparkline,
        }
        logger.debug("Context prepared successfully")
        print(json.dumps(context, indent=4))

        # Set up Jinja2 environment and render the template
        logger.info("Rendering Jinja2 template")
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template(template_file)
        rendered_report = template.render(context)
        logger.info("Template rendered successfully")

        logger.info(f"KPI report generation completed for {location_name}")
        return rendered_report

    except Exception as e:
        logger.error(f"Error generating KPI report for {location_name}: {str(e)}", exc_info=True)
        raise

# Send the data to the masses
import requests
import base64
import time
def process_location(location):
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
        print(f"{location}: Status Code - {response.status_code}")
        print(f"{location}: Response - {response.text}")

    # Example of how to call send_file_to_power_automate
    # Uncomment the following line when ready to use
    send_file_to_power_automate(report, "https://prod-148.westus.logic.azure.com:443/workflows/b8fc45168e654c988a50c8560a77fc4f/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=JALTLF2jAkgSFe5Zujplz2coX5Vt_avUuVqoEqlp_8Y")

    # Save the report to an HTML file
    output_file = f"reports/{location_name}/{location_name}_KPI_Report_{datetime.now().strftime('%Y-%m-%d')}.html"

    # Ensure the 'reports' directory exists
    os.makedirs(f'reports/{location_name}', exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(report)

    print(f"Report saved to: {output_file}")

# Create and start threads for each location
threads = []
for location in location_names:
    thread = threading.Thread(target=process_location, args=(location,))
    threads.append(thread)
    time.sleep(3)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

print("All reports generated and saved.")
    
#print(f"KPI report for {location_name} has been generated and saved as {output_file}")
