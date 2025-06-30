print("Connecting to Redshift and Spawning Threads...")

import base64
import os
import threading
from io import BytesIO
import matplotlib
matplotlib.use('Agg')  # Use the 'Agg' backend which is thread-safe
import matplotlib.pyplot as plt
import statistics
from datetime import datetime, timedelta
import pytz
import washmetrix
from jinja2 import Environment, FileSystemLoader
import json
from colorama import init, Fore, Style
init()

# Set up logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Use the existing washmetrix setup
location_timezones = {
    '269529698b913dfb25f26ceace416fac': 'America/Chicago',  # Plainfield
    '718cfd435065af468cae6cfa655448dc': 'America/Chicago',  # Hub Office (Fed Hub | DRB [WSHUIL-HQ1])
    '441b92a084e0d23ac1f1efc1d4eb7367': 'America/Chicago',  # Query Server (ECOM | DRB [WSHUIL-HQ2])
    '827ccde4158d5b6917661c3dd369341f': 'America/Chicago',  # Villa Park (VILLA)
    'da4f80608864892504784372113ba322': 'America/Chicago',  # Burbank
    '46b724ec5bb89743f915bbe3ebf6b343': 'America/Chicago',  # Carol Stream (CAROL)
    '011f3e8d4c5c304c4a956de1eb89e75b': 'America/Chicago',  # Des Plaines (DESPLN)
    '88a2b3c8e7d1b7f1073d73de6b5a56f5': 'America/Chicago',  # Berwyn
    '39584e8cd2a8e19da2c9406faac47c2e': 'America/Chicago',  # Joliet
    '220e853565de0c739bb3da29e512ed18': 'America/Chicago',  # Naperville (NAPER)
    '8a656bc1f58397ff7e026a3076411420': 'America/Chicago',  # Evergreen
    '959558c4617dd2911a33de89489ed6b1': 'America/Chicago',  # Niles

    '5203385e07d8589c1b5d07da8865e015': 'America/Los_Angeles',  # Fiesta (FIESTA)
    'cf2257e6113a6298f68113a16929cad8': 'America/Los_Angeles',  # Centennial (CENT)
}

location_names = {
    'Plainfield':       '269529698b913dfb25f26ceace416fac',
    'Villa Park':       '827ccde4158d5b6917661c3dd369341f',
    'Burbank':          'da4f80608864892504784372113ba322',
    'Carol Stream':     '46b724ec5bb89743f915bbe3ebf6b343',
    'Des Plaines':      '011f3e8d4c5c304c4a956de1eb89e75b',
    'Berwyn':           '88a2b3c8e7d1b7f1073d73de6b5a56f5',
    'Joliet':           '39584e8cd2a8e19da2c9406faac47c2e',
    'Naperville':       '220e853565de0c739bb3da29e512ed18',
    'Evergreen Park':   '8a656bc1f58397ff7e026a3076411420',
    'Fiesta':           '5203385e07d8589c1b5d07da8865e015',
    'Centennial':       'cf2257e6113a6298f68113a16929cad8',
    'Niles':            '959558c4617dd2911a33de89489ed6b1',

}

location_managers = {
    'Plainfield':       'NTrujillo@washucarwash.com',
    'Villa Park':       'WSalkeld@washucarwash.com',
    'Burbank':          'RFlannigan@washucarwash.com',
    'Carol Stream':     'KVega@washucarwash.com',
    'Des Plaines':      'moustafa@washucarwash.com',
    'Berwyn':           'angelica@washucarwash.com',
    'Joliet':           'JNeedham@washucarwash.com',
    'Naperville':       'SSieg@washucarwash.com',
    'Evergreen Park':   'jjenkins@washucarwash.com',
    'Fiesta':           'antoniovega@washucarwash.com',
    'Centennial':       'ysneath@washucarwash.com',
    'Niles':            'ahasenjaeger@washucarwash.com'
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
    numeric_data = [float(x.replace('$', '').replace(',', '').replace('%', '')) if isinstance(x, str) else float(x) for x in data]
    
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
    conversion_rates = []
    while current_date <= today:
        # Set time range for the current day
        start_day = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = current_date.replace(hour=23, minute=59, second=59, microsecond=0)

        # Fetch daily totals
        daily_cars = washmetrix_api.total_cars(start_date=start_day, end_date=end_day, location_key=location_id)
        daily_sales = washmetrix_api.total_sales(start_date=start_day, end_date=end_day, location_key=location_id)
        conversion_rate = washmetrix_api.conversion_rate(start_date=start_day, end_date=end_day, location_key=location_id)*100

        # Update MTD totals
        mtd_total_cars += daily_cars
        mtd_total_sales += daily_sales
        conversion_rates.append(conversion_rate)

        # Store daily data with cumulative MTD totals
        daily_data.append({
            'date': current_date.strftime('%m-%d'),
            'cars': f"{int(daily_cars):,}",
            'sales': f"${daily_sales:,.2f}",
            'conversion_rate': f"{conversion_rate:,.2f}%",
            'mtd_total_cars': f"{int(mtd_total_cars):,}",
            'mtd_total_sales': f"${mtd_total_sales:,.2f}",
            'mtd_conversion_rate': f"{statistics.mean(conversion_rates):,.2f}%"
        })
        # Move to the next day
        current_date += timedelta(days=1)

    return daily_data

def generate_kpi_report(location_name, template_file_path):
    """
    Generate a KPI report for a specific location using washmetrix API data.
    
    Args:
        location_name (str): Name of the location
        template_file_path (str): Path to the Jinja2 template file
    
    Returns:
        str: Rendered HTML report
    """
    logger.info(f"Starting KPI report generation for {location_name}")
    
    def setup_dates():
        today = datetime.now(pytz.timezone('America/Chicago'))
        return {
            'today': today,
            'start_of_month': today.replace(day=1),
            'start_of_yesterday': today.replace(hour=0, minute=0, second=0),
            'end_of_yesterday': today.replace(hour=23, minute=59, second=59)
        }

    def validate_location(name):
        location_id = location_names.get(name)
        if not location_id:
            raise ValueError(f"Invalid location name: {name}")
        return location_id

    def fetch_kpi_data(dates, location_id):
        date_params = {
            'start_date': dates['start_of_yesterday'],
            'end_date': dates['end_of_yesterday'],
            'location_key': location_id
        }
        
        monthly_params = {
            'year': dates['today'].year,
            'month': dates['today'].month,
            'location_key': location_id
        }

        daily_metrics = {
            'total_cars': washmetrix_api.total_cars,
            'total_sales': washmetrix_api.total_sales,
            'blended_awp': washmetrix_api.blended_awp,
            'retail_cars': washmetrix_api.retail_car_count,
            'retail_awp': washmetrix_api.retail_awp,
            'redeemed_memberships': washmetrix_api.membership_redemptions,
            'new_memberships': washmetrix_api.new_memberships_sold,
            'cancelled_members': washmetrix_api.memberships_cancelled,
            'conversion_rate': (washmetrix_api.conversion_rate, 100),
            'redemption_rate': (washmetrix_api.membership_redemption_rate, 100),
            'labor_percentage': (washmetrix_api.labor_percentage, 100)
        }

        monthly_metrics = {
            'churn_rate': (washmetrix_api.churn_rate, 100),
            'growth_rate': (washmetrix_api.growth_rate, 100)
        }

        results = {}
        for metric_name, method in daily_metrics.items():
            if isinstance(method, tuple):
                method, multiplier = method
                value = method(**date_params)
                results[metric_name] = value * multiplier if value is not None else 0
            else:
                results[metric_name] = method(**date_params)

        for metric_name, (method, multiplier) in monthly_metrics.items():
            value = method(**monthly_params)
            results[metric_name] = value * multiplier if value is not None else 0

        return results

    def generate_sparklines(daily_data):
        metrics = ['cars', 'sales', 'conversion_rate', 'mtd_total_cars', 'mtd_total_sales', 'mtd_conversion_rate']
        colors = ['blue', 'green', 'red', 'purple', 'yellow', 'orange']
        
        return {
            f"{metric}_sparkline": generate_sparkline([day[metric] for day in daily_data], color)
            for metric, color in zip(metrics, colors)
        }

    def prepare_context(dates, kpi_data, daily_data, sparklines):
        format_specs = {
            'int': lambda x: '{:,}'.format(int(x)),
            'float': lambda x: '{:,.2f}'.format(float(x)),
            'float2': lambda x: '{:.2f}'.format(float(x))
        }

        metrics_format = {
            'total_cars': 'int',
            'total_sales': 'float',
            'blended_awp': 'float',
            'retail_cars': 'int',
            'retail_awp': 'float',
            'redeemed_memberships': 'int',
            'new_memberships': 'int',
            'cancelled_members': 'int',
            'conversion_rate': 'float2',
            'redemption_rate': 'float2',
            'labor_percentage': 'float2',
            'churn_rate': 'float2',
            'growth_rate': 'float2'
        }

        formatted_data = {
            key: format_specs[metrics_format[key]](value)
            for key, value in kpi_data.items()
            if key in metrics_format
        }

        return {
            'location_name': location_name,
            'report_date': dates['today'].strftime("%Y-%m-%d"),
            **formatted_data,
            'daily_data': daily_data,
            **sparklines
        }

    try:
        # Main execution flow
        dates = setup_dates()
        logger.debug(f"Date range: {dates['start_of_yesterday']} to {dates['end_of_yesterday']}")

        location_id = validate_location(location_name)
        logger.debug(f"Location ID: {location_id}")

        kpi_data = fetch_kpi_data(dates, location_id)
        logger.debug("KPI data fetched successfully")

        print(f"{Fore.CYAN}[INFO] {Fore.GREEN}Fetching daily and MTD data for {Fore.YELLOW}{location_name}{Fore.GREEN}...{Style.RESET_ALL}")
        daily_data = get_daily_and_mtd_data(location_id, dates['start_of_month'], dates['today'])
        logger.debug(f"Daily data entries: {len(daily_data)}")

        sparklines = generate_sparklines(daily_data)
        logger.debug("Sparklines generated successfully")

        context = prepare_context(dates, kpi_data, daily_data, sparklines)
        logger.debug("Context prepared successfully")

        # Render template
        logger.info("Rendering Jinja2 template")
        env = Environment(loader=FileSystemLoader(os.path.dirname(template_file_path)))
        template = env.get_template(os.path.basename(template_file_path))
        rendered_report = template.render(context)
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
    template_file_path = r"C:\Users\washu\Chris\Production\nightly-report-GMs\template.html"

    print(f"{Fore.CYAN}[INFO] {Fore.GREEN}New Thread Created for {Fore.YELLOW}{location}{Fore.GREEN}...{Style.RESET_ALL}")

    report = generate_kpi_report(location_name, template_file_path)

    def is_past_830pm(timezone_str):
        location_tz = pytz.timezone(timezone_str)
        current_time = datetime.now(location_tz)
        print(current_time, location_tz)
        return current_time.hour >= 20 and current_time.minute >= 20

    def send_file_to_power_automate(report, url, location):
        location_id = location_names.get(location)
        if not location_id:
            print(f"Unknown location: {location}")
            return

        timezone_str = location_timezones.get(location_id)
        if not timezone_str:
            print(f"No timezone found for location: {location}")
            return

        timeout = 0
        while is_past_830pm(timezone_str) == False:
            print(f"Not sending file for {location}. {location}'s Time: {datetime.now(pytz.timezone(timezone_str))}")
            print(f"[ATTEMPT(S): {timeout+1}] Waiting an hour before trying again. Do not close this window or the report will not be sent...")
            timeout += 1
            if timeout > 5:
                logger.error(f"Unable to send out a report for the location: {location}. We waited 5 hours and still did not meet the time condition to send the report.")
                break
            time.sleep(3600)

        if timeout <= 5:
            # Prepare the payload
            payload = {
                'emailContent': report,
                'subject': f"{location} End of Day Report: {datetime.now().strftime('%Y-%m-%d')}",
                'location': location,
                'generalManagerEmail': location_managers.get(location, "No manager email found")
            }

            logger.debug(f"Sending out a report for {location}.")

            # Send the POST request
            response = requests.post(url, json=payload)

            # Print the response from the server
            print(f"{location}: Status Code - {response.status_code}")
            print(f"{location}: Response - {response.text}")

    # Example of how to call send_file_to_power_automate
    power_automate_url = "https://prod-148.westus.logic.azure.com:443/workflows/b8fc45168e654c988a50c8560a77fc4f/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=JALTLF2jAkgSFe5Zujplz2coX5Vt_avUuVqoEqlp_8Y"
    send_file_to_power_automate(report, power_automate_url, location)
        

    # Save the report to an HTML file
    output_file = f"reports/{location_name}/{location_name}_KPI_Report_{datetime.now().strftime('%Y-%m-%d')}.html"
    logger.debug("Saving file to reports folder...")
    # Ensure the 'reports' directory exists
    os.makedirs(f'reports/{location_name}', exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(report)

    logger.debug(f"Report saved to: {output_file}")

washmetrix_api.total_cars(start_date=datetime.today(), end_date=datetime.today(), location_key="269529698b913dfb25f26ceace416fac") # Init the logger for the API so log is clean for this application.

# Create and start threads for each location
threads = []
for location in location_names:
    thread = threading.Thread(target=process_location, args=(location,))
    threads.append(thread)
    time.sleep(3)
    thread.start()

time.sleep(1)
print('-'*100)

# Wait for all threads to complete
for thread in threads:
    thread.join()

print("All reports generated, sent and saved.")
time.sleep(10)
#print(f"KPI report for {location_name} has been generated and saved as {output_file}")
