"""
WashMetrix Redshift API
Version 0.6
Author: Chris Nance for WashU Carwash

This API standardizes the way KPIs are pulled via SQL for reports from a Redshift database.
It now supports a location-to-timezone mapping for flexible timezone handling.

Requirements:
- Whitelisted on Redshift
- psycopg2
- logging
- pytz
- datetime

Usage:
    import washmetrix
    from datetime import datetime, timedelta
    import pytz

    # Define location to timezone mapping
    location_timezones = {
        'location_key_1': 'America/Chicago',
        'location_key_2': 'America/Los_Angeles',
        # Add more locations as needed
    }

    # Initialize the KPI module with your local timezone and location mapping
    local_tz = pytz.timezone('America/Chicago')
    wash_metrics = washmetrix.WashMetrixKPIs(local_tz, location_timezones)

    # Define the dynamic start and end dates for today
    today = datetime.now(local_tz)
    start_date = today.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = today.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Call functions
    total_cars = wash_metrics.total_cars(start_date=start_date, end_date=end_date, location_key="location_key_1")

    # Don't forget to close the connection when done
    wash_metrics.close()
"""

import psycopg2
import logging
import pytz
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Union, Tuple

class WashMetrixKPIs:



    # =======================================
    # ||        CLASS FUNCTIONS            ||
    # =======================================



    def __init__(self, local_timezone: pytz.timezone = pytz.timezone('America/Chicago'), location_timezones: Dict[str, str] = None):
        """
        Initialize WashMetrixKPIs with Redshift connection details, local timezone, and location timezone mapping.

        Args:
            local_timezone (pytz.timezone): The local timezone for the user. Defaults to America/Chicago.
            location_timezones (Dict[str, str]): A dictionary mapping location keys to timezone strings.
        """
        self.host = "washu-redshift-cluster-1.cw0kwxsso29t.us-east-2.redshift.amazonaws.com"
        self.dbname = "dev"
        self.user = "washuadmin"
        self.password = "WashYou123!"
        self.port = "5439"
        self.connection = None
        self.logger = self._setup_logger()
        self.local_timezone = local_timezone
        self.location_timezones = self._process_location_timezones(location_timezones)
        self.schema = "dev.wash_u"  # Add this line to store the schema name

    def _process_location_timezones(self, location_timezones: Dict[str, str]) -> Dict[str, pytz.timezone]:
        """
        Process the location timezone dictionary to convert timezone strings to pytz timezone objects.

        Args:
            location_timezones (Dict[str, str]): A dictionary mapping location keys to timezone strings.

        Returns:
            Dict[str, pytz.timezone]: A dictionary mapping location keys to pytz timezone objects.
        """
        if not location_timezones:
            return {}
        
        processed_timezones = {}
        for location_key, timezone_str in location_timezones.items():
            try:
                processed_timezones[location_key] = pytz.timezone(timezone_str)
            except pytz.exceptions.UnknownTimeZoneError:
                self.logger.warning(f"Unknown timezone '{timezone_str}' for location '{location_key}'. Using UTC.")
                processed_timezones[location_key] = pytz.UTC
        
        return processed_timezones

    def _setup_logger(self) -> logging.Logger:
        """Set up and return a logger for the class."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def connect(self) -> None:
        """Establish a connection to the Redshift database."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.logger.info("Connection to Redshift successful")
        except Exception as e:
            self.logger.error(f"Failed to connect to Redshift: {e}")
            raise

    def execute_query(self, query: str, params: list = None) -> list:
        """Execute a query and return the results."""
        if not self.connection:
            self.connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query, params)
                return cursor.fetchall()
            except Exception as e:
                self.logger.error(f"Query execution failed: {e}")
                self.logger.error(f"Query: {query}")
                self.logger.error(f"Params: {params}")
                raise

    def _convert_to_utc(self, local_time: datetime, location_key: Optional[str] = None) -> str:
        """
        Convert local time to UTC and format it as 'YYYY-MM-DD HH:MM:SS'.

        Args:
            local_time (datetime): The local time to convert.
            location_key (str, optional): The key of the location for timezone lookup.

        Returns:
            str: UTC time formatted as 'YYYY-MM-DD HH:MM:SS'.
        """
        if local_time.tzinfo is None:
            if location_key and location_key in self.location_timezones:
                local_time = self.location_timezones[location_key].localize(local_time)
            else:
                local_time = self.local_timezone.localize(local_time)
        utc_time = local_time.astimezone(pytz.utc)
        # print(utc_time.strftime('%Y-%m-%d %H:%M:%S'), utc_time.timestamp())
        return utc_time.strftime('%Y-%m-%d %H:%M:%S')
    
    def _process_location_timezones(self, location_timezones: Dict[str, str]) -> Dict[str, pytz.timezone]:
        """
        Process the location timezone dictionary to convert timezone strings to pytz timezone objects.

        Args:
            location_timezones (Dict[str, str]): A dictionary mapping location keys to timezone strings.

        Returns:
            Dict[str, pytz.timezone]: A dictionary mapping location keys to pytz timezone objects.
        """
        if not location_timezones:
            return {}
        
        processed_timezones = {}
        for location_key, timezone_str in location_timezones.items():
            try:
                processed_timezones[location_key] = pytz.timezone(timezone_str)
            except pytz.exceptions.UnknownTimeZoneError:
                self.logger.warning(f"Unknown timezone '{timezone_str}' for location '{location_key}'. Using UTC.")
                processed_timezones[location_key] = pytz.UTC
        
        return processed_timezones

    def _generate_time_condition(self, field_name: str, start_date: str, end_date: str) -> str:
        """
        Generate SQL condition for time range, accounting for different location timezones.

        Args:
            field_name (str): The name of the timestamp field in the query.
            start_date (str): Start date in UTC.
            end_date (str): End date in UTC.

        Returns:
            str: SQL condition string for the time range.
        """
        conditions = []
        for location_key, timezone in self.location_timezones.items():
            conditions.append(f"""
                (ticket.location_key = '{location_key}' AND
                 {field_name} >= '{start_date}'::timestamp AT TIME ZONE '{timezone}' AND
                 {field_name} < '{end_date}'::timestamp AT TIME ZONE '{timezone}')
            """)
        
        # Add a default condition for locations not in the mapping
        conditions.append(f"""
            (ticket.location_key NOT IN ({', '.join(f"'{key}'" for key in self.location_timezones.keys())}) AND
             {field_name} >= '{start_date}'::timestamp AND
             {field_name} < '{end_date}'::timestamp)
        """)
        
        return " OR ".join(conditions)
    
    
    
    # =======================================
    # ||         API FUNCTIONS             ||
    # =======================================

    def total_sales(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, location_key: Optional[str] = None) -> float:
        """
        Get total sales for a given date range.

        Args:
            start_date (datetime, optional): Start date and time for the query.
            end_date (datetime, optional): End date and time for the query.
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            float: Total sales in the specified date range and location.
        """
        if not start_date:
            start_date = datetime.now(self.local_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        if not end_date:
            end_date = start_date + timedelta(days=1)

        utc_start_date = self._convert_to_utc(start_date, location_key)
        utc_end_date = self._convert_to_utc(end_date, location_key)

        # Query for total sales
        sales_query = f"""
            SELECT COALESCE(SUM(net), 0) as total_sales
            FROM {self.schema}.ticket
            WHERE transaction_date_time BETWEEN %s AND %s
            {f"AND location_key = %s" if location_key else ""}
        """

        try:
            # Execute sales query
            sales_params = [utc_start_date, utc_end_date]
            if location_key:
                sales_params.append(location_key)
            sales_result = self.execute_query(sales_query, sales_params)
            total_sales = sales_result[0][0] if sales_result else 0

            return round(total_sales, 2)

        except psycopg2.Error as e:
            self.logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise

    def total_cars(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, location_key: Optional[str] = None) -> int:
        """
        Get the total number of cars that passed through in a date range.

        Args:
            start_date (datetime, optional): Start date and time for the query. Defaults to today at 00:00:00 in the local timezone.
            end_date (datetime, optional): End date and time for the query. Defaults to start_date + 1 day.
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            int: Total number of cars washed in the specified date range and location.
        """
        if not start_date:
            start_date = datetime.now(self.local_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        if not end_date:
            end_date = start_date + timedelta(days=1)

        utc_start_date = self._convert_to_utc(start_date, location_key)
        utc_end_date = self._convert_to_utc(end_date, location_key)

        location_filter = f"AND ticket.location_key = '{location_key}'" if location_key else ""
        time_condition = self._generate_time_condition("ticket.transaction_date_time", utc_start_date, utc_end_date)

        query = f"""
        WITH ticket_data AS (
            SELECT ticket.location_key, ticket.transaction_type, ticket.count_as_car, ticket.net, location.source_type
            FROM dev.wash_u.ticket AS ticket
            JOIN dev.wash_u.location AS location ON ticket.location_key = location.key
            WHERE ({time_condition})
            {location_filter}
        )
        SELECT COUNT(CASE WHEN ticket_data.count_as_car = TRUE THEN 1 END) AS total_cars_washed
        FROM ticket_data;
        """
        result = self.execute_query(query)
        return result[0][0] if result else 0

    def retail_sales(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, location_key: Optional[str] = None) -> Dict[str, float]:
        """
        Get retail sales and average retail AWP for a given date range.

        Args:
            start_date (datetime, optional): Start date and time for the query. Defaults to today at 00:00:00 in the local timezone.
            end_date (datetime, optional): End date and time for the query. Defaults to start_date + 1 day.
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            Dict[str, float]: Dictionary containing total retail sales and average retail AWP.
        """
        if not start_date:
            start_date = datetime.now(self.local_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        if not end_date:
            end_date = start_date + timedelta(days=1)

        utc_start_date = self._convert_to_utc(start_date, location_key)
        utc_end_date = self._convert_to_utc(end_date, location_key)

        location_filter = f"AND ticket.location_key = '{location_key}'" if location_key else ""
        time_condition = self._generate_time_condition("ticket.transaction_date_time", utc_start_date, utc_end_date)

        query = f"""
        WITH retail_sales AS (
            SELECT SUM(ticket.net) AS total_sales,
                   COUNT(ticket.key) AS total_tickets
            FROM dev.wash_u.ticket AS ticket
            WHERE ticket.transaction_type = 'INDIVIDUAL_WASH'
            AND ({time_condition})
            {location_filter}
        )
        SELECT 
            ROUND(COALESCE(total_sales, 0), 2)
        FROM retail_sales;
        """
        result = self.execute_query(query)
        return result[0][0] if result else 0
    
    def retail_car_count(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, location_key: Optional[str] = None) -> Dict[str, float]:
        """
        Get retail sales and average retail AWP for a given date range.

        Args:
            start_date (datetime, optional): Start date and time for the query. Defaults to today at 00:00:00 in the local timezone.
            end_date (datetime, optional): End date and time for the query. Defaults to start_date + 1 day.
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            Dict[str, float]: Dictionary containing total retail sales and average retail AWP.
        """
        if not start_date:
            start_date = datetime.now(self.local_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        if not end_date:
            end_date = start_date + timedelta(days=1)

        utc_start_date = self._convert_to_utc(start_date, location_key)
        utc_end_date = self._convert_to_utc(end_date, location_key)

        location_filter = f"AND ticket.location_key = '{location_key}'" if location_key else ""
        time_condition = self._generate_time_condition("ticket.transaction_date_time", utc_start_date, utc_end_date)

        query = f"""
                SELECT COUNT(ticket.key) AS total_tickets
                FROM dev.wash_u.ticket AS ticket
                WHERE ticket.transaction_type = 'INDIVIDUAL_WASH'
                AND ({time_condition})
                {location_filter}
        """
        result = self.execute_query(query)

        # Return only total_tickets
        return result[0][0] if result else 0

    
    def retail_awp(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, location_key: Optional[str] = None) -> Dict[str, float]:
        """
        Get retail sales and average retail AWP for a given date range.

        Args:
            start_date (datetime, optional): Start date and time for the query. Defaults to today at 00:00:00 in the local timezone.
            end_date (datetime, optional): End date and time for the query. Defaults to start_date + 1 day.
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            Dict[str, float]: Dictionary containing total retail sales and average retail AWP.
        """
        if not start_date:
            start_date = datetime.now(self.local_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        if not end_date:
            end_date = start_date + timedelta(days=1)

        utc_start_date = self._convert_to_utc(start_date, location_key)
        utc_end_date = self._convert_to_utc(end_date, location_key)

        location_filter = f"AND ticket.location_key = '{location_key}'" if location_key else ""
        time_condition = self._generate_time_condition("ticket.transaction_date_time", utc_start_date, utc_end_date)

        query = f"""
        WITH retail_sales AS (
            SELECT SUM(ticket.net) AS total_sales,
                   COUNT(ticket.key) AS total_tickets
            FROM dev.wash_u.ticket AS ticket
            WHERE ticket.transaction_type = 'INDIVIDUAL_WASH'
            AND ({time_condition})
            {location_filter}
        )
        SELECT 
            ROUND(COALESCE(total_sales / NULLIF(total_tickets, 0), 0), 2)
        FROM retail_sales;
        """
        result = self.execute_query(query)

        return result[0][0] if result else 0

    def labor_percentage(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, location_key: Optional[str] = None) -> float:
        """
        Get labor as a percentage of sales for a given date range.

        Args:
            start_date (datetime, optional): Start date and time for the query.
            end_date (datetime, optional): End date and time for the query.
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            float: Labor as a percentage of sales in the specified date range and location.
        """
        if not start_date:
            start_date = datetime.now(self.local_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        if not end_date:
            end_date = start_date + timedelta(days=1)

        utc_start_date = self._convert_to_utc(start_date, location_key)
        utc_end_date = self._convert_to_utc(end_date, location_key)

        # Get total sales using the total_sales method
        total_sales = self.total_sales(start_date, end_date, location_key)

        # Query for total labor cost
        labor_query = f"""
        SELECT COALESCE(SUM(total_cost), 0) as total_labor_cost
        FROM {self.schema}.clock_entry
        WHERE in_time BETWEEN %s AND %s
        {f"AND location_key = %s" if location_key else ""}
        """

        try:
            # Execute labor query
            labor_params = [utc_start_date, utc_end_date]
            if location_key:
                labor_params.append(location_key)
            labor_result = self.execute_query(labor_query, labor_params)
            total_labor_cost = labor_result[0][0] if labor_result else 0

            # Calculate labor percentage
            if total_sales > 0:
                labor_percentage = (total_labor_cost / total_sales) * 100
                return round(labor_percentage, 2)
            else:
                return 0.0

        except psycopg2.Error as e:
            self.logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            raise

    def membership_redemptions(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, location_key: Optional[str] = None) -> int:
        """
        Get the number of membership redemptions for a given date range.

        Args:
            start_date (datetime, optional): Start date and time for the query. Defaults to today at 00:00:00 in the local timezone.
            end_date (datetime, optional): End date and time for the query. Defaults to start_date + 1 day.
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            int: Number of membership redemptions.
        """
        if not start_date:
            start_date = datetime.now(self.local_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        if not end_date:
            end_date = start_date + timedelta(days=1)

        utc_start_date = self._convert_to_utc(start_date, location_key)
        utc_end_date = self._convert_to_utc(end_date, location_key)

        location_filter = f"AND ticket.location_key = '{location_key}'" if location_key else ""
        time_condition = self._generate_time_condition("ticket.transaction_date_time", utc_start_date, utc_end_date)

        query = f"""
        SELECT 
            COUNT(CASE WHEN ticket.transaction_type = 'MEMBERSHIP_REDEMPTION' THEN 1 END) AS membership_redemptions
        FROM dev.wash_u.ticket AS ticket
        WHERE ({time_condition})
        {location_filter};
        """
        result = self.execute_query(query)
        return result[0][0] if result else 0

    def membership_recharge_income_and_count(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, location_key: Optional[str] = None) -> Tuple[float, int]:
        """
        Get membership income and count of tickets for a given date range.

        Args:
            start_date (datetime, optional): Start date and time for the query. Defaults to today at 00:00:00 in the local timezone.
            end_date (datetime, optional): End date and time for the query. Defaults to start_date + 1 day.
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            Tuple[float, int]: Total membership income and count of tickets.
        """
        if not start_date:
            start_date = datetime.now(self.local_timezone).replace(hour=0, minute=0, second=0, microsecond=0)
        if not end_date:
            end_date = start_date + timedelta(days=1)

        utc_start_date = self._convert_to_utc(start_date, location_key)
        utc_end_date = self._convert_to_utc(end_date, location_key)

        location_filter = f"AND ticket.location_key = '{location_key}'" if location_key else ""
        time_condition = self._generate_time_condition("ticket.transaction_date_time", utc_start_date, utc_end_date)

        query = f"""
        SELECT 
            COALESCE(SUM(CASE WHEN ticket.transaction_type IN ('RECURRING_MEMBERSHIP_PAYMENT', 'MEMBERSHIP_REACTIVATION', 'UPGRADE')
                THEN ticket.net ELSE 0 END), 0) AS total_membership_income,
            COUNT(*) AS ticket_count
        FROM dev.wash_u.ticket AS ticket
        WHERE ({time_condition})
        AND ticket.transaction_type IN ('RECURRING_MEMBERSHIP_PAYMENT', 'MEMBERSHIP_REACTIVATION', 'UPGRADE')
        {location_filter};
        """
        result = self.execute_query(query)
        return (result[0][0], result[0][1]) if result else (0, 0)

    def membership_awp(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, location_key: Optional[str] = None) -> float:
        """
        Get average membership AWP for a given date range.

        Args:
            start_date (datetime, optional): Start date and time for the query. Defaults to today at 00:00:00 in the local timezone.
            end_date (datetime, optional): End date and time for the query. Defaults to start_date + 1 day.
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            float: Average membership AWP.
        """

        # Do not pass UTC times since they will be converted in the other functions
        # pass like you would outside of the API.
        redemptions = self.membership_redemptions(start_date, end_date, location_key)
        income = self.membership_recharge_income_and_count(start_date, end_date, location_key)[0]
        
        if redemptions > 0:
            return round(income / redemptions, 2)
        else:
            return 0
        
    def membership_average_sale_price(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None, location_key: Optional[str] = None) -> float:
        """
        Get the average membership sale price for a given date range.

        Args:
            start_date (datetime, optional): Start date and time for the query. Defaults to today at 00:00:00 in the local timezone.
            end_date (datetime, optional): End date and time for the query. Defaults to start_date + 1 day.
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            float: Average membership sale price.
        """
        total_income, ticket_count = self.membership_recharge_income_and_count(start_date, end_date, location_key)
        
        if ticket_count > 0:
            return round(total_income / ticket_count, 2)
        else:
            return 0

    def churn_rate(self, year: int, month: int, location_key: Optional[str] = None) -> float:
        """
        Get the average churn rate for a specified month, year, and location.

        Args:
            year (int): The year for which to calculate the churn rate.
            month (int): The month for which to calculate the churn rate (1-12).
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            float: Average churn rate for the specified period and location.
        """
        location_filter = f"AND location.key = '{location_key}'" if location_key else ""

        query = f"""
        SELECT AVG(mvchurn.churn_percentage) / 100 AS avg_churn_rate
        FROM dev.wash_u.mv_churn_growth_rate AS mvchurn
        JOIN dev.wash_u.location AS location
        ON mvchurn.location_id = location.id
        WHERE mvchurn.year = {year} 
        AND mvchurn.month = {month}
        {location_filter};
        """
        result = self.execute_query(query)
        return result[0][0] if result else 0

    def growth_rate(self, year: int, month: int, location_key: Optional[str] = None) -> float:
        """
        Get the average growth rate for a specified month, year, and location.

        Args:
            year (int): The year for which to calculate the growth rate.
            month (int): The month for which to calculate the growth rate (1-12).
            location_key (str, optional): Specific location to query. If None, queries all locations.

        Returns:
            float: Average growth rate for the specified period and location.
        """
        location_filter = f"AND location.key = '{location_key}'" if location_key else ""

        query = f"""
        SELECT AVG(mvchurn.growth_percentage) / 100 AS avg_churn_rate
        FROM dev.wash_u.mv_churn_growth_rate AS mvchurn
        JOIN dev.wash_u.location AS location
        ON mvchurn.location_id = location.id
        WHERE mvchurn.year = {year} 
        AND mvchurn.month = {month}
        {location_filter};
        """
        result = self.execute_query(query)
        return result[0][0] if result else 0

    def close(self):
        """Closes the connection to the database."""
        if self.connection:
            self.connection.close()
            self.logger.info("Connection to Redshift closed")
