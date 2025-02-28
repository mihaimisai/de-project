from datetime import datetime 
def gen_timestamp():
        """
        Generates a timestamp and returns it in a dictionary format.
        Returns:
                dict: A dictionary containing the following keys:
                        - year (str): The current year.
                        - month (str): The current month.
                        - day (str): The current day.
                        - hour (str): The current hour.
                        - minute (str): The current minute.
                        - seconds (str): The current seconds.
                        - date_str (str): The formatted date and time string in the format 'yyyy-mm-dd_HH-MM-SS'.
        """
# Get the current date and time
        now = datetime.now()

        # Format the output to yyyy-mm-dd hh:mm:ss
        formatted_now = now.strftime("%Y-%m-%d_%H-%M-%S")
        year = now.strftime("%Y")
        month = now.strftime("%m")
        day =now.strftime("%d")
        hour= now.strftime("%H")
        minute =now.strftime("%M")
        seconds = now.strftime("%S")
        output = {"year":year,
                  "month":month,
                  "day":day,
                  "hour":hour,
                  "minute":minute,
                  "seconds":seconds,
                  "date_str":formatted_now}
        return output



