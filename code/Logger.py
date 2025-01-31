class Logger:
    def __init__(self, parser):
        self.logging_config = parser.get_logging_config()
        self.job_name = parser.get_job_name()
        self.log_level = self.logging_config['log_level']
    
    def log_start(self):
        print(f"Starting job: {self.job_name}")
    
    def log_event(self,type, message):
        if self.log_level.upper() == "INFO" and type in ["INFO", "ERROR"]:
            print(f"{type} {message}")
        elif self.log_level.upper() == "ERROR" and type == "ERROR":
            print(f"{type} {message}")
       
    
    def log_stop(self):
        print(f"Exiting job: {self.job_name}")